# GraphDB 图数据库整体架构

本文说明 GraphDB 作为通用实体关系图数据库的整体产品架构。GraphDB 是一个基于 S3/MinIO/RustFS 兼容对象存储的多租户、读写分离图数据库，面向实体、关系、写入接入、查询、导出和运维控制场景；CMDB、IT 拓扑和依赖分析是其中的应用场景。

## 1. 产品定位

GraphDB 的核心目标是作为通用实体关系图数据底座，并通过可选的领域能力支持 CMDB 等场景：

- 以对象存储作为持久化真源，避免依赖传统数据库实例。
- 以租户 prefix 隔离数据，HTTP 侧通过 `X-Tenant-ID` 选择租户。
- 写入侧采用单租户单 writer、commit log、manifest CAS 发布模型。
- 读取侧加载 manifest 对应快照和增量 commit，支持 reader cache 和最终一致读取。
- 查询侧提供 JSON DSL、当前态 scan/export、流式查询和 persisted index 加速。
- 采集侧支持 batch ingestion、source cursor、idempotency、dead-letter、source priority 治理。

## 2. 总体部署架构

```mermaid
flowchart LR
  subgraph Clients["Internal Systems"]
    Ingest["Applications / Ingest Clients"]
    GraphApps["Graph Applications"]
    Ops["Ops / Export Jobs"]
  end

  subgraph GraphDB["GraphDB Runtime"]
    Writer["Writer API\nGRAPHDB_MODE=writer|all"]
    ReaderA["Reader API\nGRAPHDB_MODE=reader|all"]
    ReaderB["Reader API\nGRAPHDB_MODE=reader"]
    CLI["CLI / Admin Commands"]
  end

  subgraph ObjectStore["Object Storage\nS3 / MinIO / RustFS"]
    TenantPrefix["tenants/<tenant>/"]
    CommitLog["immutable commits/"]
    Manifest["manifest.parquet"]
    Snapshots["snapshots/"]
    Indexes["indexes/"]
    Control["control + config + ingest metadata"]
  end

  subgraph Observability["Observability"]
    Metrics["Prometheus /metrics"]
    Logs["JSON stdout logs"]
    Traces["OTLP traces"]
  end

  Ingest -->|POST /v1/ingest/batches| Writer
  GraphApps -->|POST /v1/commits| Writer
  GraphApps -->|POST /v1/query| ReaderA
  Ops -->|scan/export APIs| ReaderB
  CLI --> Writer
  CLI --> ReaderA

  Writer --> TenantPrefix
  ReaderA --> TenantPrefix
  ReaderB --> TenantPrefix
  TenantPrefix --> CommitLog
  TenantPrefix --> Manifest
  TenantPrefix --> Snapshots
  TenantPrefix --> Indexes
  TenantPrefix --> Control

  Writer --> Metrics
  ReaderA --> Metrics
  Writer --> Logs
  ReaderA --> Logs
  Writer --> Traces
  ReaderA --> Traces
```

## 3. 逻辑模块架构

```mermaid
flowchart TB
  HTTP["internal/httpapi\nHTTP routes, admission, responses"]
  CLI["cmd/graphdb\nCLI commands"]
  Graph["internal/graph\nmodel, validation, merge, source governance"]
  Query["internal/query\nplanner, executor, profile, stream"]
  Storage["internal/storage\nmanifest, commits, snapshots, indexes, ingest metadata"]
  Config["internal/config\nenv config and store bootstrap"]
  Obs["internal/observability\nmetrics, logs, traces"]
  Object["ObjectStore interface\nlocal file or S3-compatible"]

  CLI --> Storage
  CLI --> Query
  HTTP --> Storage
  HTTP --> Query
  HTTP --> Obs
  Query --> Graph
  Query --> Storage
  Storage --> Graph
  Storage --> Object
  Config --> Storage
  Config --> HTTP
  Storage --> Obs
```

模块职责：

- `httpapi`: HTTP API、读写模式控制、query/write admission、429 backpressure、审计日志。
- `graph`: Entity、Edge、可选 CIType、RelationType、source priority、字段归属、关系三元组 canonical identity。
- `storage`: 对象存储读写、manifest CAS、writer lease、commit replay、snapshot compact、index rebuild、ingest idempotency 和 dead-letter。
- `query`: JSON DSL、planner、index lookup、lazy materialization、path traversal、streaming。
- `observability`: Prometheus 指标、stdout JSON 日志、OTLP trace。
- `cmd`: 本地和运维 CLI。

## 4. 写入路径

```mermaid
sequenceDiagram
  participant Client
  participant HTTP as Writer HTTP API
  participant BP as Write Admission / Backpressure
  participant Store as TenantStore
  participant Graph as Graph Apply
  participant Obj as Object Storage

  Client->>HTTP: POST /v1/commits or /v1/ingest/batches
  HTTP->>BP: queue slot + pressure check
  BP-->>HTTP: allow or 429 Retry-After
  HTTP->>Store: CommitWithReport / Ingest
  Store->>Obj: acquire writer lease
  Store->>Obj: read manifest + load write cache
  Store->>Graph: apply mutations copy
  Graph-->>Store: next graph + conflicts + canonical ids
  Store->>Graph: content MD5 compare
  alt unchanged
    Store-->>HTTP: skipped=true, no new commit
  else changed
    Store->>Obj: put immutable commit object If-None-Match
    Store->>Obj: publish manifest If-Match / If-None-Match
    Store->>Obj: update persisted indexes incrementally
    Store-->>HTTP: new version + readable_version
  end
  HTTP-->>Client: 200, 207, 400, 405, or 429
```

写入关键点：

- commit object 先写入，manifest 发布成功前读端不可见；loose commit object 使用 Parquet 保存 commit identity、payload hash 和 commit payload，JSON commit envelope 和裸 `graph.Commit` JSON 不再是合法数据面对象。
- 长 commit tail 会折叠为 manifest 中的 `commit_segments`，segment 对象使用 Parquet 保存 commit key + commit，读端先回放 segment 再回放 loose commit；gzip NDJSON segment 不再是合法数据面对象。
- manifest 使用 ETag 条件写，CAS 冲突会 retry 或暴露为 backpressure 信号。
- writer lease 防止误启的第二写端或陈旧写端发布；产品边界仍是每租户一个 active writer。
- MD5 一致时跳过提交，避免重复写入造成 commit tail 增长。
- source priority 在字段级和 edge existence/field 级治理覆盖冲突。
- 低优先级 suppressed conflict 不算失败，不进入 dead-letter。

## 5. 读取和查询路径

```mermaid
sequenceDiagram
  participant Client
  participant HTTP as Reader HTTP API
  participant Admission as Query Admission
  participant Cache as Reader Cache
  participant Store as TenantStore
  participant Index as Persisted Index
  participant Query as Query Executor

  Client->>HTTP: GET entity / POST query / scan / export
  HTTP->>Admission: global + per-tenant gate
  Admission-->>HTTP: allow or limit error
  HTTP->>Cache: load tenant graph at required version
  alt cache fresh
    Cache-->>HTTP: graph snapshot
  else reload
    Cache->>Store: read manifest
    Store->>Store: load snapshot and replay commit tail
    Store-->>Cache: graph snapshot
  end
  HTTP->>Index: optional persisted index lookup
  HTTP->>Query: execute DSL / stream results
  Query-->>HTTP: results + stats + version
  HTTP-->>Client: JSON or NDJSON
```

读取关键点：

- reader 按 `GRAPHDB_POLL_INTERVAL` 或 writer invalidation 刷新。
- 查询支持 `match`、`neighbors`、`traverse`、`impact`、`shortest_path`、`explain`、`profile`；详细 JSON DSL 见 [query_capabilities.md](query_capabilities.md)，文本 GQL 见 [gql.md](gql.md)。
- 当前态运维导出使用 scan/export API，不依赖复杂 DSL。
- persisted index 可加速 field match、outbound neighbors、entity by-id 和 entity page 读取。

## 6. 对象存储布局

```mermaid
flowchart TB
  Tenant["GRAPHDB_PREFIX/tenants/<tenant>/"]
  Manifest["manifest.parquet\ncurrent visible version"]
  Commits["commits/\n00000000000000000001-<id>.parquet"]
  CommitSegments["commits/segments/\n<first>-<last>-<hash>.parquet"]
  CommitIdempotency["idempotency/commits/<key>.parquet"]
  Snapshots["snapshots/\n<version>.parquet"]
  SnapshotCatalog["snapshots/sharded/v<version>/catalog.parquet"]
  SnapshotSchema["snapshots/sharded/v<version>/schema.parquet"]
  IndexCatalog["indexes/catalog.parquet"]
  IndexDefinitions["indexes/definitions.parquet"]
  FieldIndexes["indexes/parquet/versions/v<version>/fields/<kind>/<field>.parquet"]
  EdgeShards["indexes/parquet/versions/v<version>/edges/<relation>/<from-shard>.parquet"]
  EntityPages["indexes/parquet/versions/v<version>/entities/pages/<shard>.parquet"]
  EntityByID["indexes/entities/by-id/<entity-id>.parquet"]
  TenantRegistry["../_registry.parquet"]
  TenantMetadata["metadata.parquet"]
  Config["config/source-policy.parquet\nconfig/tenant-config.parquet"]
  SavedQueries["queries/<name>.parquet"]
  Ingest["ingest/<source>/\nbatches/*.parquet\nidempotency/*.parquet\ncollectors/*.parquet\ndeadletters/*.parquet"]
  Tasks["tasks/<task-id>.parquet\ntasks/results/<task-id>.parquet"]
  IndexTasks["indexes/tasks/<task-id>.parquet"]
  Control["control/writer-lease.parquet\ncontrol/readers/*.parquet"]

  Tenant --> Manifest
  Tenant --> TenantMetadata
  Tenant --> Commits
  Tenant --> CommitSegments
  Tenant --> CommitIdempotency
  Tenant --> Snapshots
  Tenant --> SnapshotCatalog
  SnapshotCatalog --> SnapshotSchema
  Tenant --> IndexCatalog
  Tenant --> IndexDefinitions
  IndexCatalog --> FieldIndexes
  IndexCatalog --> EdgeShards
  IndexCatalog --> EntityPages
  IndexCatalog --> EntityByID
  TenantRegistry --> Tenant
  Tenant --> Config
  Tenant --> SavedQueries
  Tenant --> Ingest
  Tenant --> Tasks
  Tenant --> IndexTasks
  Tenant --> Control
```

布局原则：

- 每个租户独立 prefix，禁止跨租户读取。
- `manifest.parquet` 是读端可见性边界。
- `commits/` 是不可变提交日志。
- `idempotency/commits/` 保存直接提交幂等记录，使用 Parquet。
- `snapshots/` 是 compact 后的基线；新的 full snapshot、sharded snapshot catalog/schema 和 shard data 都使用 Parquet。
- `indexes/` 是可重建的读优化结构，不是最终真源；index definitions、catalog、secondary index、edge shard、entity page 和 by-id record 都使用 Parquet，非 Parquet index data 不会被解释为数据面对象。
- reader 进程对 Parquet secondary index、edge shard、entity page 使用本地版本化 cache；cache key 包含 tenant、catalog version、object key、catalog content hash 和 schema hash，内存 LRU 可配，磁盘 cache 位于 `GRAPHDB_READER_INDEX_CACHE_DIR`。
- tenant registry、tenant metadata、writer lease 已使用 Parquet。
- source policy、tenant config、saved query、task、index task 和 task result 已使用 Parquet。
- `ingest/` 保存采集批次、幂等记录、collector status 和 dead-letter，均使用 Parquet 控制对象。
- reader heartbeat 保存在 `control/readers/*.parquet`，reader fleet readiness 不读取 JSON heartbeat 对象。

## 7. 数据模型和治理

```mermaid
classDiagram
  class Entity {
    id
    kind
    fields
    source
    external_id
    identity_keys
    field_sources
    existence_source
  }

  class Edge {
    id
    type
    from
    to
    fields
    source
    external_id
    sources
    field_sources
    existence_source
  }

  class CIType {
    name
    fields
    identity_keys
  }

  class RelationType {
    name
    from_kind/from_kinds
    to_kind/to_kinds
    cardinality
    impact_direction
  }

  class SourcePolicy {
    default_priority
    sources
  }

  CIType "1" --> "*" Entity
  RelationType "1" --> "*" Edge
  Entity "1" --> "*" Edge : from/to
  SourcePolicy --> Entity : resolves priority
  SourcePolicy --> Edge : resolves priority
```

治理规则：

- Entity 支持 schemaless `fields`，CIType 提供可选字段规范和 identity key。
- Entity 字段按 `field_sources` 记录来源归属，优先级高的 source 覆盖低优先级。
- Edge 的真实身份是 `(type, from, to)`，`edge.id` 为 canonical ID，采集器原始 ID 作为 source alias。
- Edge existence 和 edge field 同样受 source priority 治理。
- RelationType 定义端点 kind、方向、基数和影响分析方向。

## 8. Ingestion 和运维导出

```mermaid
flowchart LR
  Collector["Collector"]
  Batch["Ingest Batch\nsource, collector_id, batch_id, cursor"]
  Normalize["Normalize + source policy"]
  Commit["Atomic commit"]
  Status["Collector Status"]
  DeadLetter["Dead Letter"]
  Scan["Current-state scan"]
  Export["Snapshot export"]

  Collector --> Batch
  Batch --> Normalize
  Normalize --> Commit
  Commit --> Status
  Normalize -->|partial invalid items| DeadLetter
  Status --> Collector

  Scan --> Ops["Ops jobs"]
  Export --> Ops
```

- Ingestion 适合采集器批量写入，支持 cursor、batch id、idempotency key。
- Direct commit 适合内部服务直接提交图 mutation。
- Dead-letter 保存失败项，支持后续 replay。
- Scan/export API 面向运维导出和同步，不走复杂查询 planner。

## 9. 控制面和可靠性

控制面能力：

- writer lease: 防止同租户误启的第二写端或陈旧写端发布；不是多 writer 调度层。
- recover: 扫描孤儿 commit，按连续版本恢复 manifest。
- repair: dry-run 或 apply deterministic repair，包括 compact、rebuild index、cleanup。
- cleanup-commits: 清理 manifest 已不引用的旧 commit。
- gc: 清理过期 dead-letter、旧 snapshot、index orphan；支持 dry-run、删除预算和 cursor checkpoint 续跑。
- index rebuild: 同步或异步重建 persisted index。
- backpressure: 基于对象存储延迟、CAS 冲突、index rebuild、commit tail、tenant quota 返回 429。
- storage maintenance: small-file 阈值可触发 compact，entity page / edge shard split/merge 阈值会进入维护报告，作为后续在线布局迁移的安全输入。

可靠性边界：

- 对象存储是最终真源，reader 本地缓存和索引均可重建。
- commit 原子性以 manifest 发布为可见边界。
- snapshot compact 降低 replay 成本，但不改变可见版本语义。
- persisted index 是优化结构，catalog stale 时查询可回退。

## 10. 运行模式

| 模式 | 用途 | 写入 | 读取 |
| --- | --- | --- | --- |
| `all` | 本地或小规模单进程 | 支持 | 支持 |
| `writer` | 生产写端 | 支持 | 可用于控制和校验 |
| `reader` | 生产读端 | 拒绝写入 | 支持 |

典型生产部署：

- 1 个 writer 实例负责 commit、ingest、compact、repair、index rebuild。
- 多个 reader 实例负责查询、scan、export。
- 所有实例共享同一个对象存储 bucket 和 prefix。
- 通过 `GRAPHDB_POLL_INTERVAL` 控制 reader 最终一致刷新间隔。

## 11. 可观测性

GraphDB 提供三类观测信号：

- Metrics: `/metrics` 暴露 HTTP、query、write backpressure、object store latency、CAS conflict、commit tail、index health 等 Prometheus 指标。
- Logs: HTTP access、write/control audit、ingest、index rebuild、slow query 以 JSON lines 写 stdout。
- Traces: 设置 `GRAPHDB_OTLP_ENDPOINT` 后通过 OTLP/HTTP 导出 trace。

建议生产告警：

- writer 429 backpressure 持续升高。
- object store p95/ewma 延迟超过阈值。
- manifest CAS conflict 持续升高。
- commit tail 超过 compact 阈值。
- index health 非 `ready`。
- reader lag 长时间大于预期。

## 12. 当前架构边界

当前版本有意保持简单：

- 不实现 Cypher/Gremlin。
- 不提供复杂 UI。
- 不内置认证系统，默认由上游网关或内部平台控制访问。
- 不提供多 writer 强事务；生产写入边界是每租户一个 active writer，writer lease 和 manifest CAS 只作为重复写端防护与故障恢复保护。
- 不把本地 reader cache 或 persisted index 作为真源。
