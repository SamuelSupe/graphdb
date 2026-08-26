# 写入与采集

[English](write-ingest.md)

GGraphDB 有三条写入路径：

- `POST /v1/commits`：直接原子图变更。
- `POST /v1/ingest/batches`：面向采集器的批量写入，支持 source、cursor、
  幂等、部分失败、死信和采集器状态。
- `POST /v1/imports`：基于 ingest batch 的异步 CSV/JSONL 批量导入，支持
  task checkpoint 和恢复。

三者都需要 `X-Tenant-ID`；在 reader 模式都返回 `405`。

这些都是通用图数据写入接口。下面的请求和文件示例在需要具体说明采集与
身份合并时使用 CMDB 风格数据；其他领域可以不使用 CI type 和 source governance。

## 直接提交

请求结构：

```json
{
  "expected_version": 0,
  "idempotency_key": "cmdb-sync-001",
  "mutations": {
    "upsert_entity_types": [],
    "upsert_relation_types": [],
    "upsert_entities": [],
    "upsert_edges": [],
    "delete_entities": [],
    "delete_entity_requests": [],
    "delete_edges": [],
    "delete_edge_requests": []
  }
}
```

`upsert_entity_types`、`delete_entity_types` 是领域中立的 1.1 名称；1.0 的
`upsert_ci_types`、`delete_ci_types` 仍然可用，并操作相同持久化结构。同一
mutation 不要同时发送两种名称。

`expected_version` 可选；设置后只有租户 manifest 仍处于该版本时才接受
提交。`idempotency_key` 可选但建议使用。相同 key 和相同 payload 会返回
已保存结果；相同 key 配合不同 payload 会返回 `idempotency_conflict`。

示例：

```sh
curl -sS -X POST "$WRITER/v1/commits" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/commit-cmdb.json
```

## 实体

实体字段：

- `id`：稳定实体 ID。
- `kind`：由应用定义的实体类型；下方 CMDB 示例使用 `host`、`service`、`database`。
- `fields`：无模式 JSON 对象。
- `source`、`external_id`、`confidence`、`source_priority`：可选的来源元数据。
- `identity_keys`：由 CI type 身份规则使用的可选身份信息。

数组字段合并：

- 在 CI type 中定义 `{"type":"array","merge_strategy":"append_unique"}`，
  默认追加不重复值。
- 在字段名后加 `!` 强制本次写入替换，例如 `"tags!": ["blue"]`。
- `!` 只改变数组合并/替换行为，不绕过 source priority。

```json
{
  "id": "host:aws:i-001",
  "kind": "host",
  "labels": ["asset", "production"],
  "source": "aws",
  "external_id": "i-001",
  "confidence": 0.9,
  "fields": {
    "hostname": "app-01",
    "region": "us-east-1",
    "tags!": ["prod"]
  }
}
```

`labels` 是 1.1 的顶层便捷字段。GGraphDB 会规范化标签并保存在兼容 1.0 的
fields map 内，因此不改变持久化实体布局，也可以使用 `labels CONTAINS
"production"` 查询。

## 关系类型和边

关系类型字段：

- `name`：关系类型名。
- `from_kind` / `to_kind`，或 `from_kinds` / `to_kinds`。
- `directed`：方向是否有语义。
- `cardinality`：`many_to_many`、`one_to_many`、`many_to_one`、
  `one_to_one`。
- `impact_direction`：影响分析的传播方向。

边以 `(type, from, to)` 作为规范身份。输入的 `edge.id` 作为来源别名
保留；GGraphDB 会将保存的边 ID 改写为稳定的规范 ID。

```json
{
  "id": "collector-edge-123",
  "type": "runs_on",
  "from": "service:api",
  "to": "host:aws:i-001",
  "source": "aws",
  "fields": {
    "status": "active"
  }
}
```

移动边端点时，应删除旧的 `(type, from, to)`，再创建新边。端点是身份，
不是可变字段。

关系属性 schema 与关系类型分开管理：

```sh
curl -sS -X PUT "$WRITER/v1/relation-schemas/cites" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{
    "strict": true,
    "fields": {
      "confidence": {"type": "number", "required": true},
      "source": {"type": "string", "default": "unknown"}
    }
  }'
```

被引用的关系类型必须已存在。默认值和校验同时作用于 direct commit、ingest
batch 和文件导入批次；删除关系类型前先删除对应属性 schema。

## Source Policy

Source policy 按租户生效：

```sh
curl -sS -X PUT "$WRITER/v1/source-policy" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/source-policy.json
```

优先级规则：

- policy 中存在 `source` 时，policy priority 覆盖请求中的
  `source_priority`。
- policy 存在但 source 未知时，使用 `default_priority`。
- 没有 policy 时，使用请求的 `source_priority`。

字段优先级：

- `field_priorities` 可以为指定规范实体字段设置 source 的绝对有效优先级，
  不改变实体级 `source_priority`。
- `source + kind + field` 规则覆盖 source 全局字段规则。
- 字段别名处理后才计算字段优先级，因此配置规范字段名。
- 没有 `source` 的直接提交实体不使用字段优先级；ingest 实体先继承批次
  source，再应用字段优先级。

字段别名：

- `field_aliases` 在合并、索引、MD5 skip、查询、scan 和 export 前，把
  输入的顶层 `entity.fields` 名称映射为规范字段名。
- `source + kind` 规则优先于 source 全局回退规则。
- 没有 `source` 的直接提交实体不使用别名；ingest 实体先继承批次 source。
- 同一 payload 同时提供规范名和别名时，规范名优先；不同的别名值作为
  带有 `alias_field` 的 suppressed conflict 返回。
- 同一规范字段存在多个别名时，按别名字段名排序解决，结果确定。
- 别名不支持嵌套路径、通配、正则或值/类型转换；Query DSL 和 scan/export
  始终使用规范字段名。

字段合并：

- 高优先级覆盖低优先级；
- 优先级相同则比较更高的 `confidence`；
- 优先级和 confidence 都相同则使用最后写入；
- 低优先级写入被 suppressed，不会失败。

suppressed conflict 会出现在 commit 和 ingest 响应中，不会进入死信。

## 删除

管理员强制删除：

- `delete_entities`：实体 ID 列表；
- `delete_edges`：规范边 ID 或已知别名。

来源感知删除：

- `delete_entity_requests`
- `delete_edge_requests`

采集器应使用来源感知的边删除：

```json
{
  "type": "runs_on",
  "from": "service:api",
  "to": "host:aws:i-001",
  "source": "aws",
  "reason": "collector no longer observes relation"
}
```

低优先级删除不能移除高优先级边存在性，会返回 suppressed conflict。

## Ingestion Batch

请求结构：

```json
{
  "source": "aws",
  "collector_id": "collector-a",
  "batch_id": "aws-batch-001",
  "idempotency_key": "aws-batch-001",
  "cursor": "next-source-cursor",
  "items": [
    {
      "external_id": "i-001",
      "entity": {
        "id": "host:aws:i-001",
        "kind": "host",
        "fields": {"hostname": "app-01"}
      }
    }
  ]
}
```

支持的 item：

- `entity`
- `edge`
- `delete_entity`
- `delete_edge`
- `relation_type`
- `ci_type`
- `entity_type`（`ci_type` 的 1.1 别名）

响应字段：

- `applied`：已纳入 commit 的 item；
- `failed`：无效 item 或提交失败；
- `suppressed`：低优先级字段/删除冲突；
- `skipped`：该批次未创建图数据 commit；
- `skip_reason`：`logical_noop` 表示应用后的逻辑图未变化，
  `idempotent_replay` 表示返回此前批次的幂等重放结果；
- `cursor`：返回的采集器 cursor；
- `failures`：item 级错误；
- `conflicts`：被抑制冲突和提交失败原因。

### 本地 WAL 模式

GGraphDB 1.2 的本地 writer 默认使用 `GRAPHDB_INGEST_MODE=wal`、
`GRAPHDB_INGEST_METADATA_MODE=segment` 和 sync durability。请求会先追加到
进程级分段 WAL；不同租户可共享一次 group-fsync，后台默认使用两个 graph
write worker 并保持同租户 FIFO 顺序；graph flush trigger 为 8 个请求 / 2 MiB，
忙租户可合并同一轮队列。同一租户的一次 flush 保留每个请求的逻辑 commit 顺序，
但把这些 commit 写入一个 Parquet commit segment，并只发布一次 manifest。

PostgreSQL coordination 不提供分布式 WAL。PostgreSQL writer 必须显式设置
`GRAPHDB_INGEST_MODE=direct`，省略时启动会失败关闭。reader 不接收写入，因而
自动选择 direct 模式。

同步耐久模式在 WAL fsync 后返回 `202 Accepted`、`Location` 和状态 URL：

```json
{
  "batch_id": "aws-batch-001",
  "state": "accepted",
  "durability": "durable",
  "accepted_at": "2026-07-30T00:00:00Z",
  "estimated_flush_at": "2026-07-30T00:00:01Z",
  "status_url": "/v1/ingest/batches/aws/collector-a/aws-batch-001"
}
```

需要等待最终结果的兼容调用方可以发送 `Prefer: wait=committed`。查询状态：

```sh
curl -sS "$WRITER/v1/ingest/batches/aws/collector-a/aws-batch-001" \
  -H 'X-Tenant-ID: demo'
```

WAL 目录属于耐久性边界。容器部署必须把 `GRAPHDB_DATA_DIR` 挂载到持久化
本地存储；仓库提供的 Compose profile 已为 writer 挂载 `/var/lib/graphdb`。

主要配置：

- `GRAPHDB_INGEST_WAL_DIR=${GRAPHDB_DATA_DIR}/wal/ingest`
- `GRAPHDB_INGEST_WAL_DURABILITY=sync`
- `GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB`
- `GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=5ms`
- `GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB`
- `GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB`
- `GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK=80`
- `GRAPHDB_INGEST_WAL_HIGH_WATERMARK=70`
- `GRAPHDB_INGEST_WAL_STOP_WATERMARK=85`
- `GRAPHDB_INGEST_MAX_PENDING_AGE=2m`
- `GRAPHDB_INGEST_FLUSH_INTERVAL=250ms`
- `GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=8`
- `GRAPHDB_INGEST_FLUSH_MAX_BYTES=2MiB`
- `GRAPHDB_INGEST_FLUSH_WORKERS=2`
- `GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms`
- `GRAPHDB_INGEST_METADATA_MAX_REQUESTS=256`
- `GRAPHDB_INGEST_METADATA_MAX_BYTES=8MiB`
- `GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=4GiB`
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=20000`
- `GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s`

内存队列达到 80%、WAL 磁盘达到 70%，或最老待提交请求超过 2 分钟时，writer
返回带 `Retry-After` 的 `429`。WAL 磁盘达到 85% 后 readiness 进入 drain-only，
直到已提交记录回收。调用方必须在建议延迟后复用原 batch/idempotency identity
重试。

#### metadata segment 模式（1.1.3+）

1.1.3 在本地 WAL 模式上引入 metadata 攒批；1.2 将其设为本地 writer 默认值：

```ini
GRAPHDB_INGEST_MODE=wal
GRAPHDB_COORDINATION=local
GRAPHDB_INGEST_METADATA_MODE=segment
GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms
GRAPHDB_INGEST_METADATA_MAX_REQUESTS=256
GRAPHDB_INGEST_METADATA_MAX_BYTES=8MiB
GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2
```

direct 模式使用 `legacy`，本地 WAL 模式默认使用 `segment`。segment 模式把同一租户跨
graph flush 的完整请求、结果、摘要和本窗口最终 collector status 写入一个
内容寻址 Parquet segment，再用一次独立 ingest manifest CAS 发布。batch 和
idempotency identity 可以同时指向 segment 内同一条记录；正常 256 请求窗口
不再创建逐请求 batch/idempotency 对象或 collector status 对象。

manifest 直接保存最近 32 个 segment 引用，更早的引用进入分层索引目录；
每层最多 8 个目录对象。目录合并只重写引用和 Bloom，不重写或删除历史 payload
segment。点查顺序为活跃 WAL、新 segment/index、旧 batch/idempotency 对象。
旧历史不迁移也不删除，首次新格式 collector 状态会从旧状态或旧 batch 记录
初始化累计值。

graph manifest 发布后请求进入 `published`；metadata manifest 发布后才进入
`committed` 并允许 WAL 回收。`Prefer: wait=committed` 会立即 flush 当前租户
metadata 窗口，普通 `202` 允许等待阈值或 500 ms 窗口。segment PUT、manifest
CAS 和 `FINALIZED` 之间崩溃时，WAL 中持久化的 metadata flush ID、LSN 范围
和内容哈希保证恢复不会重复 graph version、collector totals 或结果。

升级必须分两步：

1. 所有 reader 和 writer 先升级到 1.1.3，继续使用 `legacy`；
2. 再停止写入、确认没有 1.1.2 writer 后显式切换到 `segment`。

租户一旦存在 segment manifest，1.1.3 legacy writer 会拒绝继续旧格式写入。
此后不能直接回退到 1.1.2 writer；回退前必须停止写入并用 1.1.3
legacy-compatible 工具导出或转换。direct 模式、graph commit 格式、逻辑
version、FIFO、租户隔离和 deadletter 对象布局不变。

WAL 在 HTTP 监听前完成恢复并持有目录进程锁。损坏的中间记录会阻止启动；
只有最后一个 segment 的不完整尾记录会被截断。已经 durable accepted 的
请求即使客户端断开也会继续写入；manifest 已发布但 ingest metadata 尚未
完成的请求会用 WAL 中已 fsync 的 PREPARED 提交计划恢复，不会重复增加
graph version。首次遇到历史 loose commit 尾部时会将其并入 segment；该租户
随后不再承担这次迁移成本。

#### 1.1.4 稀疏多租户运行保障

metadata flush worker 与 graph write worker 独立，默认值为 `2`，500 ms 与
256 个请求 / 8 MiB 是调度 trigger。数百个以稀疏写入为主的租户可根据对象存储
p95 延迟和 metadata deadline-overshoot 指标继续调优。同一租户始终最多只有
一个进行中的 metadata flush，metadata segment
也绝不跨租户混装；提高 worker 数只改善公平性，不改变顺序或隔离语义。

每租户自动 maintenance 会等待 ingest 空闲满 1 分钟，才运行 compact、GC 或
索引追赶。后台重型任务默认单并发。

每个 writer 在进程内维护有界 LRU，缓存 ingest metadata manifest、目录和
不可变 segment：最多 1,024 个对象、64 MiB 计费驻留量。解码后的 segment 按
编码字节数与每个保留 item 4 KiB 估算值中的较大者计费。manifest 条目 TTL 为
1 秒，不可变 index 和 segment 条目 TTL 为 15 分钟。同一个对象的并发冷读会合并
为一次对象存储请求；热查不会再发起 GET。该缓存可随进程丢弃并在重启后重建；
成功的 metadata manifest CAS 会替换本地 manifest 条目，它不是跨 writer 的
一致性机制。

WAL prune 推进到已知安全位置时，writer 会在
`GRAPHDB_INGEST_WAL_DIR` 原子写入 `checkpoint.json`。启动时优先从有效
checkpoint 恢复，只扫描活跃尾部；缺失、截断或无效 checkpoint 会回退为完整
WAL 扫描，WAL 本身损坏仍会阻止启动。不要手工修改 checkpoint，也不要脱离
对应 WAL 目录单独复制或恢复它；它只是恢复加速记录，不能替代 WAL 或对象存储
备份。

使用 `graphdb_ingest_metadata_deadline_overshoot_seconds`、
`graphdb_ingest_metadata_cache_total` 和
`graphdb_ingest_wal_checkpoint_{total,scanned_bytes,duration_seconds}` 分别观察
worker 饱和、点查压力和恢复成本。JSON 日志新增
`ingest_wal_checkpoint_written`、`ingest_wal_checkpoint_recovery`；相应结果为
`used`、`miss`、`fallback`、`written` 或 `error`。

`/metrics` 提供 `graphdb_ingest_wal_*`、`graphdb_ingest_queue_*`、
`graphdb_ingest_flush_*` 和 `graphdb_ingest_metadata_*` 指标，包括
append/fsync、WAL 内存与磁盘占用、
written/durable LSN、待处理请求与最老等待时间、状态缓存命中/淘汰、flush
延迟与请求/commit/segment/manifest 数、metadata 物理 PUT/字节、manifest
冲突、Bloom 候选、metadata cache 结果、checkpoint 扫描成本、恢复重放字节，
以及 recovery 结果。这些指标只有
固定状态类标签，不包含 tenant、source、collector 或 batch 等高基数标签。

进程日志会输出 `ingest_wal_recovery`、采样后的 `ingest_wal_accepted`
（首个成功事件及之后每第 1,024 个成功事件）、
`ingest_flush_started`、`ingest_flush_completed`、
`ingest_metadata_flush_started`、`ingest_metadata_segment_completed`、
`ingest_metadata_manifest_published`、WAL rotate/prune/fsync
失败和 shutdown 等 JSON 事件；租户、batch、LSN、flush ID、耗时和错误原因
留在日志中；指标、trace、失败日志和 WAL 记录不采样。设置
`GRAPHDB_OTLP_ENDPOINT` 后，accept、WAL append/group
write、flush、batch apply、publish、metadata encode/PUT、manifest CAS 和
Bloom/index lookup 会通过 OTLP/HTTP
导出。异步 group write 和 flush 使用 OTel links 关联原请求 span；accepted
记录还会保存 trace context，因此进程恢复后仍可关联原始写入请求。

#### 1.1.5 WAL 故障处理

瞬态对象存储或 metadata flush 错误会进入重试。重试尚未完成时，readiness
保持不可用；重试成功后 writer 清除 last error，无需重启即可恢复就绪。这不会
改变 durable `202` 的契约。

本地 WAL 的 append、短写、rotate 或 fsync 发生致命错误时，writer 会被 fence。
新的 ingest append 返回 `503` 和稳定错误码 `ingest_wal_unavailable`
（`retryable=true`），不会分配新的 LSN；已经 durable accepted 的记录仍以 WAL
作为恢复事实来源。请保留 WAL 目录，先修复或替换故障存储，确认原因后再重启。
被 fence 的 writer 不得继续向可能损坏的尾部追加。

v1.1.5 发行门禁使用真实二进制、`GRAPHDB_INGEST_MODE=wal`、
`GRAPHDB_COORDINATION=local` 和显式 `GRAPHDB_INGEST_METADATA_MODE=segment`
验证上述行为。提交绑定的证据覆盖 durable accepted 批次跨进程重启和对象存储
中断；这些模式在该门禁中均为显式配置。

#### 1.2.0 性能优先默认值

v1.1.5 到 v1.2.0 是单向数据升级：停止旧 writer，保留 WAL 与对象数据，再
启动 v1.2.0；v1.2.0 激活 segment metadata 后不要再运行 v1.1.5 writer。
发行门禁不提供反向兼容。图模型、逻辑 commit/version 顺序、FIFO 语义和对象
布局保持不变，变化仅限物理攒批和默认运行策略。

发行门禁在同一固定 OrbStack 主机上分别运行 v1.1.5、v1.2.0 各 5 次、每次
30 分钟。v1.2.0 必须达到至少 10,000 committed mutations/s 和 v1.1.5 中位数
的 1.5 倍；accepted p95/p99 不超过 20/50 ms，committed p95/p99 不超过
8/15 秒；RSS 不超过 7 GiB 且不超过基线 110%；每 1,000 mutation 的 CPU
至少下降 25%；吞吐离散不超过 5%；direct 写入与查询回归不超过 10%。

CMDB 采集场景的批次建议：

- 从每批 200 个逻辑 CMDB 组开始，对象存储和 writer 超时稳定后再接近 500；
- 优先扩大批次，而不是并发大量小批次；每批都有固定的 commit、manifest、
  幂等和采集器元数据开销；
- 如果 `batch_id` 已代表采集器 checkpoint，复用它作为 `idempotency_key`，
  writer 可以合并为一个 ingest 元数据对象；
- 429 后复用相同 `batch_id` 和 `idempotency_key`，指数退避加抖动重试；
- 从 200 提升到 500 组时，应同步增加 HTTP 超时，因为每组通常会展开为
  多个实体和边。

采集器状态：

```sh
curl -sS "$WRITER/v1/ingest/collectors/aws/collector-a" \
  -H 'X-Tenant-ID: demo'
```

死信：

```sh
curl -sS "$WRITER/v1/ingest/deadletters/aws" -H 'X-Tenant-ID: demo'
curl -sS -X POST "$WRITER/v1/ingest/deadletters/aws/replay?limit=10" -H 'X-Tenant-ID: demo'
```

## CSV 与 JSONL 导入

JSONL 的每个非空行都是一个已有的 ingestion item：

```jsonl
{"external_id":"doc-1","entity":{"id":"document:1","kind":"document","labels":["article"],"fields":{"title":"Graph Storage"}}}
{"external_id":"cite-1","edge":{"type":"cites","from":"document:1","to":"document:2","fields":{"confidence":0.95}}}
```

```sh
curl -sS -X POST \
  "$WRITER/v1/imports?source=knowledge-base&collector_id=files&batch_size=500&on_error=continue" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @graph.jsonl
```

CSV 必须包含 `record_type`。entity/node 和 edge/relationship 行使用下方保留
列，其他非空列会成为带类型的属性；`labels` 支持 JSON 字符串数组或 `|`
分隔文本。

```csv
record_type,id,entity_type,labels,relation_type,from,to,title,confidence
entity,document:1,document,article|published,,,,Graph Storage,
edge,,,,cites,document:1,document:2,,0.95
```

```sh
curl -sS -X POST "$WRITER/v1/imports?format=csv&on_error=abort" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: text/csv' \
  --data-binary @graph.csv
```

CSV `record_type` 支持 `entity`/`node`、`edge`/`relationship`、
`delete_entity`/`delete_node`、`delete_edge`/`delete_relationship`、
`entity_type`、`relation_type`。类型定义行把 JSON 放在 `payload` 或
`definition` 列。

接口返回 `202`、普通 `bulk_import` task 和 `Location` header。轮询
`/v1/tasks/{id}` 获取 checkpoint、进度、问题样本和最终计数。`format` 可由
JSONL/CSV Content-Type 推断；`batch_size` 默认 500、最大 5000；`on_error`
为 `abort` 或 `continue`。当前上传上限是 32 MiB，每个租户同时只运行一个
bulk import。

## MD5 Skip

commit 和 ingest 作用于当前图。如果结果 MD5 与当前已存图相同，GGraphDB
会跳过新 commit 并返回 `skipped=true`，避免重复采集导致 commit tail
增长。

## 写入背压

写入准入可能以 `429` 返回结构化原因：

```json
{
  "error": "write backpressure",
  "code": "write_backpressure",
  "retry_after_ms": 2000,
  "reasons": [
    {
      "code": "commit_tail_too_long",
      "current": 301,
      "threshold": 1500,
      "message": "compact required"
    }
  ]
}
```

采集器应遵守 `Retry-After`，使用相同 `idempotency_key` 重试；同一原因
反复出现时降低并发。

常见原因：

- 对象存储延迟或错误；
- manifest CAS 冲突；
- 索引重建或维护任务运行中；
- commit tail 过长；
- 租户实体/边/对象/字节配额超限。
