# Ingest WAL、攒批与精确去重最终方案

## 1. 文档状态

- 状态：1.1.2 WAL/graph batching 基线与 1.1.3 metadata segment 已实现；
  1.2.0 已将该路径设为本地 writer 的性能优先默认值。
- 目标版本：1.2.0 默认性能 profile。
- 目标部署：
  - `GRAPHDB_COORDINATION=local`；
  - 单个活动 writer 进程；
  - 多租户并发 ingest；
  - writer 具备持久化本地卷；
  - graph 数据仍发布到本地或 S3 兼容对象存储。

本文只覆盖 `POST /v1/ingest/batches`。直接提交、租户生命周期和维护任务
不进入 WAL，但必须与租户 ingest 队列建立顺序屏障。

## 2. 结论

最终方案采用：

1. 一个进程级、分段、可恢复的本地 WAL，通过内存 buffer 和 group fsync
   合并多租户的小写入。
2. WAL 持久化后，按租户进入独立 FIFO 队列；默认最大等待 1 秒，达到
   请求数或字节阈值时提前 flush。
3. 一次租户 flush 保留多个按请求顺序生成的逻辑 commit，不把异构请求
   粗暴拼成一个 `graph.Mutations`。
4. 多个逻辑 commit 直接写入现有 Parquet commit segment，只发布一次
   manifest。
5. 在 flush 内对语义完全相同的写入做精确去重，并把结果映射回每个原始
   batch。
6. 使用 `ACCEPTED -> PREPARED -> PUBLISHED -> FINALIZED` WAL 状态机关闭
   manifest 已发布但 ingest metadata 尚未完成时的崩溃窗口。
7. WAL append buffer 和待 flush 热缓存分别限额；缓存淘汰只丢弃解码结果，
   永远不丢弃尚未完成的 WAL 记录。

该设计的关键收益是：

- 多租户请求共享 WAL group fsync；
- 同一租户一个 flush 只写一个或少量 commit segment；
- 同一租户一个 flush 只发布一次 manifest；
- graph 写缓存只加载一次；
- 最终内容 MD5 和增量索引只计算、更新一次；
- 精确重复内容不再产生额外逻辑 commit。

跨租户数据不能合并到同一个 graph commit 或 commit segment。租户隔离、
manifest、版本和恢复边界仍然独立。

## 3. 当前实现依据

当前代码的主要边界如下：

- `internal/storage/ingest.go` 在租户写锁内完成幂等检查、构建 mutation、
  `commitWithRetryLocked`、ingest batch metadata 和 collector status。
- `internal/storage/tenant_commit.go` 为每次变更生成独立 commit ID，写不可变
  commit object，随后发布 manifest。
- `internal/graph/apply.go` 按 mutation 类型执行固定顺序，而不是按 HTTP
  请求顺序执行。
- `internal/storage/commit_segment.go` 已支持一个 Parquet segment 保存多个
  连续版本的 `graph.Commit`。
- `internal/storage/tenant_load.go` 会按 segment 内顺序重放 commit，并校验
  版本连续性。
- `internal/storage/ingest_store.go` 使用条件写保存 batch/idempotency 结果，
  相同结果可以安全重试。

因此，最安全的批处理方式不是把多个请求的 mutation slice 简单拼接，而是
保留多个逻辑 commit，再借助现有 commit segment 一次发布。

## 4. 方案审查与优化结果

| 原始设计点 | 审查风险 | 最终调整 |
|---|---|---|
| 多请求合成一个 `graph.Commit` | `applyMutations` 按类型固定排序，可能把后到的 delete 排在先到的 upsert 前面 | 每个请求保留独立逻辑 commit，按 FIFO 顺序应用 |
| 每个请求独立 commit object | 对象 PUT 和 manifest CAS 仍接近请求数 | 多个逻辑 commit 直接写一个 commit segment，manifest 只发布一次 |
| 每租户独立 timer/goroutine | 大量租户产生 timer、goroutine 和 GC 压力 | 一个全局 deadline heap，加有界 flush worker |
| 攒批队列仍默认同步返回 | 低流量租户每次请求会占用连接直到 flush | WAL 模式默认 durable 202；需要同步结果时显式等待 committed |
| WAL 中使用 DATA/REF 内容引用 | 恢复和 segment GC 复杂，REF 可能长期钉住旧 segment | 第一版 WAL 保存完整请求；只对相同幂等身份共享待处理记录 |
| 内存 append queue、WAL buffer、热缓存都保留 payload | 同一请求可能出现三份内存副本 | 每个阶段只保留一份所有权明确的 payload，落盘后释放 append buffer |
| collector status、索引失败阻止 WAL 回收 | 可修复派生状态可能永久钉住 WAL | manifest 和 ingest batch record 为必需状态，派生状态异步修复 |
| HTTP write admission 覆盖整个 10 秒等待 | 等待请求占满 write slot | WAL 容量准入与实际 flush commit 准入分离 |
| flush 中任一坏请求导致整批失败 | 单个坏请求放大为租户级失败 | 快速批处理失败时回退到按请求顺序隔离 |

## 5. 目标与非目标

### 5.1 目标

- 已返回 durable accepted 的请求在进程崩溃后可恢复。
- 默认 graph flush 最大等待时间为 1 秒。
- 保留同一租户请求的 FIFO 语义。
- 保留每个有效、发生变化的请求对应的逻辑 graph version。
- 相同幂等身份的重试只处理一次。
- 不同 batch 的完全相同内容只产生一次真实状态变化。
- 内存、WAL 磁盘、flush 并发和 metadata 写入全部有界。
- 不增加 PostgreSQL 依赖。

### 5.2 非目标

- 不支持多个活动 writer 共享本地 WAL。
- 不跨租户合并 graph commit。
- 第一版不让 `/v1/commits`、purge、restore 等写操作进入 WAL。
- 第一版不改变 ingest batch metadata 的对象布局。
- 第一版不实现 WAL 压缩、跨 segment 内容引用或自定义磁盘加密。
- 不把仅在内存中的记录作为默认成功确认。

## 6. 总体架构

```mermaid
flowchart LR
  Client["Ingest Client"]
  HTTP["Writer HTTP"]
  Append["Bounded Append Queue"]
  WAL["Global Segmented WAL\nMemory Buffer + Group Sync"]
  Scheduler["Tenant Queue Scheduler\nDeadline Heap"]
  Cache["Bounded Hot Cache"]
  Flush["Flush Workers"]
  Graph["Sequential Logical Commits"]
  Segment["Parquet Commit Segment"]
  Manifest["One Manifest Publish"]
  Metadata["Per-request Ingest Records"]
  Derived["Collector Status + Index Repair"]

  Client --> HTTP
  HTTP --> Append
  Append --> WAL
  WAL --> Scheduler
  Scheduler --> Cache
  Scheduler --> Flush
  Cache --> Flush
  Flush --> Graph
  Graph --> Segment
  Segment --> Manifest
  Manifest --> Metadata
  Metadata --> Derived
```

逻辑上只有 WAL writer 串行分配 LSN；graph flush 仍可跨租户并发。同一租户
任意时刻最多有一个 flush worker。

## 7. API 与确认语义

建议增加：

```ini
GRAPHDB_INGEST_MODE=direct|wal
```

### 7.1 `direct`

- 保持当前同步行为和 200/207 响应。
- 用于 PostgreSQL 多 writer 回归验证，或明确需要同步可见性语义的调用方。
- v1.2.0 不再把 `direct` 作为本地 writer 的兼容默认值；PostgreSQL writer
  必须显式配置 `GRAPHDB_INGEST_MODE=direct`。

### 7.2 `wal`

- 请求完成基础校验和 WAL fsync 后返回 `202 Accepted`。
- 响应包含 `batch_id`、`state=accepted`、`durability=durable`、预计最晚
  flush 时间和 `status_url`。
- 使用 `Location` header 指向：

```text
GET /v1/ingest/batches/{source}/{collector_id}/{batch_id}
```

- batch 状态：

```text
accepted -> flushing -> committed | failed
```

状态查询先检查活跃 WAL index；记录完成后复用现有 ingest batch record。这样
不需要维护一套新的长期 operation ID 索引。

- 客户端需要同步等待时使用：

```http
Prefer: wait=committed
```

该请求仍进入 WAL 和攒批队列，但 HTTP 连接等待最终 200/207。等待连接不占用
实际 graph commit 的 write admission。

### 7.3 超时语义

请求一旦达到 `durableLSN`，客户端断开或等待超时都不能取消对应 WAL 记录。
客户端必须使用相同 `batch_id` 和 `idempotency_key` 查询或重试。

## 8. WAL 格式与持久性

### 8.1 Segment

WAL 目录默认：

```text
${GRAPHDB_DATA_DIR}/wal/ingest/
```

segment 使用单调递增编号：

```text
00000000000000000001.wal
00000000000000000002.wal
```

每条记录格式：

```text
magic
format_version
record_type
lsn
payload_length
payload
crc32c
```

恢复时必须先校验 `payload_length` 上限，再分配内存，避免损坏长度造成异常
分配。CRC 使用 Castagnoli 多项式。

WAL 目录权限为 `0700`，segment 文件权限为 `0600`。自定义加密不属于第一版，
部署侧应使用加密持久卷。

### 8.2 Record 类型

```text
ACCEPTED
PREPARED
PUBLISHED
FINALIZED
FAILED
```

- `ACCEPTED`：完整、规范化的请求和请求摘要已经持久化。
- `PREPARED`：已经生成 base version、逻辑 commits、commit IDs、segment
  内容摘要和逐请求结果。
- `PUBLISHED`：目标 manifest 已发布。
- `FINALIZED`：必需的 ingest batch/idempotency records 已持久化。
- `FAILED`：确定性错误已保存为最终失败结果。

`PREPARED` 必须在写 commit segment 和发布 manifest 之前 fsync。它关闭
manifest 已成功但进程还没有保存结果时无法准确恢复的窗口。

创建新 segment、原子替换 checkpoint 或删除旧 segment 时，除了同步文件，
还必须同步父目录，保证目录项在主机故障后可恢复。

### 8.3 内存写缓冲和 group fsync

WAL writer 由一个 goroutine 独占：

```text
acceptedLSN  已进入有界 append queue
writtenLSN   已写入操作系统 page cache
durableLSN   已完成 file.Sync
```

触发一次 WAL write + sync 的条件：

- buffer 达到 4 MiB；
- 5 ms group-sync 周期到期；
- segment 轮转；
- 服务关闭；
- 恢复或管理流程要求强制同步。

默认只有 `durableLSN >= requestLSN` 才能返回 durable accepted。内存 buffer
减少 write syscall 和 fsync 次数，但不改变 durable 边界。

v1.2.0 的公开 WAL server 模式只允许 `sync`。低于 fsync 的确认不能返回
`202 Accepted`，也不属于可发布配置。

## 9. 内存模型

内存分为两个独立预算。

### 9.1 WAL append 内存

- 保存尚未写入 WAL 文件的编码记录。
- 达到上限时短暂阻塞；超过 queue timeout 返回 429。
- 记录进入 `writtenLSN` 后释放编码 buffer 的所有权。

### 9.2 待 flush 热缓存

- 保存已经 durable、但尚未 graph flush 的规范化 request 和 mutation。
- 按字节计费，不按租户数或 entry 数计费。
- 超限时淘汰解码结果，只保留：

```text
tenant_id
lsn
segment
offset
length
deadline
request_digest
```

- flush 时缓存未命中则按 WAL offset 重新读取。
- 淘汰优先保留接近 deadline 的租户，而不是简单保留最近访问租户。

不要同时在 append queue、WAL buffer 和热缓存长期保存三份 payload。编码
buffer 写入后释放；热缓存保存规范化结构；WAL 文件是唯一恢复依据。

## 10. 多租户队列与调度

不为每个租户创建 goroutine、ticker 或 `time.Timer`。

调度器维护：

```text
map[tenantID]*tenantQueue
deadline min-heap
ready FIFO
bounded flush worker semaphore
```

租户队列第一条记录确定：

```text
deadline = firstAcceptedAt + flushInterval
```

后续请求不能延长 deadline。

任一条件触发提前 flush：

- deadline 到期；
- 请求数达到 flush trigger；
- 待 flush 请求字节数达到 flush trigger；
- direct commit 或租户生命周期屏障；
- full-sync 屏障；
- WAL 或内存进入压力状态；
- 服务关闭。

公平性规则：

- 同一租户最多一个运行中 flush。
- hot tenant 完成一个 flush 后重新放到 ready FIFO 尾部。
- flush worker 总数继续受全局 write admission 约束。
- 一个租户的对象存储错误不能占用全部 worker；重试进入延迟队列。

## 11. Flush 的核心设计

### 11.1 不合成单个大 `graph.Commit`

现有 `graph.ApplyCommit` 会按 mutation 类型执行固定顺序。例如 delete 类操作
先于 entity upsert。若把多个请求直接拼成一个 `graph.Mutations`，请求：

```text
request 1: upsert entity A
request 2: delete entity A
```

可能被重排为 delete 后 upsert，改变最终状态。

最终方案保留请求边界：

```text
request 1 -> logical commit V+1
request 2 -> logical commit V+2
request 3 -> logical no-op
request 4 -> logical commit V+3
```

发生变化的逻辑 commit 保持连续版本。no-op 不增加版本，与当前行为一致。

### 11.2 直接生成 commit segment

一次租户 flush：

1. 只加载一次当前 graph、manifest 和 write cache。
2. 按 FIFO 顺序处理每个 request。
3. 为每个发生变化的 request 生成独立 `graph.Commit`、commit ID、version 和
   `ApplyReport`。
4. 将逻辑 commits 组成 `[]commitSegmentItem`。
5. 写一个或少量现有格式的 Parquet commit segment。
6. manifest 追加 segment refs，设置最终 version 和最后一个 commit ID。
7. 只执行一次 manifest 条件发布。
8. final graph 只计算一次 Content MD5。
9. persisted indexes 从 base graph 到 final graph 只更新一次。

若当前 manifest 仍有 loose `CommitKeys`，必须把这些已有 loose commits 放在
新 segment 的开头，再追加本次逻辑 commits，随后清空 `CommitKeys`。不能把
新 segment 简单追加到 `CommitSegments` 后而保留旧 loose tail，否则 loader
会先重放新 segment，再重放旧 key，破坏版本顺序。

segment 应按编码字节数和 commit 数量切分；多个 segment 可以在一次 manifest
发布中按顺序追加。

如果只有一个新逻辑 commit，且没有需要折叠的 loose tail，可以继续使用现有
单 commit object 路径，避免单行 segment 的额外开销。

commit segment 降低对象数量，但不会降低逻辑 replay 数量。
`ManifestCommitTailLength` 仍按 segment 内 commit 数量计费。生成 PREPARED
前必须计算：

```text
projectedTail = currentTail + changedLogicalCommits
```

若 projected tail 超过现有 `WriteMaxCommitTail`，先触发 compact 并暂停该租户
flush；同时停止继续接受该租户的新记录。不能因为只发布一个 segment 就绕过
commit-tail backpressure。

### 11.3 Graph apply 快速路径

为了避免每个逻辑 commit 都执行一次大 map Copy-on-Write，增加面向 storage
的批量 apply 快速路径：

```text
ApplyCommitBatchStorageCopyWithOptions(
    baseGraph,
    orderedLogicalCommits,
) -> finalGraph, []ApplyReport
```

快速路径：

- 根据整批 mutation 的并集只 clone 一次需要修改的顶层结构。
- 在 private graph 上按逻辑 commit 顺序应用。
- 每个逻辑 commit 仍设置自己的 version 和 `CreatedAt`。
- 每一步执行 schema、关系、唯一性和 quota 校验。
- 最终只计算一次内容 MD5。

如果任一请求在快速路径中出现确定性校验错误，丢弃 private graph，回退到
按请求顺序处理：

- 成功请求继续形成逻辑 commit；
- 失败请求保存 item/commit failure；
- 后续请求基于最近成功状态继续执行。

错误路径可以更慢，正常 ingest 快速路径保持一次 clone。

### 11.4 可见性

多个逻辑 commit 通过一次 manifest 发布原子可见。客户端获得各自的逻辑
version，但 reader 可能直接从旧 manifest 跳到本次 flush 的最终 version。
这是 WAL 模式的明确契约。

## 12. 精确重复合并

### 12.1 请求级幂等

幂等身份：

```text
tenant_id + source + collector_id + idempotency_key
```

没有 `idempotency_key` 时使用 `batch_id`。

- 相同身份、相同请求摘要：共享同一个待处理记录和结果。
- 相同身份、不同请求摘要：立即返回 idempotency conflict。
- 首先查询活跃 WAL request index；flush 前再查询已有持久化 ingest record，
  覆盖进程重启和历史请求。

### 12.2 内容级精确去重

内容摘要：

```text
SHA-256(
  tenant_id +
  mutation_type +
  normalized_semantic_payload
)
```

规则：

- `source`、规范化后的 ID、字段、优先级、confidence 和操作类型参与摘要。
- `batch_id`、`idempotency_key`、`cursor` 和 transport metadata 不参与
  graph 内容摘要。
- 只在同一租户、同一次 flush 内合并。
- same resource ID 但字段不同不合并。
- delete、schema change 和 full-sync 第一版只做请求级幂等，不做跨 batch
  内容去重。

若不同 batch 的增量 upsert 内容完全相同：

- 第一条按正常逻辑应用；
- 后续请求不再生成物理 mutation；
- 若第一条改变了 graph，后续请求返回 `skipped=true`、
  `skip_reason=logical_noop` 和当前逻辑 version；
- 每个 batch 仍保存独立 ingest result、cursor 和 collector 统计。

第一版 WAL 仍保存每个已接受请求的完整 payload，不使用跨记录 DATA/REF。
只有相同幂等请求的并发等待者不重复追加 WAL。

## 13. PREPARED、发布与恢复

### 13.1 正常路径

```text
ACCEPTED durable
  -> load base graph/manifest
  -> build ordered logical commits and per-request results
  -> PREPARED durable
  -> put immutable commit segment(s)
  -> publish one manifest
  -> PUBLISHED durable
  -> save required ingest records
  -> update derived collector/index state
  -> FINALIZED durable
```

`PREPARED` 至少包含：

- base manifest version 和 ETag；
- final version；
- 最后一个 commit ID；
- 每个逻辑 commit 的 ID、version、created_at 和 mutation digest；
- segment key、content hash、first/last version；
- 每个 request 的最终 `IngestResult`；
- flush ID 和涉及的 WAL LSN 范围。

commit ID、created_at 和 segment 内容必须在 PREPARED 后保持不变，使对象
写入和恢复重试具有确定性。

### 13.2 恢复矩阵

| WAL 状态 | 恢复动作 |
|---|---|
| 部分尾记录或 CRC 失败 | 只截断最后一个不完整记录；中间损坏则 fail closed |
| `ACCEPTED` 无 `PREPARED` | 重新进入原租户队列 |
| `PREPARED`，manifest 仍是 base version | 复用相同 commit IDs 和 segment 内容继续发布 |
| `PREPARED`，manifest head/version 等于目标 | 视为已发布，继续 metadata finalize |
| `PREPARED`，manifest 已前进但 head 不匹配 | 标记 repair required，不盲目重放 |
| `PUBLISHED` 未 `FINALIZED` | 重试保存逐请求 ingest records 和必需结果 |
| `FINALIZED` | 不再排队，等待 segment GC |

writer 对外监听前必须完成 WAL 扫描和未完成状态重建。恢复未完成时 readiness
失败，但 liveness 保持正常。

### 13.3 必需状态与派生状态

WAL 可以回收前必须完成：

- manifest 已发布，或请求已确定失败；
- 每个 request 的 batch/idempotency result 已保存。

以下状态可重建，不应永久阻止 WAL 回收：

- materialized collector status；
- persisted index；
- relation schema validation checkpoint；
- metrics、audit log 和 trace。

这些派生更新失败时记录 warning 并进入后台 repair queue。

## 14. Metadata 写入优化

第一版保留每个请求现有 ingest batch/idempotency object，避免同时更改长期
幂等布局。

可立即实施的优化：

- 建议客户端令 `batch_id == idempotency_key`，减少为一个 metadata object。
- metadata PUT 使用有界 worker pool，不按请求无限创建 goroutine。
- 同一个 flush 内，按 `(tenant, source, collector_id)` 汇总 collector status，
  每组只执行一次物化状态写。
- ingest batch records 仍逐请求保存，保证查询、重试和 dead-letter 兼容。

如果上线后 metadata PUT 成为主要瓶颈，再单独设计 ingest record segment 和
查询索引；不与第一版 WAL 同时实施。

## 15. 屏障与其他写操作

第一版只有 ingest 进入 WAL。其他租户写操作必须通过同一个 tenant write
sequencer 建立屏障：

- direct commit：先强制 flush 当前租户，再执行 direct commit。
- full-sync：先 flush 前序增量，full-sync 单独形成逻辑 commit，后续请求进入
  新队列窗口。
- disable：停止接收该租户新请求，完成已 durable 的前序请求后再
  disable。
- delete、purge、restore、clone：要求租户队列为空且无 prepared flush。
- compact、GC、index rebuild：等待当前 tenant flush 完成，再取得现有任务锁。

崩溃恢复阶段不允许生命周期或维护任务越过未完成 WAL 记录。

## 16. 背压与故障处理

压力处理顺序：

1. 热缓存超限：淘汰解码对象，保留 WAL offset。
2. append queue 超限：等待 queue timeout。
3. 等待超时：返回 429 和 `Retry-After`。
4. flush batch 达到请求数或字节触发阈值：提前 flush。
5. WAL 达到磁盘上限或剩余空间低于安全水位：停止接收新请求。
6. 已 durable 请求继续 flush，释放 WAL 空间。

对象存储瞬时错误：

- 保持请求为 accepted/prepared；
- 指数退避并加抖动；
- 释放 flush worker；
- 不拆分逻辑 batch。

确定性请求错误：

- 保存失败的 ingest result；
- 按现有规则生成 dead-letter；
- 标记该请求 `FAILED`；
- 不阻塞同租户后续有效请求。

## 17. 配置

建议第一版只暴露必要配置：

```ini
# v1.2.0 本地 single writer 默认性能模式
GRAPHDB_INGEST_MODE=wal
GRAPHDB_INGEST_METADATA_MODE=segment

GRAPHDB_INGEST_WAL_DIR=${GRAPHDB_DATA_DIR}/wal/ingest
GRAPHDB_INGEST_WAL_DURABILITY=sync
GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB
GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=5ms
GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB

GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB
GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK=80
GRAPHDB_INGEST_WAL_HIGH_WATERMARK=70
GRAPHDB_INGEST_WAL_STOP_WATERMARK=85
GRAPHDB_INGEST_MAX_PENDING_AGE=2m
GRAPHDB_INGEST_FLUSH_INTERVAL=250ms
GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=8
GRAPHDB_INGEST_FLUSH_MAX_BYTES=2MiB
GRAPHDB_INGEST_FLUSH_WORKERS=2
GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms
GRAPHDB_INGEST_METADATA_MAX_REQUESTS=256
GRAPHDB_INGEST_METADATA_MAX_BYTES=8MiB
GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2
GRAPHDB_WRITE_CACHE_MAX_BYTES=4GiB
GRAPHDB_WRITE_MAX_COMMIT_TAIL=20000
GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s
```

说明：

- `FLUSH_INTERVAL=250ms` 是最大驻留时间，不是固定轮询周期；graph flush
  trigger 为 8 个请求 / 2 MiB，忙租户可合并同一轮队列。
- `WAL_FSYNC_INTERVAL` 与 graph flush interval 完全独立。
- metadata flush 默认等待 500ms，以 256 个请求或 8 MiB 作为调度 trigger，并
  使用 2 个 worker；同一租户仍保持单个进行中的 flush。
- segment 目标大小第一版使用内部常量，不增加额外运维参数。
- flush worker 数量复用现有 `GRAPHDB_WRITE_MAX_CONCURRENT`。
- 配置为 `wal` 时必须校验 local coordination、single writer topology 和可写
  WAL 目录；不满足条件则启动失败。
- 每租户自动 maintenance 要求 ingest 空闲 1 分钟；后台重型 task 默认单并发，
  避免维护工作与写入在高峰期相互争抢。

上述容量是初始建议值，必须通过实际请求大小、磁盘延迟和对象存储吞吐测试
校准。

## 18. 进程生命周期

启动顺序：

1. 打开并锁定 WAL 目录，防止同一 WAL 被两个进程打开。
2. 扫描 segment 和 checkpoint。
3. 重建 active request index、tenant queue、deadline heap 和 prepared flush。
4. 完成必须的发布确认和 metadata finalize。
5. 启动 scheduler 和 flush workers。
6. readiness 变为 ready。
7. 开始监听数据面 HTTP。

关闭顺序：

1. readiness 变为 not ready。
2. 停止接受新 WAL append。
3. 写完内存 WAL buffer 并 `Sync`。
4. 强制调度所有 ready tenant queue。
5. 在 shutdown timeout 内等待运行中 flush。
6. 未完成请求留在 WAL。
7. 关闭 WAL 文件和进程锁。

不能只依赖当前 HTTP server shutdown；batcher 和 WAL 必须成为显式的服务
生命周期组件。

## 19. 代码组织

建议按职责新增：

- `internal/storage/ingest_wal.go`
  - segment、record codec、append、sync、checkpoint 和 recovery。
- `internal/storage/ingest_wal_writer.go`
  - bounded append queue、内存 buffer、LSN waiter 和 group fsync。
- `internal/storage/ingest_batcher.go`
  - tenant queue、deadline heap、ready FIFO 和 worker admission。
- `internal/storage/ingest_flush.go`
  - ordered logical commits、PREPARED/publish/finalize 和错误隔离。
- `internal/storage/ingest_dedup.go`
  - request digest、semantic digest 和 origin mapping。
- `internal/graph/storage_batch_apply.go`
  - 一次 clone、顺序 apply、逐 commit report 的 storage 快速路径。

修改：

- `internal/httpapi/ingest.go`
  - direct/wal 路由、202 和 committed wait。
- `internal/config/config.go`
  - 配置解析、默认值和部署条件校验。
- `cmd/graphdb/commands.go`
  - WAL/batcher 启动、恢复、readiness 和关闭。
- `internal/observability/metrics.go`
  - WAL、队列、flush 和恢复指标。

不复用现有 task 系统。task 适合长任务和运维操作，不适合低延迟 durable
append。

## 20. 可观测性

全局指标：

```text
graphdb_ingest_wal_append_total
graphdb_ingest_wal_append_bytes_total
graphdb_ingest_wal_buffer_bytes
graphdb_ingest_wal_disk_bytes
graphdb_ingest_wal_written_lsn
graphdb_ingest_wal_durable_lsn
graphdb_ingest_wal_fsync_total
graphdb_ingest_wal_fsync_duration_seconds
graphdb_ingest_wal_group_records

graphdb_ingest_queue_pending_requests
graphdb_ingest_queue_pending_bytes
graphdb_ingest_queue_memory_bytes
graphdb_ingest_queue_cache_hits_total
graphdb_ingest_queue_cache_evictions_total
graphdb_ingest_queue_oldest_seconds

graphdb_ingest_flush_total
graphdb_ingest_flush_duration_seconds
graphdb_ingest_flush_requests
graphdb_ingest_flush_logical_commits
graphdb_ingest_flush_segments
graphdb_ingest_flush_manifest_publishes
graphdb_ingest_flush_exact_dedup_total
graphdb_ingest_flush_fallback_total
graphdb_ingest_wal_recovery_total
```

这些指标不增加 tenant 标签，避免多租户高基数。具体租户、flush ID、LSN
范围和错误原因写入结构化日志和 trace。

readiness 至少检查：

- WAL recovery 是否完成；
- WAL 是否可写；
- WAL 磁盘是否低于安全水位；
- oldest pending request 是否超过最大允许年龄；
- flush worker 是否持续推进 durable backlog。

## 21. 验证方案

测试默认在 OrbStack 中运行。

### 21.1 行为边界测试

- 同租户多个请求在一个 flush 内形成连续逻辑 versions。
- upsert 后 delete 与 delete 后 upsert 保持原请求顺序。
- 多个逻辑 commits 只生成一个 commit segment 和一次 manifest publish。
- 现有 loose tail 与新 commits 合并后仍按版本顺序 replay。
- projected commit tail 超限时先 compact 或 backpressure，不绕过现有阈值。
- 相同幂等身份、相同 payload 共享待处理记录。
- 相同幂等身份、不同 payload 返回 conflict。
- 不同 batch、相同增量内容只产生一次状态变化。
- full-sync、schema、direct commit 和 lifecycle barrier 不越界。
- 一个确定性坏请求不阻止后续有效请求。
- 热缓存淘汰后从 WAL 重读得到相同结果。

### 21.2 崩溃恢复测试

在以下切点执行 SIGKILL 并重启：

- append 前；
- `ACCEPTED` fsync 后；
- `PREPARED` fsync 后；
- commit segment PUT 后；
- manifest 发布后；
- 部分 ingest records 保存后；
- `FINALIZED` 前后；
- segment 轮转和 checkpoint 替换时。

要求：

- durable accepted 请求不丢；
- 不重复增加 graph version；
- 不产生不连续 commit version；
- 已发布但未 finalize 的请求能够补齐 metadata；
- 部分 WAL 尾记录只截断未完整部分。

### 21.3 故障测试

- WAL 磁盘满和只读；
- 对象存储超时、限流和条件写冲突；
- 单个租户持续失败；
- 大量冷租户同时达到 deadline；
- shutdown timeout；
- 进程重启时存在大量已完成和少量未完成 segment。

### 21.4 性能基准

至少覆盖：

- 单租户高频小 batch；
- 多租户，每租户每 10 秒多次写入；
- 多租户，每租户每 10 秒只有一次写入；
- 高比例完全重复内容；
- 大 payload 触发字节阈值；
- 热缓存命中和强制淘汰两种情况。

比较：

- 请求数与 WAL fsync 次数；
- 请求数与 commit/segment object PUT 次数；
- 请求数与 manifest publish 次数；
- graph clone、MD5、index update 次数；
- durable ack latency；
- flush completion latency；
- CPU、allocations、GC、WAL 磁盘和热缓存峰值。

## 22. 验收标准

正确性：

- durable accepted 请求在任意已覆盖的崩溃切点不丢失。
- 同一租户逻辑 commit versions 连续且请求顺序不变。
- manifest 永远不引用不可读取或内容 hash 不匹配的 segment。
- exact dedup 不跨租户、不跨 full-sync，不合并不同内容。
- direct、lifecycle 和维护操作不会越过未完成 ingest。

性能和资源：

- 同租户一个 flush 至多发布一次 manifest。
- 两个及以上变化请求优先写 commit segment，不逐请求 PUT loose commit。
- 正常批处理路径 graph storage copy 和 Content MD5 各执行一次。
- WAL fsync 次数显著低于 accepted 请求数。
- 所有内存和磁盘队列都有硬上限。
- 多租户指标不引入 tenant 高基数标签。

部署边界：

- 本地 writer 默认 `wal + segment + sync`，默认响应为 durable `202`。
- `GRAPHDB_INGEST_MODE=direct` 仍保持同步 200/207 行为，但必须显式启用。
- 现有 reader 无需理解 WAL，只读取原有 manifest 和 commit segment。
- 现有 compact、GC、backup 和 recovery 必须能够处理新生成的短 commit
  segment。

## 23. 实施阶段

### 阶段一：WAL 与恢复闭环

- segmented WAL、内存 buffer、group fsync；
- durable 202、batch status 查询；
- recovery、磁盘限额、进程锁；
- 暂时每个请求单独 flush，验证耐久性。

### 阶段二：Commit segment 攒批

- tenant queue、deadline heap、默认 1 秒；
- ordered logical commits；
- 一次 manifest publish；
- existing loose tail 合并；
- graph batch apply 快速路径和错误回退。

### 阶段三：精确去重与 metadata 收尾

- 活跃请求幂等共享；
- flush 内 semantic digest；
- collector status 按 collector 合并；
- derived state repair queue。

### 阶段四：容量验证与灰度

- OrbStack + RustFS 故障和 SIGKILL 矩阵；
- 多租户 benchmark；
- v1.2.0 以本地 `wal + segment + sync` 作为默认性能 profile；
- PostgreSQL 多 writer 仅保留显式 `direct` 回归路径；
- 5 次 30 分钟运行和回归门禁全部通过后才允许发布。

## 24. 收益边界

如果每个租户在 10 秒内只有一个请求：

- 仍能获得跨租户 group fsync、durable queue、平滑对象存储峰值和背压收益；
- graph commit 数量不会明显下降；
- commit segment 对象数收益有限。

如果同一租户在 10 秒内有多个请求：

- manifest publish 从每请求一次下降为每 flush 一次；
- 多个 logical commits 合并为一个或少量 segment PUT；
- graph load、storage copy、MD5 和 index update 可以显著减少；
- exact duplicate 进一步减少 logical commits。

第一版 ingest metadata object 仍近似随请求数增长，因此最终收益必须同时观察
graph 数据对象和 ingest metadata 两类 PUT，不能只统计 manifest。

## 25. 1.1.3 metadata segment 扩展

1.1.3 以显式配置 `GRAPHDB_INGEST_METADATA_MODE=segment` 关闭上一节保留的
逐请求 metadata 写放大。该模式只允许 local coordination + WAL ingest。

graph manifest 发布后，worker 将同一 graph flush 的请求一次性追加为
`PUBLISHED` WAL 状态。独立的全局 metadata deadline heap 再按租户跨 graph
flush 攒批；达到 500ms、256 请求、8 MiB、shutdown 或
`Prefer: wait=committed` 时：

1. 用一个批量 `PUBLISHED` WAL 记录固化 metadata flush ID 和精确请求边界；
2. 编码并条件创建内容寻址 Parquet metadata segment；
3. CAS 发布独立 ingest metadata manifest；
4. 批量追加 `FINALIZED`，完成请求并允许 WAL prune。

segment 保存完整 request/result、请求摘要、accepted LSN、原请求 trace context
以及每个触达 collector 的窗口最终累计状态。batch、idempotency 和 collector
分别使用固定宽度 Bloom；同一记录同时建立 batch/idempotency identity，不再
复制 payload。

manifest 直接保存 32 个最近引用。溢出的引用写到 level-0 目录；同层超过 8
个目录时，把目录内的 segment 引用和 Bloom 合并到下一层。目录是内容寻址
Parquet 对象，只包含引用，不包含或重写历史 ingest payload。查询按 recent
segment、从新到旧的目录、legacy 对象顺序精确验证 Bloom 候选。

segment key 由 tenant、first/last accepted LSN 和规范 payload hash 决定。
metadata flush ID 在任何对象 PUT 前 fsync；因此 segment PUT 后、manifest CAS
前后或部分 `FINALIZED` 后崩溃，恢复仍使用相同边界和 key。manifest 已含引用
时直接 finalize；只有 segment 未被发布时才继续 CAS。

首次 segment collector 状态优先读取新 segment 历史，然后读取现有 materialized
collector status；若该对象不存在，则扫描 legacy batch 对象恢复累计值。旧对象
不迁移、不删除。存在 segment manifest 是不可逆的 writer marker：1.1.3 legacy
writer 会拒绝该租户，1.1.2 writer 在启用后不再是安全回退目标。

该扩展不改变 graph commit segment、逻辑 version、commit tail 计数、direct
模式或 deadletter 对象。`graphdb_ingest_metadata_*` 指标单独报告物理 segment
PUT、manifest publish、目录 PUT、编码字节、CAS 冲突、Bloom 候选和 WAL replay
字节，避免用逻辑 commit 数推断物理写放大。
