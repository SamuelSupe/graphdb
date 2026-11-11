# 分片 WAL 接入与复制受理队列

这是 main 的开发功能，适用于已启用大图分片且 home group 运行协议 6 的租户。生产化边界见[分片实施说明](large-graph-sharding-implementation.zh-CN.md)。

## 接入契约

home group 的所有副本设置 `GRAPHDB_RAFT_PROTOCOL_VERSION=6` 和 `GRAPHDB_INGEST_MODE=wal`，通过 Router 提交现有批次 API：

```http
POST /v1/ingest/batches
X-Tenant-ID: big
Content-Type: application/json

{"source":"agent","collector_id":"collector","batch_id":"batch-1","idempotency_key":"stable-1","items":[{"entity":{"id":"host:1","kind":"host"}}]}
```

`202` 表示完整规范化请求及其 generation、摘要、受理时间和顺序已由 home group 多数派持久化，返回 `durability=raft_majority`、`accepted_lsn`、`accepted_at`、`generation` 和 `status_url`。它不表示图已可读；`accepted_lsn` 是 home group 日志位置，不能当作图版本。只在受理记录持久化成功后返回，Router 的内存和本地文件不承担持久性。

通过原 `status_url` 查询 `accepted / committed / failed`。状态可经其他 Router 或新 Leader 查询，终态包含 `result`。客户端需要写后立即读时发送 `Prefer: wait=committed`；返回终态成功后使用结果的 `version` 和受理返回的 generation 读取。调用取消或超时只结束等待，不撤销已经持久受理的请求；保留状态 URL，并以原身份重试。分片 home group 使用 `direct` 模式时也经过同一复制队列，但等待终态再响应。

每张图按受理 Raft index 执行批次，活动事务和排队记录互相独立。后台约每 200ms 调度，单轮最多处理 `GRAPHDB_INGEST_FLUSH_MAX_REQUESTS` 个批次，并受现有 5 秒后台工作期限约束；不会为每个批次等待一个 tick，也不等待 `GRAPHDB_INGEST_FLUSH_INTERVAL`。`estimated_flush_at=accepted_at` 表示立即具备调度资格，不是完成期限。每图仍串行发布，当前实现不合并多个批次为一个图事务。

`expected_version`、实体/边前置条件和 atomic/best_effort 沿用现有语义；它们在执行时对当前图版本判断。`value_from=accepted_at` 始终使用第一次持久受理的时间。版本、前置条件、atomic 校验及 atomic 抑制失败分别保持 `409 / 412 / 422 / 409`；通过异步状态查询时，失败原因在 `result.error_code`。版本或前置条件失败、atomic 整批失败不会增加图版本。best_effort 可发布有效项并返回 `failed` 状态及对应版本；这些终态都不会阻塞后续合法批次。

## 持久性和幂等边界

- 同一 generation 内，租户、source、collector、batch 组成批次身份；相同身份的不同正文返回 `409 idempotency_conflict`。idempotency_key 在租户、source、collector 内绑定第一次受理；同内容换 batch ID 重试仍返回原受理和原状态 URL，不新建排队项。
- 队头进入分片事务前验证身份和输入摘要，后面的批次不能越过它。事务的图决定和 ingest 结果一起复制；只有匹配该决定的队列完成命令才能移除排队项。
- Leader 在 `202` 后、输入准备中、图发布后或队列终态更新前退出，新 Leader 都从受理记录及原事务继续。图已经发布而队列仍为 accepted 时，补齐原结果，不重新应用原请求。
- 副本快照包含排队请求、身份别名、活动事务输入和决定。重启重建内存队列索引；终态后缩小请求正文，保留摘要、受理身份及结果。
- `GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES` 沿用复制队列的字节预算，默认 256 MiB，作用于该 home group 的所有租户。满时新请求返回可重试的 `503 ingest_queue_full`，已受理请求和原身份重试继续保留。接入前也检查数据盘空间。编码后的事务输入限 8 MiB、4096 个 item，编码后的 cursor 限 1 MiB，另受依赖闭包和分区变更预算限制。失败和冲突详情合计限约 1 MiB，超过时结果标记 `details_truncated`，计数和错误码照常保留，避免超大的回执阻塞队列。

队列量可从节点状态的 `raft.ingest_queue` 查看；后台失败日志带 tenant、group 和原因，原受理记录继续保留。不同图使用已有的有界后台并发。终态收据及身份别名的自动保留回收仍未实现，不能据此声称磁盘占用已稳定。

## 当前范围

支持增量批次和批次状态。新 `full_sync` 在入队前返回 `501 operation_disabled`，已完成的历史 `full_sync` 可按原身份重试。import、collector/deadletter 管理和带存量数据的 schema 变更仍未开放。

转换前排空旧队列。转换后 home group 继续从保留的源收据读取旧批次状态，重试时按原批次身份或幂等键核对正文，并在当前 generation 保存终态收据。同内容重试返回旧状态 URL、结果和 WAL LSN，正文冲突返回 `409 idempotency_conflict`。这条路径只读取匹配的收据。旧的图读取 generation 会失效，状态查询应使用当前 generation 或省略读取 generation 请求头。分片内的分区搬迁保持原 generation。

先在协议 5 下升级 home group 的全部二进制，再在 home group 的维护窗口内统一切到协议 6。Leader 第一次写入协议 6 命令前核对各副本的协议。持久化的协议下限随 Raft 日志和快照保存，协议 5 二进制会拒绝打开已激活的目录。目录组和仅承载分区数据的组可以继续使用协议 5。曾用早期开发版在协议 5 接收批次的集群，应从支持其现有日志的二进制起升级。未完成批次在切到协议 6 后继续处理。

## 验证入口

- `TestPartitionIngestDecisionSurvivesLostResponses`：丢失 begin、prepare、commit、finalize、complete 响应，重试保持原结果和图版本。
- `TestPartitionIngestTransactionalFailuresAndRecovery`：版本/前置条件/atomic 失败、accepted_at 条件以及后续 best_effort 批次。
- `TestHAPartitionIngestDurableQueueAndLeaderRecovery`：真实九节点 HTTP 复制，数据组故障期间受理、排队预算、独立快照安装、全部 home 副本重启、图已发布但队列未完成时切主、跨 Router 幂等重试，以及 home 少数派拒绝持久化受理。
- `TestHAPartitionIngestUpgradePreservesSourceReceipts`：验证混合协议拒绝激活、转换后旧状态 URL、历史 `full_sync` 重放、正文冲突及切主后的终态。
- `scripts/partition_wal_gate.py`：暂停数据组并强杀 Leader、全部 home 副本及两个 Router。确认原卷重启后能读取待完成批次，再通过离线归档恢复到新卷，核对原受理时间、LSN、提交顺序、内容及重试后的图版本。可通过 `GRAPHDB_GATE_PROTOCOL5_IMAGE` 检查旧二进制拒绝打开已激活的归档。

运行容器故障门禁时，先构建本地候选镜像，再设置 `GRAPHDB_GATE_IMAGE` 和一个不存在的 `GRAPHDB_GATE_OUTPUT` 目录执行脚本。它只创建和清理自己随机命名的 Compose 项目，不发布镜像、标签或版本。端口可用 `GRAPHDB_GATE_PORT_OFFSET` 平移；地址池不足时可通过 `GRAPHDB_GATE_SUBNET` 指定空闲子网。这些是单机上的进程故障验证，不等于宿主机断电、多主机容灾或容量压测。

2026-11-11 在 OrbStack Linux arm64、Go 1.27.3 验证。九副本和两个 Router 的独立容器门禁通过：受理 LSN `46` 和原时间跨 Leader 强杀、全 home 组及 Router 强杀保持不变；恢复数据组后两批分别发布版本 `2`、`3`，重试保留版本 `3` 和原始字段内容。分片、既有 HA WAL、严格失败响应及回执上限的定向回归通过，主体全仓回归亦通过。最终候选通过分片、九节点 HA 和 HTTP 严格错误响应的 race 检查及全仓 vet。源码摘要及原始日志在本次 worktree 的 `.workflow/partition-wal/`。

协议 6 验证沿用 OrbStack Linux arm64 和 Go 1.27.3。九个副本的离线归档恢复到新卷后，原受理 LSN `71` 和受理时间保持，待完成批次依序发布版本 `3`、`4`。原跨组关系的正反查询均通过，重复提交保留原结果。用 `75ec1e8` 构建的协议 5 二进制拒绝打开恢复出的 home 组目录。运行记录见 `.workflow/partition-production/recovery-with-edge/`。

首轮全仓运行还发现既有 `receipt-check` CLI 损坏页用例偶发返回通过；在未改动的 `75ec1e8` 基线上运行 30 次复现 5 次。本轮未改动该命令。
