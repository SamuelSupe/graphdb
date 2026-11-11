# 分片 WAL 接入与复制受理队列

这是 main 的开发功能，尚未发布；2.5.1 的发行包不包含它。适用范围是已启用协议 5 大图分片的租户，生产化剩余边界见[分片实施说明](large-graph-sharding-implementation.zh-CN.md)。

## 接入契约

home group 的所有副本设置 `GRAPHDB_INGEST_MODE=wal`，通过 Router 提交现有批次 API：

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

仅接入增量批次和批次状态。`full_sync` 需要后续来源对账任务，在入队前返回 `501 operation_disabled`；import、collector/deadletter 管理及带存量数据的 schema 变更仍未开放。

队列绑定分片 generation。旧单组转换前必须排空旧队列并完成客户端交接；本轮没有迁移转换前的历史 WAL 状态和 ingest 身份，不能依靠旧状态 URL 或旧身份跨转换续接。分片内的分区搬迁不改变 generation。

沿用协议 5 预览版的维护窗口升级：先统一升级 Router、home group 和全部数据组，再恢复流量。新队列命令和 ingest 事务结果不受旧二进制支持，不承诺混合版本或原目录直接回退。

## 验证入口

- `TestPartitionIngestDecisionSurvivesLostResponses`：丢失 begin、prepare、commit、finalize、complete 响应，重试保持原结果和图版本。
- `TestPartitionIngestTransactionalFailuresAndRecovery`：版本/前置条件/atomic 失败、accepted_at 条件以及后续 best_effort 批次。
- `TestHAPartitionIngestDurableQueueAndLeaderRecovery`：真实九节点 HTTP 复制，数据组故障期间受理、排队预算、独立快照安装、全部 home 副本重启、图已发布但队列未完成时切主、跨 Router 幂等重试，以及 home 少数派拒绝持久化受理。
- `scripts/partition_wal_gate.py`：独立 Linux 容器，暂停数据组；`SIGKILL` home Leader，再强杀全部 home 副本和两个 Router；使用原卷重启并恢复数据组，核对原受理时间、LSN、顺序、内容和重复提交后的图版本。

运行容器故障门禁时，先构建本地候选镜像，再设置 `GRAPHDB_GATE_IMAGE` 和一个不存在的 `GRAPHDB_GATE_OUTPUT` 目录执行脚本。它只创建和清理自己随机命名的 Compose 项目，不发布镜像、标签或版本。端口可用 `GRAPHDB_GATE_PORT_OFFSET` 平移；地址池不足时可通过 `GRAPHDB_GATE_SUBNET` 指定空闲子网。这些是单机上的进程故障验证，不等于宿主机断电、多主机容灾或容量压测。

2026-11-11 在 OrbStack Linux arm64、Go 1.27.3 验证。九副本和两个 Router 的独立容器门禁通过：受理 LSN `46` 和原时间跨 Leader 强杀、全 home 组及 Router 强杀保持不变；恢复数据组后两批分别发布版本 `2`、`3`，重试保留版本 `3` 和原始字段内容。分片、既有 HA WAL、严格失败响应及回执上限的定向回归通过，主体全仓回归亦通过。最终候选通过分片、九节点 HA 和 HTTP 严格错误响应的 race 检查及全仓 vet。源码摘要及原始日志在本次 worktree 的 `.workflow/partition-wal/`。

首轮全仓运行还发现既有 `receipt-check` CLI 损坏页用例偶发返回通过；在未改动的 `75ec1e8` 基线上运行 30 次复现 5 次。本轮未改动该命令。
