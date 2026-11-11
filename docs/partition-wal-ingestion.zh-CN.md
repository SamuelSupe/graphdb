# 分片 WAL 接入与复制受理队列

这是 main 的开发功能，适用于已启用大图分片且 home group 运行协议 6 或更高版本的租户。生产化边界见[分片实施说明](large-graph-sharding-implementation.zh-CN.md)。

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

队列量可从节点状态的 `raft.ingest_queue` 查看；后台失败日志带 tenant、group 和原因，未完成的受理记录继续保留。不同图使用已有的有界后台并发。

### 终态回执保留期

在协议 7 home group 的每个副本设置 `GRAPHDB_PARTITION_INGEST_RECEIPT_RETENTION`，例如 `168h`。默认 `0s`，永久保留。受理时将保留期随批次复制，到期时间从 `committed` 或 `failed` 终态记录的 `finished_at` 起算。配置变更只影响新受理的原生分片批次。既有回执、转换前来源回执及它们的重试仍沿用原规则。

设置保留期的批次返回含 `accepted_lsn` 查询参数的 `status_url`。终态响应提供 `receipt_expires_at`。到期后由后台分批回收原回执和对应幂等别名。在保留期内，用原批次 ID 或原幂等键重试返回原受理。回收完成后，原状态 URL 返回 `404`，相同批次 ID 或幂等键的再次提交可成为新批次。采集端应保存完整状态 URL，并在保留期内完成重试。

## 磁盘和快照

home group 启用协议 7 后，分片 WAL 事务引用已有的受理正文。Leader 恢复和快照安装都按租户、代次和摘要校验该引用。参与分区完成后，同一条 Raft 命令保存终态受理回执并删除事务里的重复结果。状态 URL、原 LSN、终态结果和幂等别名继续保留。升级时尚未完成的旧式暂存事务仍可继续。

每次启动后，协议 7 的 Leader 分批检查一次历史受理记录。每步最多检查 64 条，读取预算约 4 MiB，较大的单条记录独自检查，步骤间隔 1 秒。只有原终态回执仍在、结果相同、事务无输入和待收尾分区且不再活动时，才通过 Raft 删除重复事务文件。完成一轮后退出，失败按指数退避重试并记日志。`partition_receipt_cleanup` 操作指标记录步骤耗时及失败。

图 GC 在历史视图剪枝后清理当前代次的未认领暂存输入，每步最多 32 个文件、8 MiB。Leader 在应用屏障外列出候选，Raft 应用只核对有界清单并重新保护当前活动事务。若回收先于 `begin` 生效，协议 7 返回可重试的 `graph_input_expired`，协调器重新暂存并按原身份继续；活动事务缺输入仍按恢复错误处理。历史日志按原修订号执行。

设置保留期的终态回执转入独立目录，以哈希分桶，重启重建待处理队列时跳过该目录。Leader 每步检查最多 64 条、约 4 MiB，步骤间隔 200 ms。扫描完一轮后最多等待一分钟再扫描。应用删除前重新核对到期时间和原受理 LSN，回执和对应别名在同一次应用里删除。`partition_receipt_expiry` 记录步骤耗时和失败。到期到实际回收的延迟取决于扫描积压和 Leader 可用性。

Raft 删除文件并持久化检查点后，会尝试回收空父目录，回收失败或超时时由下次启动清理。已经被快照硬链接固定的文件继续由该快照持有，直到快照构建结束。

## 当前范围

支持增量批次和批次状态。新 `full_sync` 在入队前返回 `501 operation_disabled`，已完成的历史 `full_sync` 可按原身份重试。import、collector/deadletter 管理和带存量数据的 schema 变更仍未开放。

转换前排空旧队列。转换后 home group 继续从保留的源收据读取旧批次状态，重试时按原批次身份或幂等键核对正文，并在当前 generation 保存终态收据。同内容重试返回旧状态 URL、结果和 WAL LSN，正文冲突返回 `409 idempotency_conflict`。这条路径只读取匹配的收据。旧的图读取 generation 会失效，状态查询应使用当前 generation 或省略读取 generation 请求头。分片内的分区搬迁保持原 generation。

先在协议 5 下升级 home group 的全部二进制，再在 home group 的维护窗口内统一切到协议 6。Leader 第一次写入协议 6 命令前核对各副本的协议。持久化的协议下限随 Raft 日志和快照保存，协议 5 二进制会拒绝打开已激活的目录。目录组和仅承载分区数据的组可以继续使用协议 5。曾用早期开发版在协议 5 接收批次的集群，应从支持其现有日志的二进制起升级。未完成批次在切到协议 6 后继续处理。

启用磁盘优化时，先保持原协议更新 Router 和 home group 的全部副本，再在维护窗口将 home group 统一设置为 `GRAPHDB_RAFT_PROTOCOL_VERSION=7`。协议 7 持久化后，最高只支持协议 6 的二进制会拒绝启动该目录。

## 验证入口

- `TestPartitionIngestDecisionSurvivesLostResponses`：丢失 begin、prepare、commit、finalize、complete 响应，重试保持原结果和图版本。
- `TestPartitionIngestTransactionalFailuresAndRecovery`：版本/前置条件/atomic 失败、accepted_at 条件以及后续 best_effort 批次。
- `TestHAPartitionIngestDurableQueueAndLeaderRecovery`：真实九节点 HTTP 复制，数据组故障期间受理、排队预算、独立快照安装、全部 home 副本重启、图已发布但队列未完成时切主、跨 Router 幂等重试，以及 home 少数派拒绝持久化受理。
- `TestHAPartitionIngestUpgradePreservesSourceReceipts`：验证混合协议拒绝激活、转换后旧状态 URL、历史 `full_sync` 重放、正文冲突及切主后的终态。
- `scripts/partition_wal_gate.py`：暂停数据组并强杀 Leader、全部 home 副本及两个 Router。确认原卷重启后能读取待完成批次，再通过离线归档恢复到新卷，核对原受理时间、LSN、提交顺序、内容及重试后的图版本。可通过 `GRAPHDB_GATE_PROTOCOL5_IMAGE` 检查旧二进制拒绝打开已激活的归档。

运行容器故障门禁时，先构建本地候选镜像，再设置 `GRAPHDB_GATE_IMAGE` 和一个不存在的 `GRAPHDB_GATE_OUTPUT` 目录执行脚本。home group 默认使用协议 7，可通过 `GRAPHDB_GATE_HOME_PROTOCOL=6` 验证原路径，或通过 `GRAPHDB_GATE_PREVIOUS_HOME_IMAGE` 验证旧二进制拒绝打开已激活的归档。脚本只创建和清理自己随机命名的 Compose 项目。端口可用 `GRAPHDB_GATE_PORT_OFFSET` 平移；地址池不足时可通过 `GRAPHDB_GATE_SUBNET` 指定空闲子网。这些记录覆盖单机上的进程故障，宿主机断电、多主机容灾及容量仍需独立验收。

加上 `GRAPHDB_GATE_RECEIPT_RETENTION=20s` 可验证短保留期下的故障恢复、状态地址与 LSN 绑定、身份重用和持续受理后的回收。

2026-11-11 在 OrbStack Linux arm64、Go 1.27.3 验证。九副本和两个 Router 的独立容器门禁通过：受理 LSN `46` 和原时间跨 Leader 强杀、全 home 组及 Router 强杀保持不变；恢复数据组后两批分别发布版本 `2`、`3`，重试保留版本 `3` 和原始字段内容。分片、既有 HA WAL、严格失败响应及回执上限的定向回归通过，主体全仓回归亦通过。最终候选通过分片、九节点 HA 和 HTTP 严格错误响应的 race 检查及全仓 vet。源码摘要及原始日志在本次 worktree 的 `.workflow/partition-wal/`。

协议 6 验证沿用 OrbStack Linux arm64 和 Go 1.27.3。九个副本的离线归档恢复到新卷后，原受理 LSN `71` 和受理时间保持，待完成批次依序发布版本 `3`、`4`。原跨组关系的正反查询均通过，重复提交保留原结果。用 `75ec1e8` 构建的协议 5 二进制拒绝打开恢复出的 home 组目录。运行记录见 `.workflow/partition-production/recovery-with-edge/`。

首轮全仓运行还发现既有 `receipt-check` CLI 损坏页用例偶发返回通过；在未改动的 `75ec1e8` 基线上运行 30 次复现 5 次。本轮未改动该命令。

协议 7 在 OrbStack Linux arm64 上通过同一九副本、双 Router 故障门禁。新卷恢复保留原 LSN `68`，两批依序提交，原跨组边和幂等结果保持，协议 6 二进制拒绝启动该 home 组。协议 6/7 的同一回归负载包含一个 64 KiB 随机值编码的 cursor。活动批次的压缩 home 快照从 167,203 降至 83,689 字节，完成后从 169,229 降至 84,514 字节。两种快照都在独立目录安装并校验。数据图页占比更高的负载会得到不同的整组缩减比例。记录见 `.workflow/partition-storage/`。

历史清理回归将含旧终态回执的 home group 从协议 6 切到 7，原批次和幂等别名均返回原 LSN 和状态 URL。压缩快照从 169,226 降至 84,522 字节，安装后仍能读到原结果。九副本容器门禁在新卷恢复后完成图 GC，三个 home 副本各清掉 12 个遗弃输入文件（384 KiB 原始数据）及空目录。引用已回收输入的 `begin` 返回可重试冲突，幂等重试保持原结果和图摘要。竞态测试及全仓 `go vet` 通过。记录见 `.workflow/partition-storage-round2/`。

启用 20 秒保留期后，九副本容器门禁通过 Leader 强杀、全 home 组重启和新卷恢复，原 LSN `82` 与保留期内的幂等结果保持。回收后重用原批次 ID 和别名，旧状态 URL 仍为 `404`，新批次仅发布一次。额外连续受理 72 批，到期后逐一检查三个 home 副本，终态回执与别名目录内均剩余 0 个文件。快照恢复和旧清理命令保护新 LSN 的竞态回归通过。记录见 `.workflow/partition-storage-round3/`。此负载未验收生产规模的长期占用或回收吞吐。
