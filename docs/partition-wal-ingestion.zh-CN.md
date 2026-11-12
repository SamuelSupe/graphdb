# 分片 WAL 接入与复制受理队列

适用于 **GGraphDB 2.6.0-preview.1** 中已启用大图分片且 home group 运行协议 6 或更高版本的租户。使用范围见[分片实施说明](large-graph-sharding-implementation.zh-CN.md)，启用前请核对[协议与升级步骤](#显式启用与升级)。

## 接入契约

在 home group 的所有副本统一配置协议 6 或更高版本，并设置 `GRAPHDB_INGEST_MODE=wal`。通过 Router 提交现有批次 API。

```http
POST /v1/ingest/batches
X-Tenant-ID: big
Content-Type: application/json

{"source":"agent","collector_id":"collector","batch_id":"batch-1","idempotency_key":"stable-1","items":[{"entity":{"id":"host:1","kind":"host"}}]}
```

`202` 表示完整规范化请求及其 generation、摘要、受理时间和顺序已由 home group 多数派持久化，返回 `durability=raft_majority`、`accepted_lsn`、`accepted_at`、`generation` 和 `status_url`。它不表示图已可读；`accepted_lsn` 是 home group 日志位置，不能当作图版本。只在受理记录持久化成功后返回，Router 的内存和本地文件不承担持久性。

通过原 `status_url` 查询 `accepted / committed / failed`。状态可经其他 Router 或新 Leader 查询，终态包含 `result`。客户端需要写后立即读时发送 `Prefer: wait=committed`；返回终态成功后使用结果的 `version` 和受理返回的 generation 读取。调用取消或超时只结束等待，不撤销已经持久受理的请求；保留状态 URL，并以原身份重试。分片 home group 使用 `direct` 模式时也经过同一复制队列，但等待终态再响应。

每张图按受理 Raft index 执行批次，活动事务和排队记录互相独立。后台约每 200ms 调度，单轮最多处理 `GRAPHDB_INGEST_FLUSH_MAX_REQUESTS` 个批次。协议 6 和 7 的后台工作期限为 5 秒。协议 8 为事务和维护步骤分别设置期限，图 GC 期间在短写入轮次之间推进回收。分片队列无需等待 `GRAPHDB_INGEST_FLUSH_INTERVAL`。`estimated_flush_at=accepted_at` 表示立即具备调度资格。每张图串行发布，每个 WAL 批次独立执行前置条件和版本检查。

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

Raft 删除文件并持久化检查点后，每个副本在本地回收空父目录。批量删除进入有界队列，后台分批检查目录是否仍为空，短暂的读取争用会重试。未能完成的目录清理可在下次启动时补齐。已经被快照硬链接固定的文件继续由该快照持有，直到快照构建结束。

### 协议 8 的跨组提交和回收

home group 在固定图版本和布局 revision 上提前规划 WAL 批次。Raft 受理事务时确认队头、版本、布局和输入摘要，并登记参与分区。控制组将准备凭证和图版本一起持久化，参与分区收尾后，在同一次 Raft 应用中保存终态回执并清理活动事务。重启后从持久受理正文和活动事务恢复。

支持协议 8 的数据组可将最多 32 个分区的同阶段操作写成一条有界 Raft 命令，图页回收每条最多推进 16 个分区。小型变更先在内存生成，再统一记录回滚所需的原始内容，最后持久化。图页回收单分区每步最多标记 32 页或扫描 128 个对象，扫描阶段沿用每步 8 MiB 的删除预算。使用较早协议的数据组沿用逐分区操作。

### 协议 9 的图页保护

协议 9 数据组启动的新回收周期将最多 128 个待合并的页哈希随分区状态持久化，超过上限时批量合并到标记树。扫描同时查两份保护记录，快照校验覆盖两份记录引用的图页。标记和删除预算沿用协议 8。使用协议 9 的 home group 在 GC 启动写栅栏内连续推进有界的安装批次，以缩短等待。

### 协议 10 的扫页清单和写入调度

协议 10 数据组的 Leader 在应用锁外枚举最多 128 个页候选，将清单和预期游标写入 Raft。应用时重新检查分区 epoch、GC 身份、阶段、游标和当前保护标记，然后按原删除字节预算推进。清单生成期间新增的页可以留到下一轮回收。历史日志按原方式重放。

协议 10 的 home group 可在一个有界 GC 步骤运行时规划下一批 WAL，待该步骤完成后顺序提交。GC 启动仍需安装保护边界。Raft 应用的阶段耗时可从 `graphdb_ha_operation_seconds` 查看，操作名包括 `partition_gc_scan`、`partition_gc_apply`、`replication_before_images`、`replication_object_sync` 和 `partition_wal_read_barrier`。回滚日志和同步指标覆盖相应的复制文件操作。

home group 的 `partition_wal_execute` 从取出队头后计时。可用 `partition_txn_plan`、`partition_txn_wait_gc`、`partition_txn_begin`、`partition_txn_prepare`、`partition_txn_commit`、`partition_txn_finalize` 和 `partition_txn_complete` 查看事务阶段。数据组的 `partition_fragment_prepare` 衡量本地应用处理，`replication_journal_open`、`replication_checkpoint`、`replication_sync_files` 和 `replication_sync_directories` 衡量相应的落盘步骤。

缓冲的分区准备和收尾命令在批量记录回滚数据后，按文件分派给有界写入协程。各协程在关闭临时文件前完成文件同步。重命名仍由复制日志保护，目录同步完成后才推进应用检查点。

### 协议 11 的均衡叶页分裂

协议 11 数据组为新提交的分区准备命令记录修订号 7。已有叶页溢出且能容纳在两个叶页时，选择最接近按行数平分的有效分界。两个新页沿用原有的行数和字节预算及大记录单行例外。需要三个或更多页的批量更新沿用原有装页规则。

旧修订号的准备命令按原规则重放，产生原有的页根。重试已准备的事务复用持久化的准备结果。现有页格式、未触及的页、批量初始化以及 GC 和快照配置沿用原规则。

## 当前范围

支持增量批次和批次状态。新 `full_sync` 在入队前返回 `501 operation_disabled`，已完成的历史 `full_sync` 可按原身份重试。import、collector/deadletter 管理和带存量数据的 schema 变更仍未开放。

转换前排空旧队列。转换后 home group 继续从保留的源收据读取旧批次状态，重试时按原批次身份或幂等键核对正文，并在当前 generation 保存终态收据。同内容重试返回旧状态 URL、结果和 WAL LSN，正文冲突返回 `409 idempotency_conflict`。这条路径只读取匹配的收据。旧的图读取 generation 会失效，状态查询应使用当前 generation 或省略读取 generation 请求头。分片内的分区搬迁保持原 generation。

先在协议 5 下升级 home group 的全部二进制，再在 home group 的维护窗口内统一切到协议 6。Leader 第一次写入协议 6 命令前核对各副本的协议。持久化的协议下限随 Raft 日志和快照保存，协议 5 二进制会拒绝打开已激活的目录。目录组和仅承载分区数据的组可以继续使用协议 5。曾用早期开发版在协议 5 接收批次的集群，应从支持其现有日志的二进制起升级。未完成批次在切到协议 6 后继续处理。

启用磁盘优化时，先保持原协议更新 Router 和 home group 的全部副本，再在维护窗口将 home group 统一设置为 `GRAPHDB_RAFT_PROTOCOL_VERSION=7`。协议 7 持久化后，最高只支持协议 6 的二进制会拒绝启动该目录。

## 显式启用与升级

Raft 默认协议仍为 1。对预览租户，先按[管理接口](large-graph-sharding-implementation.zh-CN.md#管理接口)显式启用大图分片。需要异步受理时，另在 home group 的每个副本设置 `GRAPHDB_INGEST_MODE=wal`。

按组为全部投票副本设置 `GRAPHDB_RAFT_PROTOCOL_VERSION`。在 Compose 部署中，通过附加的 Compose 配置为每个副本的 `environment` 显式设置。目录组可继续使用协议 5。

| 协议 | home group | 承载图分区的数据组 |
| --- | --- | --- |
| 8 | WAL 提前规划、合批协调、终态回执随事务清理持久化 | 分区准备、收尾和 GC 批处理。低于 8 的数据组使用逐分区请求 |
| 9 | 连续安装有界 GC 保护批次 | 新启动的 GC 周期合并页保护标记。在途周期按原格式完成 |
| 10 | 在有界 GC 步骤期间并行规划下一批 WAL | Leader 在应用锁外准备扫页清单 |
| 11 | 同时承载图分区时启用该组的均衡分裂 | 仅新准备命令启用均衡分裂。历史日志和已准备事务保留原页根 |

1. 在测试环境恢复一份完整的离线集群归档，并核对图版本、内容摘要和 WAL 状态。为已有组安排维护窗口，暂停写入和迁移，确认副本健康及追赶位置。
2. 保持原协议更新 Router 和相关组的全部副本。通过私有 `/raft/status` 核对每个投票副本的 `protocol_max` 覆盖目标版本。
3. 在独立维护步骤中，为目标组的每个投票副本统一配置目标协议并重启。所有副本报告目标 `protocol_version` 后，检查组健康及读写，再恢复流量。首次激活协议 6 及以上时，Leader 会核对全组配置。
4. 组内日志或快照持久化新协议后，原目录的协议下限随之提高。支持新协议的二进制会保留该下限，配置更小的协议号也不会降低它。低于下限的旧二进制会拒绝原目录。需要回到旧版本时，使用激活前、已验证可恢复的完整归档和隔离目录。

现有自动滚动脚本只接受协议 1–4 的 inventory。协议 5 及以上使用上述维护窗口。当前预览的协议 11 门禁验证了故障后的恢复及旧二进制拒绝打开高协议目录，真实跨版本大图滚动升级仍待验收。

以下为九副本 Compose 示例，使用 `shard-a` 作为预览租户的 home group。保存为 `compose.preview.yml`，与 `docker-compose.sharded.yml` 叠加。已有数据卷按上面的升级顺序操作。

```yaml
x-preview-catalog: &preview-catalog
  GRAPHDB_RAFT_PROTOCOL_VERSION: "5"
x-preview-home: &preview-home
  GRAPHDB_RAFT_PROTOCOL_VERSION: "11"
  GRAPHDB_INGEST_MODE: wal
x-preview-data: &preview-data
  GRAPHDB_RAFT_PROTOCOL_VERSION: "11"
services:
  catalog1: {environment: *preview-catalog}
  catalog2: {environment: *preview-catalog}
  catalog3: {environment: *preview-catalog}
  a1: {environment: *preview-home}
  a2: {environment: *preview-home}
  a3: {environment: *preview-home}
  b1: {environment: *preview-data}
  b2: {environment: *preview-data}
  b3: {environment: *preview-data}
```

新建试验集群时，设置 `GRAPHDB_RAFT_TOKEN` 后运行 `docker compose -f docker-compose.sharded.yml -f compose.preview.yml up -d --build`。注册数据组后，通过 Router 创建并核对租户归属，再调用[分片管理接口](large-graph-sharding-implementation.zh-CN.md#管理接口)将指定租户切换为分片图。

## 验证入口

- `TestPartitionIngestDecisionSurvivesLostResponses`：丢失 begin、prepare、commit、finalize、complete 响应，重试保持原结果和图版本。
- `TestPartitionIngestTransactionalFailuresAndRecovery`：版本/前置条件/atomic 失败、accepted_at 条件以及后续 best_effort 批次。
- `TestHAPartitionIngestDurableQueueAndLeaderRecovery`：真实九节点 HTTP 复制，数据组故障期间受理、排队预算、独立快照安装、全部 home 副本重启、图已发布但队列未完成时切主、跨 Router 幂等重试，以及 home 少数派拒绝持久化受理。
- `TestHAPartitionIngestUpgradePreservesSourceReceipts`：验证混合协议拒绝激活、转换后旧状态 URL、历史 `full_sync` 重放、正文冲突及切主后的终态。
- `TestBalancedLeafPreparePreservesLegacyReplay`：验证旧修订号页根、修订号 7 的分裂结果以及切换后的准备重试。
- `scripts/partition_wal_gate.py`：暂停数据组并强杀 Leader、全部 home 副本及两个 Router。确认原卷重启后能读取待完成批次，再通过离线归档恢复到新卷，核对原受理时间、LSN、提交顺序、内容及重试后的图版本。可通过 `GRAPHDB_GATE_PROTOCOL5_IMAGE` 检查旧二进制拒绝打开已激活的归档。

运行容器故障门禁时，先构建本地候选镜像，再设置 `GRAPHDB_GATE_IMAGE` 和一个不存在的 `GRAPHDB_GATE_OUTPUT` 目录执行脚本。home group 默认使用协议 7，可通过 `GRAPHDB_GATE_HOME_PROTOCOL=6` 验证原路径，或通过 `GRAPHDB_GATE_PREVIOUS_HOME_IMAGE` 验证旧二进制拒绝打开已激活的归档。脚本只创建和清理自己随机命名的 Compose 项目。端口可用 `GRAPHDB_GATE_PORT_OFFSET` 平移；地址池不足时可通过 `GRAPHDB_GATE_SUBNET` 指定空闲子网。这些记录覆盖单机上的进程故障，宿主机断电、多主机容灾及容量仍需独立验收。

加上 `GRAPHDB_GATE_RECEIPT_RETENTION=20s` 可验证短保留期下的故障恢复、状态地址与 LSN 绑定、身份重用和持续受理后的回收。

2026-11-11 在 OrbStack Linux arm64、Go 1.27.3 验证。九副本和两个 Router 的独立容器门禁通过：受理 LSN `46` 和原时间跨 Leader 强杀、全 home 组及 Router 强杀保持不变；恢复数据组后两批分别发布版本 `2`、`3`，重试保留版本 `3` 和原始字段内容。分片、既有 HA WAL、严格失败响应及回执上限的定向回归通过，主体全仓回归亦通过。最终候选通过分片、九节点 HA 和 HTTP 严格错误响应的 race 检查及全仓 vet。源码摘要及原始日志在本次 worktree 的 `.workflow/partition-wal/`。

协议 6 验证沿用 OrbStack Linux arm64 和 Go 1.27.3。九个副本的离线归档恢复到新卷后，原受理 LSN `71` 和受理时间保持，待完成批次依序发布版本 `3`、`4`。原跨组关系的正反查询均通过，重复提交保留原结果。用 `75ec1e8` 构建的协议 5 二进制拒绝打开恢复出的 home 组目录。运行记录见 `.workflow/partition-production/recovery-with-edge/`。

首轮全仓运行还发现既有 `receipt-check` CLI 损坏页用例偶发返回通过；在未改动的 `75ec1e8` 基线上运行 30 次复现 5 次。本轮未改动该命令。

协议 7 在 OrbStack Linux arm64 上通过同一九副本、双 Router 故障门禁。新卷恢复保留原 LSN `68`，两批依序提交，原跨组边和幂等结果保持，协议 6 二进制拒绝启动该 home 组。协议 6/7 的同一回归负载包含一个 64 KiB 随机值编码的 cursor。活动批次的压缩 home 快照从 167,203 降至 83,689 字节，完成后从 169,229 降至 84,514 字节。两种快照都在独立目录安装并校验。数据图页占比更高的负载会得到不同的整组缩减比例。记录见 `.workflow/partition-storage/`。

历史清理回归将含旧终态回执的 home group 从协议 6 切到 7，原批次和幂等别名均返回原 LSN 和状态 URL。压缩快照从 169,226 降至 84,522 字节，安装后仍能读到原结果。九副本容器门禁在新卷恢复后完成图 GC，三个 home 副本各清掉 12 个遗弃输入文件（384 KiB 原始数据）及空目录。引用已回收输入的 `begin` 返回可重试冲突，幂等重试保持原结果和图摘要。竞态测试及全仓 `go vet` 通过。记录见 `.workflow/partition-storage-round2/`。

启用 20 秒保留期后，九副本容器门禁通过 Leader 强杀、全 home 组重启和新卷恢复，原 LSN `82` 与保留期内的幂等结果保持。回收后重用原批次 ID 和别名，旧状态 URL 仍为 `404`，新批次仅发布一次。额外连续受理 72 批，到期后逐一检查三个 home 副本，终态回执与别名目录内均剩余 0 个文件。快照恢复和旧清理命令保护新 LSN 的竞态回归通过。记录见 `.workflow/partition-storage-round3/`。此负载未验收生产规模的长期占用或回收吞吐。
