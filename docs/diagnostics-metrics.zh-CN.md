# 诊断指标与采集

单机 direct/WAL、单组 Raft、catalog、数据分片和 router 均提供本地诊断。数据库节点使用管理监听器 `GET /metrics`、`GET /v1/diagnostics`；兼容单监听器部署也保留入口。router 使用自身监听器上的同名路径，必须携带 `Authorization: Bearer <router token>`，只接受 GET。

采集每个进程的私有地址，不能经过只选择 Leader 或 readiness 正常成员的负载均衡器。drain、quorum 丢失、catalog 不可达时仍能采集。指标采集不访问 Leader、请求读屏障、遍历持久化对象或获取共享应用屏障。磁盘检查使用文件系统状态调用；缓存仅遍历已受限的本地 leader/placement 缓存及当前 Raft 成员。

## 覆盖范围

| 排查方向 | 主要指标 |
| --- | --- |
| 请求与查询 | 既有 `graphdb_http_requests_total`、`graphdb_http_request_duration_seconds`、`graphdb_queries_total`、查询耗时、慢查询、读版本及 catch-up；`graphdb_queries_running` 为本进程运行或等待准入的查询数 |
| 并发与排队 | `graphdb_admission_{enabled,active,waiting,global_limit,per_tenant_limit,queue_timeout_seconds}{pool}（query/read/write）`；已有读、写准入等待直方图和背压原因计数 |
| 存储容量 | `graphdb_filesystem_{inspection_success,total_bytes,available_bytes,minimum_free_bytes,write_ready}{role}（data/wal/raft）`；分别检查配置的真实目录。相同文件系统的各角色容量不能相加；JSON 中 `filesystem_id` 可识别共享磁盘。旧 `graphdb_disk_*` 数据磁盘指标继续保留 |
| 单机 WAL | 既有 WAL append、字节、fsync 耗时/失败、写入及持久化 LSN、磁盘/缓冲占用、接入积压数量/字节/最老年龄、flush 批量/耗时、恢复及去重指标 |
| Raft 状态 | `graphdb_raft_node_info`、`state`、`term`、`commit_index`、`applied_index`、`application_lag`、`leader_known`、`leader_changes_total`、`voters`、`learners`、`draining`、`protocol_version`、`failed` |
| Raft 提案与应用 | `proposal_bytes`、`pending_proposals`、`pending_reads`、`application_bytes`、`application_commits_total`、`application_entries_total`；操作耗时和当前并发见下文 |
| 副本复制与发送 | `graphdb_raft_peer_{progress_known,match_index,next_index,replication_lag,recent_active,learner,paused,send_queue,inflight_messages}{peer_id}`；`graphdb_raft_events_total{event="transport_queue_full"}` 记录有界发送队列丢弃 |
| 快照与 Raft 磁盘 | `graphdb_raft_snapshot_{index,bytes,failed}`、`graphdb_raft_storage_{inspection_success,bytes}`；快照捕获、构建、持久化、发送、恢复的耗时/失败；数据库大小包含可重用页面，不代表有效日志大小 |
| Raft 接入积压 | `graphdb_raft_ingest_{observation_known,pending_requests,pending_bytes,observed_timestamp_seconds}`；采用现有本地接入缓存，不在 scrape 时重建。缓存未初始化或被生命周期/恢复操作失效时 known=0，缺失数量不能当作零 |
| 维护及迁移 | 既有维护阶段耗时、内存池估算、任务失败转换及索引健康；`graphdb_ha_*` 提供 maintenance capture/prepare/poll、ingest flush poll、租户分配和各迁移阶段的操作统计 |
| catalog | `graphdb_catalog_{observation_known,version,shards,draining_shards,move_errors,observed_timestamp_seconds}`、`tenants{state}`、`moves{phase}`；从已提交 catalog 操作及现有迁移巡检更新，不在采集时扫描 catalog |
| router 与发现 | `graphdb_router_{draining,placement_cache_entries,placement_cache_valid_entries}`；placement hit/miss/invalidation、代理响应类别、429 背压和传输失败事件；`graphdb_sharding_client_*` 提供 leader 缓存、最后成功发现时间、GET 重试和发现/请求耗时 |
| 进程与内存 | `graphdb_build_info`、`graphdb_process_start_time_seconds`、`graphdb_go_goroutines`、堆对象/累计分配字节、Go runtime 保留内存、GC 次数及 GC CPU 秒数；采集使用 `runtime/metrics`，不调用全量 `ReadMemStats` |

指标的精确定义随 `/metrics` 的 HELP 输出。未发生过的操作/事件可能尚无样本；累计 counter、直方图的 count/sum 及 leader 变化从进程启动计数，重启后归零。

## 操作耗时

`graphdb_{raft,ha,router,sharding_client}_operation_seconds{operation,status}` 为直方图，提供 `_bucket`、`_sum` 和 `_count`；`*_operations_inflight{operation}` 为当前执行数。status 固定为 `ok/error/timeout/canceled`。操作名固定，不使用租户、URL、请求身份或错误内容作为标签。

有限耗时桶从 1ms 到 1800s，覆盖短请求与长时间快照/维护操作，超过上限进入 `+Inf` 桶。

Raft 主要操作为 `proposal`、`read_barrier`、`quorum_barrier`、`persist`、`configuration_persist`、`apply`、`snapshot_capture/build/persist/restore/send`、`message_send`。`proposal` 从节点提案入口到本地应用回应；业务拒绝的 HTTP 命令仍可能成功提交，应同时看 HTTP 状态。`apply` 包含一个有界日志窗口中的应用批次，不等于一条业务请求。发送耗时包含编码后的传输准备和 HTTP 回应。

router `proxy_read/write` 覆盖 Leader 查找及响应转发；4xx/5xx 记为 error，可用事件类别区分业务拒绝、背压和上游故障。sharding client `read/write_request` 计至 HTTP 响应头，响应体读取另由调用方处理；`leader_lookup` 包含缓存，`leader_discovery` 只记录网络发现。GET 重试沿用已有规则，指标不会引入写请求自动重放。

```promql
# Raft 提案 p99，按进程查看
histogram_quantile(0.99, sum by (job, instance, le) (
  rate(graphdb_raft_operation_seconds_bucket{operation="proposal",status="ok"}[5m])))

# 当前排队请求
graphdb_admission_waiting

# Leader 观察到的复制积压，排除未知进度
graphdb_raft_peer_replication_lag and (graphdb_raft_peer_progress_known == 1)

# 近五分钟发送错误/超时次数
sum by (job, instance) (increase(graphdb_raft_operation_seconds_count{
  operation=~"message_send|snapshot_send",status=~"error|timeout"}[5m]))
```

## 观察边界与部署

Raft state/leader_known/recent_active 均不是实时 quorum 证明；副本 match 是日志复制位置，不是远端图应用位置。逐节点 applied_index/application_lag 才能判断各副本应用积压。Follower 没有 Leader 的 progress 时 `progress_known=0`，其余进度零值表示未知。catalog/接入数量是本地缓存，结合 known 与观测时间判断，不能当作线性一致集群总量。

JSON 诊断保留构建、部署、协调、磁盘、WAL、Raft 和问题列表，并增加准入、查询数量、逐副本状态和已有接入/catalog 观察。HTTP 200 表示诊断接口可读，不能据此放流；router JSON 返回本地 drain/cache 状态，不主动探测集群，也不声称集群健康。数据库诊断的可选 `X-Tenant-ID` 任务查询会读取该租户最多 100 条任务，通用周期采集不带此头。

[采集配置示例](../deploy/prometheus/scrape.yml.example) 列出全部部署形态。只保留实际使用的 job，替换地址；单机示例使用生产模板的管理端口 8081，Raft 为 8082，分片节点需显式配置 `GRAPHDB_ADMIN_ADDR=:8082`。监控连接管理网络，router token 挂载为只读文件，不能把 Raft 8081 传输端口当作管理端口。每组配置固定 `raft_group` 标签，避免不同组的 peer_id 混淆。混部升级期间旧程序没有新增指标，两条磁盘告警切换到新 `graphdb_filesystem_*` 后需保留旧版本的监控直至完成升级。

[18 条告警示例](../deploy/prometheus/graphdb.rules.yml) 覆盖容量、Raft 状态/应用/复制积压、持久化/传输、频繁选主、准入压力、迁移错误、router 上游故障及单机 WAL 积压。阈值与持续时间需依据实际容量和 SLO 调整；`up`/抓取丢失告警由监控平台提供。OS CPU、RSS、文件描述符、磁盘 IOPS/延迟和网络吞吐需配合 node_exporter/容器监控，不能把 Go runtime 保留内存当 RSS。在线接口不执行全图完整性审计，索引健康仍需周期运行既有检查。
