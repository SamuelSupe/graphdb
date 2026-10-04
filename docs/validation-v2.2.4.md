# GGraphDB 2.2.4 发布验证 / Release validation

## 中文

[v2.2.4](https://github.com/SamuelSupe/graphdb/releases/tag/v2.2.4) 已于 2026-10-04T15:13:52Z 发布为 GitHub 的稳定 Latest。标签和发行程序固定在提交 `043d082cf8a73032c702d7c6c3d7676ee3f6d7dc`，Go 1.26.7；后续说明更新不重写标签或资产。同一程序继续支持单机 direct/WAL、单组 Raft 与租户分片。

### 发行门禁与下载验证

| 范围 | 结果与实际证据 |
| --- | --- |
| 全仓 unit、vet、race、Go/Python SDK、备份 worker、版本契约 | [精确标签工作流](https://github.com/SamuelSupe/graphdb/actions/runs/37208814068) PASS |
| 单机 direct/WAL 集成、负载、恢复与重启 | Linux amd64 正式门禁 PASS |
| 单机三十分钟维护长测 | 125,770 次记录操作，零操作错误；compact 5、GC 2、index rebuild 2；读取就绪采样 59 次、unready 0 |
| 单机索引与写入长尾 | unhealthy 0、stale 28、最后采样 stale；强读查询仍通过。ingest P99 29.556 秒；不将 stale 算作 ready，也不声明立即索引就绪或低延迟 SLO |
| 真实 MinIO/S3 | 备份、拒绝损坏、重开与全新目录恢复 PASS |
| TLS 网关 | 真实角色、编码路径、租户头替换与配置绑定 PASS |
| Raft 协议 1/2/3、分片、故障与恢复 | 流式快照、准备维护、运行态恢复、磁盘压力、协议 3 GC 与串行重启 PASS |
| 三副本三十分钟维护长测 | 65,570 次记录操作，零操作错误；compact 5、GC 2、index rebuild 2；读取 unready 0、索引 unhealthy 0/stale 0，最后 ready。ingest P99 10.680 秒 |
| 限定基线的双版本滚动 | 开发提交 `5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a` 到本版、协议 1 PASS；来源不是已发布 v2.2.2 |
| 解压发行包 | 工作流验证 Compose、备份 worker、容器构建与 readiness PASS |
| 实际 GitHub 下载包 | GitHub 资产摘要、外层校验文件、三程序 SHA256、构建身份和 1,010 个标签源文件逐一比较 PASS |
| 下载的 macOS arm64 / Linux arm64 程序 | 两平台分别完成 direct/WAL HTTP E2E、同目录优雅停止与重启、图导出一致性；有效 WAL 批次的 committed 终态在重启后保持一致 |

操作数是各类操作指标的累计值合计，包含查询、控制、维护与写入，不作为业务 QPS 或容量承诺。Linux amd64 包内程序与协议 1、增强门禁、协议 3、滚动目标、网关门禁的 SHA256 全部相同。arm64 的上述下载验收只认证单机路径；amd64 的 Raft 结果不转移为 arm64/macOS 的 Raft 全量认证。结构化证据见 [JSON](validation-v2.2.4.json)，包内门禁文件见 `release/evidence/`。

### 下载与身份

- 归档：`graphdb-v2.2.4.tar.gz`
- 外层 SHA256：`4909186b7fecf740e4289481e0034aa6cf82a3e4742f9cc7dee57ee425fc1591`
- Linux amd64：`224df57d837aa6cd475881681c286bfae1c2670da245c24a5399820ec22fb86a`
- Linux arm64：`e093656c102ad36e51e4032f4bec853b3fa5cbeffaa3cc4cb6bca58f7b48a2c0`
- macOS arm64：`70ffdf4f91e932dffc6082500f1122adb1a5f6ee2d0646228fe4351888fa7ee3`
- 构建日期：`2026-10-04T22:11:48+08:00`

发行包中的本页为门禁启动前的冻结快照；确切 PASS 证据位于包内 `release/evidence/`，本 GitHub main 页面补记发行和下载后验收。

### 修复与保留的失败

2.2.4 保留写租约更新和准备维护发布前的不可变读图，标记过期后仍核对当前 manifest 身份或相同逻辑摘要；写入、控制、索引和来源配置缓存仍失效。接管、恢复、清空与重建边界继续完整失效。新增回归在 v2.2.3 旧代码稳定复现租约更新、compact 和索引重建的三次不必要整图重载；修复后 Linux arm64 的三轮针对性 race 与完整存储/HTTP/HA race 通过。未延长五秒查询预算，未改变多数派、强读、回滚日志或 fsync 顺序。

[v2.2.3 的正式标签门禁](validation-v2.2.3.md)因一次保存查询 HTTP 504 失败，未发布；原标签和失败记录保留。缓存缺陷已被独立复现和修复，但没有据此断言历史 504 只有该根因。本版重新完成全部精确标签门禁，不申请豁免。

[主分支 CI 首轮](https://github.com/SamuelSupe/graphdb/actions/runs/37208813941/attempts/1)中，一个使用内存存储的恢复演练测试未在原两秒轮询窗口内结束。该路径不进入本次本地缓存修改；Linux arm64/GOMAXPROCS=2 的针对性 race 连续 30 轮通过，根因未获完整复现。同提交[分支 CI](https://github.com/SamuelSupe/graphdb/actions/runs/37208366118)、正式标签全仓检查和 OrbStack 完整 race 通过；主分支失败作业按原代码、原断言和原超时重跑后通过。首轮仍为 FAIL，不改写为 PASS，也不据重跑排除所有时序问题。

下载验收的补充 WAL 探针最初缺少已安装 host schema 的必需 `hostname` 字段，服务按契约返回 207。修正探针输入后用新目录完成两个平台验证；原日志保留，产品代码和超时未改。

历史[最终性能候选](performance-raft-batching-2026-10-04.zh-CN.md)的四次入口 503、45,116 次操作与 FAIL 状态继续保留。捕获微基准约 63–66% 分配字节减少不代表整体吞吐收益；稳定 A/A 和生产容量仍未认证。

### 升级与尚未验收

单机先备份并停止旧进程，再用同一 prefix 和目录启动新版，不允许两个进程共享目录。Raft 滚动资格仅限上述固定开发基线到本版的协议 1 窗口，不代表已发布 v2.2.2 的无条件滚动兼容。受来源身份修复影响的数据组不能与旧解释程序混部；升级前完成或取消旧迁移、部分 S3 恢复，窗口内暂停新迁移/恢复。未验证组合采用维护升级或经过校验的备份恢复到新组；协议 2/3 在全组支持后独立激活。见[滚动说明](raft-rolling-upgrade.zh-CN.md)。

按用户决定，跨宿主机保留 **NOT RUN**。真实容量、慢盘/网络长尾、24/72 小时稳定性、生产身份、告警通知和分片 router 的备份调度联测仍待验收。默认快照预算 512MiB；逻辑备份不含全部运行态或待发布 WAL，不提供 PITR、自动租户均衡或租户内部图分区。历史容量包络不认证本版或 Raft 容量。

官网版本文本已做构建检查；部署/HTTP 结果另补记。Mac 锁定导致本次 Chrome 复验 **NOT RUN**，不把静态构建或 HTTP 文本检查表述为浏览器呈现/交互验证。

## English

[v2.2.4](https://github.com/SamuelSupe/graphdb/releases/tag/v2.2.4) is published as the stable Latest release, pinned to `043d082cf8a73032c702d7c6c3d7676ee3f6d7dc` and Go 1.26.7. All exact-tag gates passed: unit/vet/race and SDK/worker contracts; standalone direct/WAL and a 30-minute workload; real S3 recovery and TLS authorization; Raft protocols 1/2/3, sharding, failure/recovery, disk pressure, bounded-source rolling and a 30-minute maintenance workload; extracted package checks.

The local workload recorded 125,770 metric operations with no operation errors, but its final index sample was stale and ingest P99 was 29.556 seconds. The Raft workload recorded 65,570 metric operations with no operation errors and ingest P99 of 10.680 seconds. These totals include control/query/maintenance work and are not a capacity or throughput qualification.

Downloaded assets matched GitHub digests, outer/inner checksums, build identity and 1,010 tag source files. The packaged amd64 binary matches Raft, rolling and gateway evidence. Downloaded macOS/Linux arm64 binaries independently passed standalone direct/WAL HTTP E2E, graph consistency across graceful restart and persisted committed WAL terminal state. Their Raft paths were not independently qualified.

The failed v2.2.3 tag, prior performance failures and first main CI test polling timeout remain recorded. The main failure was not reproduced in 30 focused Linux arm64 race runs; rerunning unchanged assertions/timeouts passed, without proving its root cause. The supplemental native WAL probe initially omitted a schema-required hostname; the fixture was corrected in fresh directories without product changes.

Rolling only qualifies development commit `5fd0c9704ca573b902c66eaa1cbbffd2dc8c9b4a` to this target in protocol 1, not all published v2.2.2 deployments. Cross-host, day-scale stability, production capacity and integrations remain unqualified. Native Chrome revalidation was blocked by the locked Mac. Package documentation retains its pre-gate snapshot; packaged evidence and this main record provide the actual results.
