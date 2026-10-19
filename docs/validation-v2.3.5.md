# GGraphDB v2.3.5 发布验证 / Release validation

**PENDING_EXACT_TAG_GATES**。本记录是确切标签验收计划；稳定 Latest 仍为
[v2.3.4](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.4)。
不申请门禁豁免。发布后在 main 补充流水线、标签、下载摘要与运行结果；不移动标签、替换二进制。

## 已完成候选验证

OrbStack Linux/arm64、Go 1.26.7 下，全仓 Go 测试有 1827 个测试／子测试 PASS 事件，
7 项跳过（6 项可选对象存储、1 项 root 权限场景）；vet 通过。storage/HA 聚焦 race 连续
两轮、graph 复制／顺序／不可变性 race 两轮通过。并发重建回归在 C3 稳定复现 manifest
版本冲突，C7 两种预算分支连续三轮通过。完整回归集合包含重建／compact／直接重建各自的
极小预算全量回退及默认预算增量追平分支。

单机待追平时已确认版本 290、catalog 289，最小版本查询可见写入；SIGKILL 后数据恢复，
约 34.8 秒后自动追平（默认 30 秒维护周期），deep audit 通过。这不是两秒恢复目标认证。
前后各三份 Raft 磁盘独立冷读，与对应轮次 API 快照一致；本轮未做 Raft SIGKILL、物理断电或跨宿主机分区。

## 性能证据与限制

[相同输入的 C3/C7 候选对照](performance-tail-validation-2026-10-20.zh-CN.md)和
[机器可读结果](performance-tail-validation-2026-10-20.json)已完成。
单机写入 P99 9.470 → 3.948 秒；原始 30 分钟增长计划下 Raft 查询最大值
13.132 → 5.584 秒。单机 stale 131/327 → 134/327，未改善；样本均 updating，静默后追平。
单机容器内存峰值增加 6.4%；Raft 基线资源计数缺失，不能比较 CPU/RSS。
Raft 接入最大值含重试仍达 27.789 秒。报告保留输入、二进制摘要、数据等价边界及失败尝试。

对照对象为带相同私有 pprof 初始化的工作区候选，不能表述成已发布 v2.3.4/v2.3.5
二进制对照。单机和 Raft 各自前后输入一致，不能跨部署直接比较。

## 待完成确切标签验收

- 全仓 unit/vet/race、版本冻结、Go/Python SDK、控制台边界、备份自动化。
- direct/WAL HTTP、负载、备份恢复、重启、MinIO/S3 和 TLS 网关。
- Raft 协议 1–3、流式快照、准备维护、恢复、磁盘、分片、已发布 v2.3.0 的协议 1 滚动。
- 单机与 Raft 各 30 分钟维护长测；它们是发行门禁，不代替上述固定计划性能对照。
- 下载正式包，核对远端标签、双层 SHA256、源码、构建身份、amd64 门禁摘要绑定；
  独立运行 Linux arm64（OrbStack）和 macOS arm64（本机）的 direct/WAL SDK、HTTP、负载和崩溃重开检查。

持久化格式、Go 模块 `/v2`、HTTP `/v1` 及独立协议编号不变。跨宿主机故障域、日级稳定性、
生产容量和统一低延迟 SLO 未验收。既有导出整图物化、持续快照超预算拒写、已提交应用失败及
CRC 损坏 WAL 的保护策略不在本次改动范围。历史恢复测试两秒偶发超时根因没有新证据，不与
本轮已复现的重建版本冲突混为同一缺陷。先前记录见[2.3.4 验证](validation-v2.3.4.md)。
