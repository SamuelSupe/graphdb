# GGraphDB v2.3.6 发布验证 / Release validation

**PUBLISHED_AND_VERIFIED**：[v2.3.6](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.6) 于 `2026-10-19T20:19:15Z`
发布为稳定 Latest，非草稿、非预发行，无门禁豁免。
标签提交为 `b51c54daf55efc9f14fc6be52ea4493f3482574b`；[完整标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39310357929) 全部通过。
本记录在下载核验后更新于 main；不移动标签，不替换二进制。包内保留标签源码的 PENDING 计划记录。
[机器可读证据](validation-v2.3.6.json)保留摘要、作业结论及长测统计。

## 确切标签门禁 / Exact-tag gates

| 范围 | 结果 |
| --- | --- |
| Local disk 30-minute mixed read/write soak | PASS |
| gateway / gateway | PASS |
| Local disk, WAL recovery, load, and restore | PASS |
| Unit, vet, race, and 2.0 contracts | PASS |
| raft / raft | PASS |
| object-backup / Object snapshots and fresh-directory restore | PASS |
| Build and publish release assets | PASS |

包括全仓 Go unit/vet/race、版本冻结、Go/Python SDK、控制台边界、备份自动化，
单机 direct/WAL、负载、备份恢复和重启，MinIO/S3 与 TLS 网关，Raft 协议 1–3、
流式快照、准备维护、恢复、磁盘、分片、已发布 v2.3.0 的协议 1 滚动，以及两项 30 分钟长测。
单机长测使用标签源码的常规构建；Raft 和网关的 amd64 摘要严格绑定发行包二进制。

## 维护长测 / Maintenance soaks

| 部署 | 时长 | 操作数 / 门禁有效错误 | 写入 P99 / 最大 | 索引样本 |
| --- | --- | --- | --- | --- |
| standalone | 30m0s | 114073 / 0 | 5.627 / 7.652 秒 | `index_health samples=59 unhealthy=0 stale=53 last_status=stale` |
| raft | 30m0s | 70840 / 0 | 7.154 / 10.968 秒 | `index_health samples=59 unhealthy=0 stale=0 last_status=ready` |

- standalone 最大查询 `scan-entities-min-version`：P99 0.327 秒，最大 3.686 秒。
- raft 最大查询 `scan-entities-min-version`：P99 2.066 秒，最大 16.042 秒。

全部原始错误事件：standalone 0；raft 0。预热、关停和计划重启分类及原始事件保留在 JSON；
门禁有效错误不等于原始错误数。不同部署的批量、数据量、请求完成数不同，不能直接比较，
也不是固定负载前后对照。操作总数混合业务、控制和维护，不能当作 QPS 或生产容量。
完整报告在包内 `release/evidence/`；上表是发行门禁，本版不宣称消除长尾或瞬时 stale。

## 下载资产 / Downloaded assets

压缩包共 57,860,449 bytes，SHA256：`b0d59f2e59ff26d1cfe0ac930877d861a164340a6ffe44cbe107f6ac2f1171e3`。
独立下载后核对 GitHub API 大小和摘要、外部校验文件、包内 SHA256，全部一致。

| 二进制 | SHA256 |
| --- | --- |
| `bin/graphdb-darwin-arm64` | `d954a119b4dbc7e1c757da76014bf803a3d4f23cb447ff0754f45c0486e8112a` |
| `bin/graphdb-linux-amd64` | `c0cc71ba91c3be88e5c88ffc6be7e5e66c5b84d306ed5ed43bba8cd7bffdfe0f` |
| `bin/graphdb-linux-arm64` | `7938b0a56ad09270218a71c3f93becf762f29aa12bbd51853b4edf7aacca6a9b` |

构建身份为 v2.3.6 / 上述提交 / go1.26.7，日期 `2026-10-20T03:21:48+08:00`。
包内 1052 个标签跟踪文件逐字节一致。Raft 三协议、滚动目标和网关 PASS
metadata 与 amd64 摘要一致；滚动来源摘要与下载的正式 v2.3.0 包一致。
正式构建不包含候选压测专用的 pprof 初始化文件。

下载的 Linux arm64 在 OrbStack、macOS arm64 在本机分别执行 direct/WAL：真实服务 Python SDK、
HTTP E2E（含备份恢复）、4 writer / 16 reader 负载、SIGKILL 后重新打开的完整快照相同，
以及 `/ui/` HTTP 返回均通过。每个平台每种模式 SDK 均为 13 项、无跳过。
macOS 的数据目录位于 `/tmp` 下，覆盖父目录为符号链接的兼容性边界。
amd64 运行资格来自 GitHub Linux 门禁，未重复做本机模拟运行。
管理控制台呈现和交互未修改，未重新做控制台 Chrome 全页面验收。
官网仅更新两处版本元信息；构建和本机 Chrome 的 2.3.6 可见标记、Releases 链接核验通过，控制台无 warning/error。

## 候选性能、修复及边界 / Candidate performance and boundaries

[相同输入的 C3/C7 对照](performance-tail-validation-2026-10-20.zh-CN.md)已完成：
单机写入 P99 9.470 → 3.948 秒；原始 30 分钟增长计划下 Raft 查询最大值
13.132 → 5.584 秒。单机 stale 131/327 → 134/327，未改善；全部在 updating，静默后追平。
单机容器内存峰值增加 6.4%；Raft 基线资源计数缺失，不能比较 CPU/RSS。
Raft 接入最大值含 429 重试仍为 27.789 秒。
这些是带相同私有 pprof 初始化的工作区候选，不是已发布 v2.3.4/v2.3.6 二进制对照。
两种部署各自前后输入一致，不能跨部署直接比较；原始输入和语义等价边界保留于报告。

候选全仓 Go 测试 1827 个 PASS 事件、7 项跳过，vet、聚焦 storage/HA/graph race 两轮通过；
确切标签全仓普通和 race 测试另由上述发布门禁完成。直接重建回归在旧 C3 上复现
manifest 版本冲突，C7 极小／默认预算分支三轮通过。本版统一捕获与索引发布顺序，
本地维护期间有界合并追平需求；超限、schema 变化或基准不匹配仍回退全量构建。

候选单机版本 290 已确认、catalog 289 时最小版本查询可见；SIGKILL 后图数据恢复，
约 34.8 秒自动追平（默认 30 秒维护周期），deep audit 通过。这不是两秒 RTO 认证。
前后各三份 Raft 磁盘冷读与对应 API 快照一致，本轮候选未做 Raft SIGKILL。
正式门禁的 Raft 恢复、协议与滚动场景另见包内证据。

既有普通 JSON 导出整图物化、持续快照超预算拒写、已提交应用失败及 CRC 损坏 WAL
保护策略未变；历史两秒恢复测试偶发超时根因没有新证据，不与本轮已复现的重建冲突混淆。
跨宿主机故障域、物理断电、日级稳定性、生产容量和统一低延迟 SLO 未验收。
v2.3.5 因增强门禁 Compose 启动失败未发布，原 stderr 缺失，确切触发原因未证实。
独立复现出站客户端占用 36082 后 Docker 无法绑定，16082 成功；本版统一默认门禁偏移 -20000，
保留显式偏移并输出启动/恢复诊断。`internal/`、`cmd/`、`tools/` 与 v2.3.5 的优化实现完全相同。
失败标签不移动，见[原记录](validation-v2.3.5.md)。历史发布资格见[2.3.4 验证](validation-v2.3.4.md)。

Published after all exact-tag gates passed. Independent downloaded binaries passed
checksums, build identity and runtime checks. Candidate matched comparisons and
release qualification are separate evidence; seconds-long tails and transient
staleness remain. No production latency or capacity SLO is certified.
