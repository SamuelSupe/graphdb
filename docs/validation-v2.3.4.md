# GGraphDB v2.3.4 发布验证 / Release validation

**PUBLISHED_AND_VERIFIED**：[v2.3.4](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.4) 于 `2026-10-19T13:59:42Z`
发布为稳定 Latest，非草稿、非预发行，无门禁豁免。
标签提交为 `a097ebc35b98efe707d94ea63f01dca48033a0c3`；[完整标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39257513975) 全部通过。
本记录在下载核验后更新于 main；不移动标签，不替换二进制。包内保留标签源码的 PENDING 计划记录。
[机器可读证据](validation-v2.3.4.json)保留摘要、作业结论及完整长测统计。

## 确切标签门禁 / Exact-tag gates

| 范围 | 结果 |
| --- | --- |
| Local disk 30-minute mixed read/write soak | PASS |
| Local disk, WAL recovery, load, and restore | PASS |
| gateway / gateway | PASS |
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
| standalone | 30m0s | 121118 / 0 | 19.929 / 26.633 秒 | `index_health samples=59 unhealthy=0 stale=21 last_status=stale` |
| raft | 30m0s | 76551 / 0 | 7.448 / 9.390 秒 | `index_health samples=59 unhealthy=0 stale=0 last_status=ready` |

全部原始错误事件：standalone 0；raft 0。预热、关停和计划重启分类及原始事件保留在 JSON；
门禁有效错误不等于原始错误数。不同部署的批量、数据量、请求完成数不同，不能直接比较，
也不是固定负载前后对照。操作总数混合业务、控制和维护，不能当作 QPS 或生产容量。
完整报告在包内 `release/evidence/`。本版不宣称消除长尾、stale 或极端查询延迟。
单机写入 P99 19.929 秒、最大 26.633 秒，21/59 次 stale，最后采样 stale；
Raft 索引 59 次均 ready，但路径过滤遍历 P99 2.814 秒、最大 12.028 秒。

## 下载资产 / Downloaded assets

压缩包共 57,861,092 bytes，SHA256：`58b0bb5448bf54f37bb161eda2f5ca2417f577baadeca6390d05ea2700270bbd`。
独立下载后核对 GitHub API 大小和摘要、外部校验文件、包内 SHA256，全部一致。

| 二进制 | SHA256 |
| --- | --- |
| `bin/graphdb-darwin-arm64` | `8b50c0868c69bd83281959d64fe3161f5c90e5c11534522371a579a3191cc2d9` |
| `bin/graphdb-linux-amd64` | `ce875d413e263c957525bddf472d1f696094e0aa66c84dcbf7a09f3a064bf37b` |
| `bin/graphdb-linux-arm64` | `642c2fa2a047679d9da764bc535d388d7de98746815cf61754be88ee68bba0e6` |

构建身份为 v2.3.4 / 上述提交 / go1.26.7，日期 `2026-10-19T21:07:28+08:00`。
包内 1043 个标签跟踪文件逐字节一致。Raft 三协议、滚动目标和网关 PASS
metadata 与 amd64 摘要一致；滚动来源摘要与下载的正式 v2.3.0 包一致。

下载的 Linux arm64 在 OrbStack、macOS arm64 在本机分别执行 direct/WAL：真实服务 Python SDK、
HTTP E2E（含备份恢复）、4 writer / 16 reader 负载、SIGKILL 后重新打开的完整快照相同，以及 `/ui/` HTTP 返回均通过。
macOS 的数据目录位于 `/tmp` 下，覆盖父目录为符号链接的兼容性边界；每个平台、每种模式真实服务 Python SDK 均为 13 项、无跳过。
amd64 运行资格来自 GitHub Linux 门禁，未重复做本机模拟运行。
本版没有改变管理控制台呈现或交互，未重新做其 Chrome 全页面行为验收。
官网仅更新两处版本元信息，构建及本机 Chrome 的版本标记、Releases 链接与查询示例切换检查通过，无控制台错误。

## 精简、回归与边界 / Cleanup and boundaries

删除 3 个只检查退休 evidence-search 功能的测试和 3 个标量索引键包装函数；
保留恢复、WAL、快照、类型兼容和分页回归。恢复夹具仅对成功入队的目标任务注入，
普通测试连续 5 轮、race 连续 2 轮通过；修正后全仓普通测试通过。
生产逻辑继承的 OrbStack 全仓普通测试、vet、全量 HA race 对应 `bceffaf5`；
本候选另行重验版本契约、SDK 和端口隔离。确切 v2.3.4 的全仓 unit/vet/race
由上述 GitHub 标签门禁完成；下载资产的 macOS 原生目录和重启边界另行验证。Python 单测的真实服务跳过项由下载二进制的 SDK E2E 补齐。

v2.3.2 的失败标签及流水线保留；新增确定性成员变更回归在旧代码失败，修正后普通 20 轮、race 5 轮通过。
本版修复构建中成员快照遗漏，以及 kind、整数索引、含点类型、时间条件、混合排序、GraphQL 诊断、快照重试、
已移除节点发送、临时任务准备错误、父目录软链、WAL 清理统计和导出完整性。
普通 JSON 导出仍物化整图；持续超预算的快照仍拒写；已提交 5xx 日志、CRC 损坏 WAL
和大小写文件系统边界未改变策略。v2.3.3 的 Compose 启动失败确切触发原因未证实，详见[原失败记录](validation-v2.3.3.md)。
历史两秒恢复测试偶发超时的底层原因未获补证；本次成员快照缺陷有独立复现，不将两者混为同一根因。
历史 v2.3.1 结果见[原记录](validation-v2.3.1.md)。
跨宿主机故障域、日级稳定性、生产容量和统一低延迟 SLO 未验收。

Published only after all exact-tag gates passed. Independent downloaded binaries passed
checksum, identity and runtime checks. Separate single-host soaks do not establish
comparable throughput, production capacity, day-scale reliability or a latency SLO.
