# GGraphDB 2.3.1 发布验证 / Release validation

## 当前状态 / Current status

**PUBLISHED_AND_VERIFIED**：[v2.3.1](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.1) 于
`2026-10-12T20:37:43Z` 发布为稳定 Latest，非草稿、非预发行，无门禁豁免。
标签指向 `8512db095030dcff86a154df3082e582f99a3937`；[标签工作流](https://github.com/SamuelSupe/graphdb/actions/runs/38299542431) 全部成功。
本记录在下载核验后更新于 main，不移动标签；包内保留发行前的 PREPARING 状态记录。

Published as stable Latest after all exact-tag gates passed. This post-publication
main record does not replace the immutable tag, binaries or candidate record in
the archive. [Machine-readable evidence](validation-v2.3.1.json) retains checksums,
job conclusions and complete maintenance-soak operation statistics.

## 确切标签门禁 / Exact-tag gates

| 范围 / Scope | 结果 / Result |
| --- | --- |
| Local disk, WAL recovery, load, and restore | PASS |
| raft / raft | PASS |
| Unit, vet, race, and 2.0 contracts | PASS |
| object-backup / Object snapshots and fresh-directory restore | PASS |
| gateway / gateway | PASS |
| Local disk 30-minute mixed read/write soak | PASS |
| Build and publish release assets | PASS |

包含全仓 unit/vet/race、版本冻结、SDK、控制台及备份 worker；direct/WAL HTTP、恢复、
负载与重启；真实 MinIO/S3 和 TLS 网关；Raft 协议 1–3、恢复、磁盘、分片、流式和
准备维护；已发布 v2.3.0 到本版的协议 1 滚动；两项三十分钟维护长测；三平台打包、
配置和容器启动。滚动来源摘要与独立下载的正式 v2.3.0 amd64 二进制一致。

## 维护长测 / Maintenance soaks

| 部署 / Topology | 时长 / Duration | 操作数 / 门禁有效错误 | 写入 P99 / 最大值 | 索引样本 / Index samples |
| --- | --- | --- | --- | --- |
| standalone | 30m0s | 120550 / 0 | 22.028 / 36.052 秒 | `index_health samples=59 unhealthy=0 stale=24 last_status=stale` |
| raft | 30m5s | 70242 / 0 | 8.314 / 9.872 秒 | `index_health samples=59 unhealthy=0 stale=0 last_status=ready` |

门禁报告设置一分钟预热并单独分类预热／关停错误，原始错误事件及分类保留在 JSON。
全部原始错误事件数：standalone 0；raft 0。
操作总数混合查询、控制、写入与维护，不能解释为 QPS 或容量。不同部署的批量、数据量
和请求完成数不同；本表不是固定负载前后对照，也不证明瞬时 stale 或极端延迟完全消失。
完整原始门禁报告位于包内 `release/evidence/`，各操作延迟见 JSON。

These separate soaks do not establish comparable throughput, production capacity
or a latency SLO. Their workload/data sizes differ; fixed-data comparisons below
have separate diagnostic binary identities.

单机长测使用标签源码的常规 Go 构建；Raft 与网关门禁的 amd64 二进制摘要与发行包严格一致。
下载资产的 direct/WAL 运行检查另行使用包内二进制执行。

## 下载资产核验 / Downloaded artifact verification

`graphdb-v2.3.1.tar.gz` 共 57,632,963 bytes，SHA256：
`11cdb8d4f72cfd4ddd288279ef13c2320c77a6d3c989a473ce43b6b6142f29fe`。
重新下载并核对 GitHub API 大小/摘要、外部校验文件和包内 SHA256，全部一致。

| 二进制 / Binary | SHA256 |
| --- | --- |
| `bin/graphdb-darwin-arm64` | `6cba4d73068fbabbc663b8bc896091a4604a569bc8cfcccbc9d3736209761381` |
| `bin/graphdb-linux-amd64` | `d1c4c09654fe52d359075ff109d7c59eea746845c84b673ab27f7805b64e837c` |
| `bin/graphdb-linux-arm64` | `1167504c41e1a90b6c1103bf54fb6eabf52246fdc89715c3180aa1e689c584c4` |

构建身份为 v2.3.1 / 上述提交 / go1.26.7，构建日期
`2026-10-13T03:40:36+08:00`。包内 1035 个标签跟踪文件逐字节一致。
amd64 摘要与 Raft 三协议、滚动目标和网关 PASS metadata 绑定；滚动来源为正式 v2.3.0。

下载的 Linux arm64 在 OrbStack 执行 direct/WAL：真实服务 SDK、HTTP E2E（含备份恢复）、
4 writer / 16 reader 负载、重启前后完整快照相同及 `/ui/` HTTP 返回均通过。
下载的 macOS arm64 在本机也执行 direct/WAL 的 SDK、HTTP E2E、负载、备份恢复及重启检查；
两平台每种模式 SDK 各 12 项均无跳过。原生测试客户端使用 Go 1.26.1，服务二进制仍为 Go 1.26.7。
amd64 运行资格来自 GitHub Linux 门禁；没有重复做本机 amd64 模拟运行。
本版未改变管理页面呈现或交互，未重新做 Chrome 全页面行为验收。

## 修复对照与边界 / Repair comparison and boundaries

[固定负载报告](maintenance-tailfix-2026-10-13.zh-CN.md)记录两种部署分别从相同停止状态
数据、相同资源和请求计划开始的一次六分钟前后对照。单机写入 P99 12.123 → 9.658 秒，
Raft 1.885 → 0.908 秒；单机 stale 7/36 → 3/36，冷查询超时 318/480 → 22/480。
完整快照重启、三个独立副本核对和深度审计、新布局的正式 v2.3.0 读取均通过。
这些诊断构建的数值不替代上述标签门禁，也不能跨部署直接比较。
正式单机长测仍有 22.028 秒写入 P99、36.052 秒最大值和 24/59 次 stale、最后采样 stale；
固定负载收益不足以证明长尾与 stale 已消除。
正式 Raft 长测主要 MATCH／遍历查询 P99 为 3.509–4.003 秒，
`profile-indexed-match` 最大 9.642 秒；极端延迟仍存在。

OrbStack Go 1.26.7 下对发行提交运行全仓 unit/vet/race 和版本冻结全部通过；
storage 普通/race 为 160.871/313.055 秒，HA 为 240.273/489.908 秒。
SDK 单测中的真实服务 E2E 跳过由下载二进制运行验证补充。

单机瞬时 stale、冷查询五秒超时仍存在。旧约六秒 Raft 极端查询未在固定基线中复现，
正式长测仍出现超过九秒的查询；极端延迟未消除。restore-drill 测试采用有上限的终态等待；
历史两秒偶发超时的底层原因仍未证明。跨宿主机故障域、日级稳定性、生产容量和统一低延迟 SLO 未认证。

Transient staleness and cold-query timeouts remain observable. Historical failures
are retained, not reclassified. Single-host gates do not certify production fault
domains, day-scale reliability, capacity or a uniform low-latency SLO.
