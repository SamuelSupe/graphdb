# GGraphDB v2.3.8 发布验证 / Release validation

**PUBLISHED_AND_VERIFIED**：[v2.3.8](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.8) 于 `2026-10-20T10:43:08Z` 发布为稳定 Latest，非草稿、非预发行，无门禁豁免。
标签提交为 `46fd6e8303385d49dbbfddc0563162779154fd63`；[完整标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39403810311) 全部通过。
本记录在独立下载核验后更新于 main；标签和二进制不移动、不替换。包内保留标签时的候选计划，实际门禁证据位于 `release/evidence/`。
[机器可读证据](validation-v2.3.8.json)记录构建身份、门禁、长测、故障注入和运行结果。

## 三处修复与回归

| 问题 | 修复行为 | 回归边界 |
| --- | --- | --- |
| 索引无限重试、等待忽略取消 | 明确瞬时错误最多三次；永久错误退出；重试前释放旧视图、构建预算和队列位置；新尝试重新捕获图 | 普通/延迟任务持续失败、耗尽、维修独占视图、重试时新提交与真实 purge |
| joined / waitFor 取消 | 等待响应 ctx，同时保留前驱屏障，后继不能越序发布 | 已取消调用及时返回，后继仍等待旧前驱，随后队列可排空 |
| WAL HTTP 被发布背压拒绝 | 解码前只获取写入槽；flush 负责发布背压；同批次重发可返回原确认 | tail/CAS/对象错误压力下新请求与重发 202，直接写入仍 429，崩溃恢复无重复 |
| 剩余 tail 容量未利用 | 未准备组继续拆分，按顺序填充剩余容量；满后等待 compact；PREPARED 不拆 | seed tail=1、上限=4、四请求提交前三个；compact/重启后余项提交，重放版本不变 |

实现集中在后台索引、HTTP WAL 准入和分组拆分三个位置。回归位于
[index_build_memory_test.go](../internal/storage/index_build_memory_test.go)、
[ingest_service_local_test.go](../internal/storage/ingest_service_local_test.go) 和
[ingest_wal_test.go](../internal/httpapi/ingest_wal_test.go)。新增断言放回旧源码后，取消、持续索引故障、剩余容量与 HTTP 背压场景出现预期失败。

OrbStack Ubuntu arm64 / Go 1.25.0 全量 unit、vet、race 通过；受影响回归五轮 race、后续新增 purge/新 head 边界三轮 race 通过。
本地 direct/WAL HTTP、并发负载、重启、导出一致性和恢复门禁通过。Node 控制台测试 12 项、备份自动化 11 项通过；Python 静态测试 12 项，未配置服务地址时跳过一项，真实服务 SDK 由集成门禁另行执行。
本机 Chrome 核对官网 2.3.8 标记；本补丁没有变更管理 UI 交互。

## 确切标签门禁

| 范围 | 结果 |
| --- | --- |
| Local disk 30-minute mixed read/write soak | PASS |
| Unit, vet, race, and 2.0 contracts | PASS |
| gateway / gateway | PASS |
| raft / raft | PASS |
| Local disk, WAL recovery, load, and restore | PASS |
| object-backup / Object snapshots and fresh-directory restore | PASS |
| Build and publish release assets | PASS |

包括全仓 unit/vet/race/版本契约/SDK、真实 direct/WAL、重启恢复、MinIO/S3 和 TLS 网关，
以及 Raft 基础故障、流式快照、prepared maintenance、运行恢复、磁盘压力、协议 3 GC、分片和从 v2.3.0 滚动升级。
单机长测为标签源码常规构建；Raft 与网关的 amd64 摘要严格绑定发行包二进制。

## 维护长测

| 部署 | 时长 | 操作数 / 门禁有效错误 | 写入 P99 / 最大 | 索引采样 |
| --- | --- | --- | --- | --- |
| standalone | 30m0s | 109289 / 0 | 4.436 / 6.018 秒 | `index_health samples=59 unhealthy=0 stale=52 last_status=stale` |
| raft | 30m2s | 77589 / 0 | 6.951 / 8.447 秒 | `index_health samples=59 unhealthy=0 stale=0 last_status=ready` |

- standalone 查询最大值：`stream-large-indexed-hosts`，5.282 秒；原始错误事件：`{}`。
- raft 查询最大值：`cmdb-owner-service-match`，13.102 秒；原始错误事件：`{}`。

原始事件与门禁错误分类分别记录。操作数包含业务、控制和维护，不能当作生产 QPS。
这不是同机固定负载的前后性能对照；长测通过不表示索引滞后或秒级长尾已消除。

## 独立下载资产

压缩包 58,083,617 bytes，SHA256：`206c7676a9aeac1426e27e5da186abe9cd90345133463ca9585d2d5a1b1a37b8`。
GitHub API digest、资产大小、外部校验文件及包内三个二进制的 SHA256 均匹配。

| 二进制 | SHA256 |
| --- | --- |
| `bin/graphdb-darwin-arm64` | `fcf4df94b0a1aed11e20e8523197c6535d3de88bd8e6e42054ebaccd599345ba` |
| `bin/graphdb-linux-amd64` | `156451d7d5002914d014208e262c1f55d691af15edce0f7a0a6098e5fe7cfdd5` |
| `bin/graphdb-linux-arm64` | `6fbfb0bd5d09fabeeaae652f80efffcf26a40e446de1939483632f1a1ada01eb` |

构建为 v2.3.8 / 上述提交 / go1.26.7，日期 `2026-10-20T17:51:48+08:00`。
包内 1058 个标签跟踪文件逐字节一致；Raft 基础、增强、协议 3、滚动目标与网关的 PASS metadata 均绑定包内 amd64 摘要，网关配置和 fixture 也匹配。

下载的 Linux arm64（OrbStack）和 macOS arm64（本机）各自完成以下运行检查，均通过：

1. 版本/readiness 构建身份；direct 与 WAL 写入；重启读取；同 batch 幂等重放；离线 runtime-backup/restore 到新目录后再次读取与重放。
2. 先用已发布 v2.3.7 在 tail=1、上限=4 下接收四请求，确认旧版停在 version=1/retrying；崩溃后切换下载的 v2.3.8，直接推进至 version=4。再次在发布背压下重启，旧批次确认和新 WAL 请求返回 202，direct commit 返回 429；compact 后推进至 version=6，重复批次没有改变图。
3. 在隔离临时目录中破坏 `indexes/catalog.parquet` 后重启。WAL 仍提交，后续 direct commit 返回 200 且带索引警告，导出保留三条实体；手动 rebuild 明确返回 409，purge 返回 200。这证明失败可返回、维修可进入，**不把损坏 catalog 的 rebuild 冲突计为修复成功**。同一探针对已发布 v2.3.7 复现后续 direct commit 超时。

Linux amd64 的真实运行资格来自 GitHub 原生 Linux 门禁，未做本机模拟运行。

## 边界

发布图数据后取消索引等待不会回滚该提交。八个后台 worker 的限额包含退避任务，防止重试扩成无界队列；退避期间不保留旧视图或构建内存。
永久磁盘或 catalog 故障仍需维修；落后的索引可由自动维护或显式 rebuild 追平。满 tail 仍须 compact，已准备组保持原子发布。
HTTP `/v1`、Go 模块 `/v2`、WAL/Parquet 格式及 Raft 协议编号不变。
本验证没有覆盖跨主机故障域、物理断电、日级稳定性或生产容量资格。
