# GGraphDB v2.3.7 发布验证 / Release validation

**PUBLISHED_AND_VERIFIED**：[v2.3.7](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.7) 于 `2026-10-20T09:01:51Z` 发布为稳定 Latest，非草稿、非预发行，无门禁豁免。
标签提交为 `b9887bf96398e902bcc72dbf63201a6b75c4cf53`；[完整标签流水线](https://github.com/SamuelSupe/graphdb/actions/runs/39391492456) 全部通过。
本记录在独立下载核验后更新于 main；标签和二进制不移动、不替换，包内保留标签时的 PENDING 计划。
[机器可读证据](validation-v2.3.7.json)保留摘要、构建身份、门禁、长测和运行结果。

## 确切标签门禁

| 范围 | 结果 |
| --- | --- |
| Unit, vet, race, and 2.0 contracts | PASS |
| Local disk, WAL recovery, load, and restore | PASS |
| Local disk 30-minute mixed read/write soak | PASS |
| gateway / gateway | PASS |
| raft / raft | PASS |
| object-backup / Object snapshots and fresh-directory restore | PASS |
| Build and publish release assets | PASS |

包括全仓 Go unit/vet/race、版本冻结、Go/Python SDK、控制台边界和备份自动化，
真实 direct/WAL、重启、备份恢复、MinIO/S3、TLS 网关，以及 Raft 协议 1–3、流式快照、
维护、磁盘压力、运行恢复、分片、从 v2.3.0 发布提交构建的滚动升级和协议 3 滚动重启。
单机长测使用标签源码的常规构建；Raft 与网关的 amd64 摘要严格绑定发行包二进制。
SDK 静态测试因未指定服务地址跳过 live 测试类；真实服务 SDK 由磁盘集成门禁另行执行。

## 维护长测

| 部署 | 时长 | 操作数 / 门禁有效错误 | 写入 P99 / 最大 | 索引样本 |
| --- | --- | --- | --- | --- |
| standalone | 30m0s | 114336 / 0 | 2.851 / 5.011 秒 | `index_health samples=59 unhealthy=0 stale=55 last_status=stale` |
| raft | 30m1s | 84346 / 0 | 6.228 / 8.953 秒 | `index_health samples=59 unhealthy=0 stale=0 last_status=ready` |

- standalone 查询最大值：`scan-entities-allow-stale`，1.107 秒；原始错误事件：`{}`。
- raft 查询最大值：`indexed-match-allow-stale`，10.869 秒；原始错误事件：`{}`。

原始事件及预热、关停、计划重启分类保留在包内证据；门禁有效错误和原始错误数分别记录。
长测通过不表示持续 stale 或秒级长尾已消除。这不是固定负载的前后性能对照，两种部署也不能直接比较；
操作总数包含业务、控制和维护，不能作为 QPS 或生产容量承诺。

## 独立下载核验

压缩包共 58,424,378 bytes，SHA256：`6f159f0c8cbcfb9cc95cf4062c45adf3a9c3f0da55e08ba4f72f81f2332ab163`。
GitHub 资产大小、外部校验文件、包内 SHA256 与实际文件一致。

| 二进制 | SHA256 |
| --- | --- |
| `bin/graphdb-darwin-arm64` | `b7980ec3bb4f5d6aa74c47778e5df33988aefa1fe7cfb66b68b880e0516183c2` |
| `bin/graphdb-linux-amd64` | `da7365fbe32bc17fac20e82584653ff6de51cf8e78964a3de758a56a400f4243` |
| `bin/graphdb-linux-arm64` | `9ed706d423e0fe6cc57ce34d7b760f43e63c61ac374d5d48d39da78db424cc94` |

构建身份为 v2.3.7 / 上述提交 / go1.26.7，日期 `2026-10-20T16:09:19+08:00`。
包内 1055 个标签跟踪文件逐字节一致；Raft 基础、增强、协议 3、滚动目标和网关 PASS
metadata 与 amd64 摘要一致，网关配置及测试 fixture 也已比对。

下载的 Linux arm64 在 OrbStack、macOS arm64 在本机分别验证：版本和 readiness 构建身份、
direct 写入、WAL wait=committed、重启后两类数据保留、同 batch 幂等重放，离线备份恢复到新目录后
再次读取及重放，全部通过。Linux amd64 的运行资格来自 GitHub 原生 Linux 门禁，未做本机模拟运行。

另外使用 4d4d71b 源码构建创建旧索引目录，只在旧版重建索引，然后切换到新版源码构建和下载的 Linux arm64：
负零旧键使用等值索引返回正确；非标量范围及大小写范围由旧版漏行变为新版正确返回，冷 catalog 采用实体扫描。
空字符串范围 fixture 是通过性对照，不能据此宣称该 fixture 在旧版失败。下载版验证未重建索引。

本机 Chrome 已验证修改涉及的平台克隆、备份创建及跨来源恢复 dry run，官网 2.3.7 标记也已检查，未见控制台错误。
不是管理控制台所有页面的全面验收。

## 修复范围与边界

本版处理 WAL 分组及 PREPARED 超限、生成号超时、租户规模内存预约与索引追平重试、catalog 单调发布、
拆分等值／范围／负零／非标量查询、旧实体分页、时间过滤、临时快照压力、Drain 移交、
租户作用域、隔离恢复确认、解码前准入、监控登记、存储错误及 Compose 默认暴露。

完整损坏 WAL 帧不自动截断；平台跨租户迁移保留。恢复确认参数不提供网络隔离。
已终态失败的旧 batch 不改写结果，需对账后使用新 batch ID。冷 catalog 查询可能回退实体扫描。
Raft 升级前结束或取消生命周期/恢复任务，并在混合版本窗口暂停这些操作。
没有跨宿主机故障域、物理断电、日级稳定性、生产容量或统一低延迟 SLO 验收。

## 原审查清单的处理

| 原条目 | 处理与证据边界 |
| --- | --- |
| P1-1 WAL 大组不推进 | 按请求数/字节分组，遇到组自身超过 commit tail 上限时拆分；真实 tail 满仍需 compaction。回归覆盖超上限积压后开始推进。 |
| P1-2 PREPARED 合并超限 | 未准备的大组拆分，单个可写请求不因合并超段大小成为终态失败；覆盖三请求小段、全部完成和重复 batch 读取。 |
| P1-3 维护预算阻塞 | 以租户图及增量估算预约；显式等维护结束，避免通过全预算预约隐含串行化。覆盖预算被其他工作部分占用时仍追平。 |
| P1-4 生成号超时 | 移除上下文覆盖；覆盖超时后同一身份可再次尝试。 |
| P1-5 快照压力停止 Raft | 明确分类的磁盘/对象存储临时压力进入重试状态；真实损坏仍失败关闭。覆盖三副本保留读屏障、拒绝新提案并在恢复后推进。 |
| P1-6 旧身份恢复在线集群 | 增加显式隔离确认及操作文档。没有宣称网络隔离或证明双 Leader；旧身份仍禁止接回在线集群。 |
| P1-7 分片等值漏行 | 写入选择最长匹配，读取兼容扫描重叠旧分片并去重；覆盖 server1/server10 分拆。 |
| P1-8 范围/前缀漏行 | 不对大小写敏感范围使用小写分片名推断；保留空串/非字符串候选，必要时扫描实体。 |
| P1-9 时间过滤 | 支持 RFC3339/Nano、UTC 日期和整数 Unix 毫秒；非法值明确报错，覆盖 neq/in。 |
| P1-10 跨租户恢复 | 租户作用域验证备份来源，通用任务入口覆盖用户伪造参数；保留私有平台迁移。 |
| P1-11 生命周期作用域 | 核对路径/创建/克隆目标；SDK 和 UI 平台方法不附加隐式默认租户；记录旧 Raft 命令回放语义。 |
| P1-12 Compose 暴露 | 默认绑定 127.0.0.1；显式环境变量可更改。 |
| P2 WAL 完整损坏尾帧 | 保持拒绝启动。完整坏帧不等于未确认帧，自动截断可能丢弃已确认数据。 |
| P2 Drain 再当选 | 保持时钟和投票，自动移交健康未 Drain 副本；没有合适副本时需要运维恢复。 |
| P2 catalog 回退/需求丢失 | 拒绝旧版本覆盖；后台失败保留队列需求并重试，关闭可取消。 |
| P2 存储错误码 | 未分类存储错误为脱敏 500；已知参数错误为 400，避免把客户端错误变成 Raft 应用故障。未知写结果不承诺自动重试安全。 |
| P2 解码顺序 | direct/WAL ingest 在 JSON 解码前获得 HTTP 写入准入，WAL 受理后在 wait=committed 前释放槽位。 |
| P2 租户监控登记 | 成功命中路由且存在提交版本后才登记，未知请求头不进入持久监控列表。 |
| P2 数值/非标量/分页 | 统一正负零新键并兼容旧键；不能证明标量覆盖则扫描实体；分页游标记录真实物理分片。 |

已终态失败的旧 batch 需先对账，再使用新 batch_id 重放；本版不改写历史受理结果。
Raft 升级先结束生命周期/恢复任务，混合版本窗口暂停这些管理操作。
