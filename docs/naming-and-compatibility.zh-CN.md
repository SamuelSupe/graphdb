# GGraphDB 版本与兼容性契约

[English](naming-and-compatibility.md)

当前发行版本为 **2.4.4**，发布及下载核验见[版本验证](validation-v2.4.4.md)。每个本地数据目录只由一个进程占用；
默认单机，亦可启用独立副本的 Raft 和租户分片；S3 兼容对象存储仅用于可选快照备份与按需恢复。不支持远端在线存储、PostgreSQL
协调、独立 reader/writer、多实例共享目录或网络文件系统。

## 版本标识

| 标识 | 当前契约 |
| --- | --- |
| 产品与发行标签 | `VERSION` 为 `2.4.4`；标签为 `v2.4.4` |
| Go/Python SDK 与 OpenAPI 文档版本 | `2.4.4` |
| Go 模块 | `github.com/SamuelSupe/graphdb/v2` |
| HTTP 路由命名空间 | `/v1/...`，不代表产品主版本 |
| Parquet/WAL 与快照格式 | Parquet/逻辑快照沿用2.x；2.4.2把WAL durable marker迁移为双槽v2；Raft另有协议和目录角色标记 |

Go SDK 导入路径为 `github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`。
安全支持范围见 [SECURITY.md](../SECURITY.md)。

## 安装与升级

- 新安装及替换 1.x 时使用新目录。不提供 1.x 自动迁移或旧 `data_md5` 响应。
  1.x 安装与备份独立保留，不得用 1.x 二进制打开 2.x 数据。
- 单机 2.4.4 可打开 2.x 的本地 Parquet 与逻辑快照；旧 WAL durable marker 会迁移为双槽 v2。降级前先排空 WAL，旧程序不能直接打开新边界。停止旧进程后，可以在相同数据根目录和 prefix 下
  复用 2.0/2.1/2.2 目录。允许复用目录不代表允许不同进程或版本同时访问。
- 升级兼容不承诺直接降级二进制或跨主版本回滚。保留经过验证的升级前备份，并遵循
  [升级步骤](user/release-deployment.zh-CN.md#从-20-升级)。
- 2.1 新增可选 S3 调度、重试、保留清理与恢复演练。逻辑图快照不包含全部运行状态；
  制定灾难恢复方案前须阅读[备份范围](object-backup.zh-CN.md)。

## 数据与 API 契约

提交结果使用 `data_hash`：`sha256-shards-v2:` 后接 64 位小写十六进制摘要。
它标识逻辑图内容，不包含版本号和时间戳；算法见[摘要规范](content-hash-v2.md)。
它与旧版整图规范 JSON 的 MD5 是不同契约。无变化写入维持当前版本和摘要，幂等
重试返回原结果。`expected_version`、`min_version`、游标版本检查及 WAL 受理、
发布、终态区分继续支持。

2.2.4 在 Parquet 列和对象内容摘要中统一以 UTC 表示类型化时间，覆盖实体/边来源、
任务及幂等元数据。时间代表的瞬间与普通用户字段保持不变；已有 UTC 编码和逻辑 `data_hash`
继续兼容。旧对象若因非 UTC 摘要与 UTC 列不一致而校验失败，仍会被拒绝；应从已验证备份
恢复，或把可信输入重放到新目录。本次修复不会恢复已经丢失的时区偏移，也不会绕过内容校验。

GraphQL 入口为 `POST /v1/query/graphql`；已弃用的文本 DSL 别名仍然是文本 DSL。
兼容控制路由中的 `reader`、`writer`、`fleet` 名称描述本地状态，不是 Raft 成员管理接口；可选分布式部署见 [Raft 运行说明](raft-ha.zh-CN.md)。

## Raft 协议与升级

产品版本、Raft 协议和快照格式分别管理。默认 Raft 协议为 1，最高支持 4。
协议 2 的流式快照/准备维护、协议 3 的准备 GC 须在全组具备支持后独立启用；
已持久化协议 3 的目录不能由最高协议为 2 的程序打开。滚动升级只限于
[已验证来源、目标和协议窗口](raft-rolling-upgrade.zh-CN.md)。
单机与副本目录不能直接互换；跨部署使用 API 导入或备份恢复到新目录。

## 发行状态与性能承诺

本发行的实际资格见 [2.4.4 验证](validation-v2.4.4.md)、标签工作流与包内证据。
此前性能候选的三十分钟长测完成 45,116 次操作，在显著共享内核资源压力下仍有四次 HAProxy 503 查询失败。
该轮为 FAIL，继续保留；不能将其转为发行 PASS，也不能据环境争用证明服务端无可用性缺陷。
跨宿主机、生产容量和天级稳定性仍未资格验证。快照捕获微基准不代表整体读写吞吐提升。
容量说明保留历史 2.1.2 的 `performance_unqualified`，不认证本发行或 Raft 容量。

## 2.4 范围页与邻接树

范围页与邻接页仍分别默认关闭。升级全部读写节点、GC 节点和 Raft 副本后，才可统一开启 `GRAPHDB_INDEX_RANGE_PAGES`；邻接树另需 `GRAPHDB_INDEX_ADJACENCY_PAGES`，并显式重建已有索引。旧布局继续读写，`ListEdges` 的全局顺序、旧游标的版本与查询绑定不变。旧程序不支持新目录；索引降级前须用新程序关闭相关开关并重建旧布局，这不构成整个 WAL／Raft 目录的降级许可。页数据改写有界，平面目录成本仍随页数增长。详见[实现与兼容边界](entity-range-paging-implementation.zh-CN.md)。

## 2.4.3 租户元数据恢复

单机默认对全部租户在线恢复，无需配置。有效 pending 或副本保留原状态及字段；无有效记录时，仅在 manifest 有效且无生效中的 purge 标记时按 active 重建。新增控制文件不改变图/WAL 编码。降级前先由新程序完成 pending 恢复；旧程序不识别该未完成意图。Raft 读取不会执行本地修复写入。详见[运维契约](product-operations.zh-CN.md)。

## 构建工具链

本发行统一由 Go 1.27.3 编译；go.mod 的 toolchain、CI 和 Docker 构建固定此版本，独立下载核对实际 binary build info。Go 模块最低语言版本不是发行编译器版本，HTTP/模块命名空间不变。

## 2.4.4 身份匹配与回退边界

单机自动为新提交启用无歧义身份编码；旧提交保留原重放语义，历史误合并数据不自动拆分。首次无变化写入只转换索引，不更新实体时间、版本或归属。新格式写入后不能用旧二进制原目录回退，请保留升级前备份。Raft 默认仍为协议 1；全组副本和 router 升级完成后单独启用协议 4，协议 1–3 继续保留历史写入语义。详见[协议升级手册](raft-rolling-upgrade.zh-CN.md)。
