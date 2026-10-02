# GGraphDB 版本与兼容性契约

[English](naming-and-compatibility.md)

当前发行版为 **2.2.2**，由 `main` 开发。每个本地数据目录只由一个进程占用；
默认单机，亦可启用独立副本的 Raft 和租户分片；S3 兼容对象存储仅用于可选快照备份与按需恢复。不支持远端在线存储、PostgreSQL
协调、独立 reader/writer、多实例共享目录或网络文件系统。

## 版本标识

| 标识 | 当前契约 |
| --- | --- |
| 产品与发行标签 | `VERSION` 为 `2.2.2`；标签为 `v2.2.2` |
| Go/Python SDK 与 OpenAPI 文档版本 | `2.2.2` |
| Go 模块 | `github.com/SamuelSupe/graphdb/v2` |
| HTTP 路由命名空间 | `/v1/...`，不代表产品主版本 |
| Parquet/WAL 与快照格式 | 单机沿用 2.0/2.1；Raft 另有协议和目录角色标记 |

Go SDK 导入路径为 `github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`。
`extensions/v1.1/` 等目录名是布局标识，不是产品版本，也不构成跨主版本兼容承诺。
安全支持范围见 [SECURITY.md](../SECURITY.md)。

## 安装与升级

- 新安装及替换 1.x 时使用新目录。不提供 1.x 自动迁移或旧 `data_md5` 响应。
  1.x 安装与备份独立保留，不得用 1.x 二进制打开 2.x 数据。
- 单机 2.2 沿用 2.0/2.1 本地数据与快照格式。停止旧进程后，可以在相同数据根目录和 prefix 下
  复用 2.0/2.1 目录。允许复用目录不代表允许不同进程或版本同时访问。
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

GraphQL 入口为 `POST /v1/query/graphql`；已弃用的文本 DSL 别名仍然是文本 DSL。
兼容控制路由中的 `reader`、`writer`、`fleet` 名称描述本地状态，不是 Raft 成员管理接口；可选分布式部署见 [Raft 运行说明](raft-ha.zh-CN.md)。

## Raft 协议与升级

产品版本、Raft 协议和快照格式分别管理。默认 Raft 协议为 1，最高支持 3。
协议 2 的流式快照/准备维护、协议 3 的准备 GC 须在全组具备支持后独立启用；
已持久化协议 3 的目录不能由最高协议为 2 的程序打开。滚动升级只限于
[已验证来源、目标和协议窗口](raft-rolling-upgrade.zh-CN.md)。
单机与副本目录不能直接互换；跨部署使用 API 导入或备份恢复到新目录。

## 发行状态与性能承诺

2.2.2 的验收范围见 [版本验证](validation-v2.2.2.md)，实际发布工作流和包内证据核对具体发行二进制。
候选已通过本机 30 分钟 Raft 维护负载，但存在预期 429 与十秒级写入长尾。
跨宿主机、真实容量与天级稳定性尚未验收，稳定标签不代表所有规模和延迟目标均获认证。
`release/capacity-envelope.yaml` 保留 2.1.2 的历史 `performance_unqualified` 记录，不能用作 2.2.2/Raft 容量承诺。
历史报告仅描述各自版本。
