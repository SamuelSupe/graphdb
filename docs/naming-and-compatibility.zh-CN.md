# GGraphDB 版本与兼容性契约

[English](naming-and-compatibility.md)

当前发行版为 **2.1.2**，由 `main` 开发。每个本地数据目录只由一个进程占用；
S3 兼容对象存储仅用于可选快照备份与按需恢复。不支持远端在线存储、PostgreSQL
协调、独立 reader/writer、多实例共享目录或网络文件系统。

## 版本标识

| 标识 | 当前契约 |
| --- | --- |
| 产品与发行标签 | `VERSION` 为 `2.1.2`；标签为 `v2.1.2` |
| Go/Python SDK 与 OpenAPI 文档版本 | `2.1.2` |
| Go 模块 | `github.com/SamuelSupe/graphdb/v2` |
| HTTP 路由命名空间 | `/v1/...`，不代表产品主版本 |
| Parquet/WAL 与快照格式 | 2.0 引入，2.1 沿用 |

Go SDK 导入路径为 `github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`。
`extensions/v1.1/` 等目录名是布局标识，不是产品版本，也不构成跨主版本兼容承诺。
安全支持范围见 [SECURITY.md](../SECURITY.md)。

## 安装与升级

- 新安装及替换 1.x 时使用新目录。不提供 1.x 自动迁移或旧 `data_md5` 响应。
  1.x 安装与备份独立保留，不得用 1.x 二进制打开 2.x 数据。
- 2.1 沿用 2.0 本地数据与快照格式。停止旧进程后，可以在相同数据根目录和 prefix 下
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
兼容控制路由中的 `reader`、`writer`、`fleet` 名称描述本地状态，不代表支持分布式部署。

## 发行状态与性能承诺

2.1.2 已发行，单元/vet/race、SDK、HTTP/重启、S3 备份恢复和发行包检查通过。
30 分钟混合负载按本次发布要求提前停止，状态为**未完成**，不能计为持续负载通过。
后续发行的默认标签工作流仍保留该门禁。

具体证据见 [2.1.2 验证记录](validation-v2.1.2.md)和[写入长尾实测](performance-write-tail.md)。
容量状态仍为 `performance_unqualified`；稳定发行标签不代表所有规模和延迟目标均获认证。
历史报告仅描述各自版本。
