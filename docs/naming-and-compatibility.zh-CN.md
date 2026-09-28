# GGraphDB 2.0 契约

[English](naming-and-compatibility.md)

2.0 是以本地盘为在线主存储的主版本；S3 兼容对象存储仅用于快照备份与按需恢复。
一个本地数据目录只能由一个进程占用。不支持远端在线存储、PostgreSQL 协调、独立
reader/writer、多实例共享目录或网络文件系统。

2.0 不提供 1.x 自动迁移、旧 `data_md5` 响应或跨版本回滚保证。部署使用新的数据目录，
保留的 1.x 安装与备份独立管理。2.0 的备份恢复使用 2.0 格式，不要用旧二进制打开 2.0 数据。

提交结果改用 `data_hash`：`sha256-shards-v2:` 后接 64 位小写十六进制摘要。
它标识逻辑图内容，不包含版本号和时间戳；算法见[摘要规范](content-hash-v2.md)。
无变化写入维持当前版本和摘要，幂等重试返回原结果。`expected_version`、`min_version`、
游标版本检查及 WAL 受理、发布、终态区分继续支持。

HTTP 路径继续为 `/v1/...`，这是路由命名空间，不是产品版本。GraphQL 入口为
`POST /v1/query/graphql`；旧文本 DSL 别名仍然是文本 DSL。`extensions/v1.1/` 等目录名
是布局标识，不代表跨版本兼容承诺。

Go/Python SDK 均为 2.1.0。Go 模块为 `github.com/SamuelSupe/graphdb/v2`，SDK 导入路径为
`github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`。

2.1 沿用 2.0 本地数据格式，新增可选 S3 自动化；见[升级步骤](user/release-deployment.zh-CN.md#从-20-升级)。
