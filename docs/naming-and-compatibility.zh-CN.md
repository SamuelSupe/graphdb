# GGraphDB 命名与 1.0 兼容

对外产品名统一为 **GGraphDB**。

规范查询协议名称为 **GraphQL**，入口是 `POST /v1/query/graphql`。它接收
GraphQL document 和变量，并返回 GraphQL `data`/`errors` envelope。schema 与
1.1 边界见 [graphql.zh-CN.md](graphql.zh-CN.md)。

为避免破坏 1.0 部署和客户端，以下技术标识在 1.1 保持不变：

| 兼容标识 | 1.1 策略 |
| --- | --- |
| `graphdb` 二进制、仓库和 module path | 保留。 |
| `GRAPHDB_*` 环境变量 | 保留。 |
| `X-GraphDB-*` 响应和控制 header | 保留。 |
| `graphdb` 对象前缀和 layout key | 保留。 |
| Go package `graphdb`、Python `GraphDBClient` | 保留。 |
| `/v1/query/gql`、`graphdb gql`、SDK `GQL`/`gql` | 已弃用的 1.0 文本 DSL 别名；不是 GraphQL。 |

新的对外文档不得再把 `FIND`/`MATCH` 兼容 DSL 称为 “GQL”。该名称既不是
GGraphDB 的查询语言名，也不再作为旧语法缩写。
