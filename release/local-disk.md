# GGraphDB v2.7.2

本补丁修复租户分片 Router 偶发截断 JSON 和 NDJSON 查询响应的问题。HTTP/1.1 转发现在允许上游继续读取请求体，同时向客户端发送响应。

- 排序后的遍历和影响查询在翻页时遵守路径数量上限。模式聚合涵盖全部匹配项。
- 邻居查询支持从边属性取值，用于排序、分组和聚合。
- 大图分片查询在 NDJSON 最终记录中返回后续游标以及请求的聚合、分组和分析结果。

This patch keeps tenant-sharded JSON and NDJSON responses intact when an HTTP/1.1 proxy starts sending a response while its upstream transport is still reading the request body. It also fixes sorted traversal and impact pagination, pattern aggregation, edge-field neighbor queries, and the final NDJSON record for partitioned graph queries.

## 升级

从 2.7.1 升级可沿用现有数据目录、WAL 和配置。单机先备份完整运行状态，停止旧进程后替换二进制。Raft 集群沿用当前协议，按组逐个替换副本，待副本追平后继续。租户分片部署还需滚动更新 Router，在停止每个实例前先从入口摘流。升级后检查就绪状态，并通过入口验证代表性的 JSON 和 NDJSON 查询。[部署和升级](https://github.com/SamuelSupe/graphdb/blob/v2.7.2/docs/user/release-deployment.zh-CN.md) · [Raft 滚动升级](https://github.com/SamuelSupe/graphdb/blob/v2.7.2/docs/raft-rolling-upgrade.zh-CN.md) · [查询指南](https://github.com/SamuelSupe/graphdb/blob/v2.7.2/docs/user/read-query.zh-CN.md)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.7.2-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.7.2-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.7.2.tar.gz` |

将归档和对应的 `.sha256` 放在同一目录。

```sh
archive=graphdb-v2.7.2-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.7.2-linux-amd64/graphdb version
```

完整版包含源码、SDK、部署示例和发布门禁证据。`BUILD-METADATA.json` 记录版本和构建提交。

## 验证范围

本地补丁候选在双 Router、双数据分片集群中验证了 195,748 次查询。直连与 HAProxy 两条路径均覆盖 JSON 和 NDJSON，逐条验证响应体、结果顺序、聚合值和分页游标。NDJSON 还检查最终记录中的 `done: true`。标签流水线记录静态与集成门禁、单机和 Raft 长测、备份恢复、网关检查及 Linux AMD64/ARM64 归档验证。[发布流水线](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.2)
