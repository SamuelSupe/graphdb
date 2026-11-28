# GGraphDB v2.7.1

本次补丁修复了索引健康误报。全量重建或增量更新时，如果反向索引目录写入失败，健康检查曾可能提前显示 `ready`。现在反向目录持久化后才发布新目录版本。失败时索引保持过期或缺失，直到后续更新或重建成功。已提交的图数据仍然可用。

This patch fixes index health reporting when a reverse index catalog write fails during a full rebuild or an incremental update. The new index version becomes visible after its reverse catalog is durable. Committed graph data remains available.

[2.7.0 发行说明](https://github.com/SamuelSupe/graphdb/releases/tag/v2.7.0)记录此补丁沿用的功能和兼容范围。

## 升级

2.7.0 用户可沿用现有数据目录、WAL 和配置。单机先保留完整运行状态备份，停止旧进程后替换二进制。Raft 集群沿用已配置协议，按组逐个替换副本，待副本追平后再继续。升级后检查就绪状态和代表性查询，通过 `/v1/indexes/health` 确认索引追平。[部署和升级](https://github.com/SamuelSupe/graphdb/blob/v2.7.1/docs/user/release-deployment.zh-CN.md) · [Raft 滚动升级](https://github.com/SamuelSupe/graphdb/blob/v2.7.1/docs/raft-rolling-upgrade.zh-CN.md) · [索引维护](https://github.com/SamuelSupe/graphdb/blob/v2.7.1/docs/user/tasks-maintenance.zh-CN.md)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.7.1-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.7.1-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.7.1.tar.gz` |

将归档和对应的 `.sha256` 放在同一目录。

```sh
archive=graphdb-v2.7.1-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.7.1-linux-amd64/graphdb version
```

完整版包含源码、SDK、部署示例和发布门禁证据。`BUILD-METADATA.json` 记录版本和构建提交。

## 验证范围

回归测试覆盖反向目录写入失败后的健康状态，以及恢复写入并重建后重新达到 `ready`。标签流水线执行静态和集成门禁、单机与 Raft 长测、备份恢复、网关检查及原生 Linux AMD64/ARM64 归档验证。[发布流水线](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.1)
