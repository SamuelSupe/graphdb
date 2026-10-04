# GGraphDB 2.x：release-deployment

[English](release-deployment.md)

默认部署使用单机单进程和持久化本地目录。新安装及替换 1.x 使用新目录；已有 2.0/2.1
目录可在停止旧进程后沿用。版本与兼容性见[版本边界](../naming-and-compatibility.zh-CN.md)。

本版还支持可选的单组 Raft 和按租户分片的 Raft，每个副本使用独立本地目录。
部署包提供 `docker-compose.raft.yml` 和 `docker-compose.sharded.yml`，见
[Raft 运维手册](../raft-operations.zh-CN.md) 与 [发布验收状态](../raft-release-readiness.zh-CN.md)。
下文升级步骤适用于单机；Raft 滚动升级只覆盖[已验收的兼容窗口](../raft-rolling-upgrade.zh-CN.md)。未验证版本使用维护或迁移流程；协议 3 启用后不能用最高协议为 2 的旧程序打开原目录。

- [启动、写入和查询](../../README.zh-CN.md)
- [配置、目录独占、备份恢复与验证](../local-disk.zh-CN.md)
- [API 用户手册](README.zh-CN.md)

容器启动：

```sh
docker compose up -d --build
```

停止服务后才能对同一目录执行离线 CLI。在线操作使用 HTTP API。

## 发行包

从 [2.2 Release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.2.4) 下载压缩包和校验和，
校验压缩包及内部二进制后启动：

```sh
sha256sum -c graphdb-v2.2.4.tar.gz.sha256
tar -xzf graphdb-v2.2.4.tar.gz
cd v2.2.4
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

ARM Linux 使用 `graphdb-linux-arm64`，Apple Silicon 使用 `graphdb-darwin-arm64`。
从 1.x 升级需使用全新的目录，不提供 1.x 数据迁移。对外开放前，在网关配置认证、租户授权和 TLS；
跨机器恢复配置见[对象快照备份](../object-backup.zh-CN.md)。提交结果改为 `data_hash`，客户端使用[版本边界](../naming-and-compatibility.zh-CN.md)列出的配套 SDK。

## 从 2.0 升级

从单机 2.0/2.1 升级到 2.2.4 也使用以下步骤。

先创建并验证快照，停止旧进程，再替换二进制，使用相同的 `GRAPHDB_DATA_DIR` 和 prefix 启动
2.2.4。数据格式兼容，目录仍为进程独占，不能同时运行两个版本。恢复流量前检查 readiness、
代表性查询和 WAL 状态。保留升级前备份用于回滚；不支持启用新自动化后直接降级旧进程。
S3 自动化需显式开启，见[调度与保留策略](../object-backup.zh-CN.md#自动备份)。
