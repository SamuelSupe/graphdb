# GGraphDB 2.0：release-deployment

本版使用单机单进程和持久化本地目录。

- [启动、写入和查询](../../README.zh-CN.md)
- [配置、目录独占、备份恢复与验证](../local-disk.zh-CN.md)
- [API 用户手册](README.zh-CN.md)

容器启动：

```sh
docker compose up -d --build
```

停止服务后才能对同一目录执行离线 CLI。在线操作使用 HTTP API。

## 发行包

从 [2.1 Release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.1.2) 下载压缩包和校验和，
校验压缩包及内部二进制后启动：

```sh
sha256sum -c graphdb-v2.1.2.tar.gz.sha256
tar -xzf graphdb-v2.1.2.tar.gz
cd v2.1.2
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

ARM Linux 使用 `graphdb-linux-arm64`，Apple Silicon 使用 `graphdb-darwin-arm64`。
从 1.x 升级需使用全新的目录，不提供 1.x 数据迁移。对外开放前，在网关配置认证、租户授权和 TLS；
跨机器恢复配置见[对象快照备份](../object-backup.zh-CN.md)。提交结果改为 `data_hash`，客户端同步升级至 2.0 SDK。

## 从 2.0 升级

先创建并验证快照，停止旧进程，再替换二进制，使用相同的 `GRAPHDB_DATA_DIR` 和 prefix 启动
2.1.2。数据格式兼容，目录仍为进程独占，不能同时运行两个版本。恢复流量前检查 readiness、
代表性查询和 WAL 状态。保留升级前备份用于回滚；不支持启用新自动化后直接降级旧进程。
S3 自动化需显式开启，见[调度与保留策略](../object-backup.zh-CN.md#自动备份)。
