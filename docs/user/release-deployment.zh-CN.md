# GGraphDB 2.4.3 发行版部署

[English](release-deployment.md) · [用户指南](README.zh-CN.md) · [下载发行版](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.3)

先选择部署，再校验发行包并启动。单机使用一个进程和一个持久化本地目录，
每个 Raft 副本使用独立目录。

## 选择部署

| 模式 | 包内示例 | 运维指南 |
| --- | --- | --- |
| 单机 | [docker-compose.yml](../../docker-compose.yml) | [本地磁盘、持久化与恢复](../local-disk.zh-CN.md) |
| 单组 Raft | [docker-compose.raft.yml](../../docker-compose.raft.yml) | [Raft 运维](../raft-operations.zh-CN.md) |
| 租户分片 Raft | [docker-compose.sharded.yml](../../docker-compose.sharded.yml) | [分片与迁移](../sharding.zh-CN.md) |

以下步骤启动单机。集群部署及验收范围见对应指南与[发布验收状态](../raft-release-readiness.zh-CN.md)。

## 安装发行包

### 1. 下载并校验

从 [2.4.3 Release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.3)
下载 `graphdb-v2.4.3.tar.gz` 和 `graphdb-v2.4.3.tar.gz.sha256`，放在同一目录。

Linux 上执行：

```sh
sha256sum -c graphdb-v2.4.3.tar.gz.sha256
tar -xzf graphdb-v2.4.3.tar.gz
cd v2.4.3
sha256sum -c SHA256SUMS
```

macOS 上将两处 `sha256sum -c` 换成 `shasum -a 256 -c`。

### 2. 选择程序

| 平台 | 程序 |
| --- | --- |
| Linux amd64 | `bin/graphdb-linux-amd64` |
| Linux arm64 | `bin/graphdb-linux-arm64` |
| macOS Apple Silicon | `bin/graphdb-darwin-arm64` |

### 3. 启动服务

以 Linux amd64 为例，在解压后的目录执行：

```sh
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/var/lib/graphdb-v2 bin/graphdb-linux-amd64 serve
```

服务用户必须有数据目录的写权限。其他平台使用表中对应程序，并选择可写目录。

### 4. 检查并打开

在另一个终端检查进程存活：

```sh
curl -fsS http://127.0.0.1:8080/v1/health
```

打开 **[http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/)**，按[写入与查询示例](../../README.zh-CN.md#写入与查询)
操作。接入流量前检查[就绪状态](../local-disk.zh-CN.md#就绪与内存预算)。

## 使用容器

在仓库根目录或解压后的 `v2.4.3/` 目录执行：

```sh
docker compose up -d --build
```

对数据目录执行离线 CLI 前必须停止服务，在线管理使用 HTTP API。

<a id="从-20-升级"></a>

## 升级已有安装

| 已有安装 | 升级规则 |
| --- | --- |
| 单机 2.0／2.1／2.2 | 停止旧进程，使用 2.4.3 沿用兼容目录 |
| Raft | 遵循[已验收的滚动窗口](../raft-rolling-upgrade.zh-CN.md)；未验证版本使用维护或迁移流程 |
| 1.x | 使用新目录，不提供 1.x 数据迁移 |

### 单机升级步骤

1. 创建并验证快照，保留升级前备份用于回滚。
2. 停止旧进程，禁止两个版本同时使用同一目录。
3. 替换程序，使用相同 `GRAPHDB_DATA_DIR` 和 prefix 启动 2.4.3。
4. 检查就绪、代表性查询和 WAL 状态，再恢复流量。

不支持启用新自动化后直接降级旧进程。Raft 协议 3 启用后，最高协议为 2 的旧程序
不能打开同一副本目录。详见[版本边界](../naming-and-compatibility.zh-CN.md)。

## 对外开放前

| 需求 | 配置 |
| --- | --- |
| 认证、租户授权与 TLS | 配置[网关](../security-deployment.zh-CN.md) |
| 跨机器恢复 | 配置[对象快照](../object-backup.zh-CN.md) |
| 定时备份 | 显式开启[单机调度与保留策略](../object-backup.zh-CN.md#自动备份)，或使用[备份 worker](../backup-automation.zh-CN.md) |
| 客户端兼容 | 按[版本边界](../naming-and-compatibility.zh-CN.md)选择配套 SDK，使用 `data_hash` 契约 |

### 2.4.2 WAL 边界迁移

先保留升级前已验证备份。2.4.2 及之后版本启动可读取旧 v1 durable marker，并迁移为双槽 v2；旧程序不能读取新 marker。降级前先排空 WAL，备份/恢复包含整个 WAL 目录，不要单独替换 `.durable` 或 `.checkpoint`。需要时由新程序完成重放或恢复到新目录；索引重建降级不等于 WAL/Raft 目录降级资格。
