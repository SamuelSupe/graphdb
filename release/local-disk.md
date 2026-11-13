# GGraphDB v2.6.0-preview.2

大图分片预览用于隔离环境试用。Raft 默认协议为 1。大图分片按租户显式启用，分片异步受理另需在 home group 开启 WAL。稳定版下载仍指向 [2.5.1](https://github.com/SamuelSupe/graphdb/releases/tag/v2.5.1)。

## 分片协议

- 协议 6–7 增加多数派持久 WAL 受理、切主恢复、幂等重试，以及终态回执和历史暂存输入回收。
- 协议 8 支持分区准备、收尾和 GC 批处理。协议 9 合并新 GC 周期的页保护标记。协议 10 在应用锁外规划扫页，并让下一批 WAL 提前规划。
- 协议 11 对新准备命令启用均衡叶页分裂。旧日志仍按原修订号重放，已准备事务保留原页根。写入协程对暂存文件同步后再重命名，复制回滚和目录同步屏障继续生效。
- 协议 12 在 WAL 已积压时合并 home group 的上一批终态回执和下一批事务建立。扩大每轮分区 GC 调度范围，复用小型复制回滚日志以减少文件创建和目录同步。

从 preview.1 升级时，先保持协议 11 替换所有相关副本和 Router，再在维护窗口为 home group 的全部投票副本显式设置 `GRAPHDB_RAFT_PROTOCOL_VERSION=12`。目录组可继续使用 5，另一数据组可继续使用 11。协议 12 持久化后，preview.1 拒绝打开原 home 目录。回退需使用激活前的完整集群归档。自动滚动脚本的 inventory 范围为协议 1–4。[启用、备份和回退步骤](https://github.com/SamuelSupe/graphdb/blob/v2.6.0-preview.2/docs/partition-wal-ingestion.zh-CN.md#显式启用与升级)

## 实测与边界

九个 Raft 节点和两个 Router 在同一台接电宿主机的普通磁盘卷上，以每秒 2 批运行，每个阶段 15 分钟。GC 间隔 45 秒，快照阈值 500 条日志。

- 原负载冷写提交 P99 从 **7.69 秒降到 2.14 秒**，受理 P99 为 218 ms。覆盖写提交 P99 从 **1.01 秒降到 0.66 秒**。
- 28,800 个不重复实体首次建立采集身份，提交 P99 从 **1.01 秒升到 1.22 秒**，受理 P99 为 267 ms。
- 5,400 批全部提交，图内容摘要一致。各阶段完成 15–17 轮 GC，末五分钟提交速率约 2 批/秒。停写后可回收历史页归零。生产容量、多宿主机容灾和在线整图备份恢复仍待验收。[实测与恢复记录](https://github.com/SamuelSupe/graphdb/blob/v2.6.0-preview.2/docs/validation-v2.6.0-preview.2.md)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.0-preview.2-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.0-preview.2-linux-arm64.tar.gz` |

将所选归档和对应的 `.sha256` 文件放在同一目录。以 Linux AMD64 为例：

```sh
archive=graphdb-v2.6.0-preview.2-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.0-preview.2-linux-amd64/graphdb version
```

完整版包含 macOS ARM64、源码、SDK、部署示例和门禁证据。Go 工具链为 1.27.3。`BUILD-METADATA.json` 记录版本和提交，`release/evidence/raft-gate/partitioned-protocol12/` 记录协议 11→12 升级、旧日志重放和协议 12 恢复结果。
