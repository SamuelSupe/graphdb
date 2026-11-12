# GGraphDB v2.6.0-preview.1

大图分片预览用于隔离环境试用。Raft 默认协议为 1。大图分片按租户显式启用，分片异步受理另需在 home group 开启 WAL。稳定版下载仍指向 [2.5.1](https://github.com/SamuelSupe/graphdb/releases/tag/v2.5.1)。

## 分片协议

- 协议 6–7 增加多数派持久 WAL 受理、切主恢复、幂等重试，以及终态回执和历史暂存输入回收。
- 协议 8 支持分区准备、收尾和 GC 批处理。协议 9 合并新 GC 周期的页保护标记。协议 10 在应用锁外规划扫页，并让下一批 WAL 提前规划。
- 协议 11 对新准备命令启用均衡叶页分裂。旧日志仍按原修订号重放，已准备事务保留原页根。写入协程对暂存文件同步后再重命名，复制回滚和目录同步屏障继续生效。

先在原协议下升级所有相关副本和 Router，再在维护窗口逐组统一设置 `GRAPHDB_RAFT_PROTOCOL_VERSION`。协议持久化后原目录保留该协议下限，低于下限的旧二进制拒绝启动。自动滚动脚本的 inventory 范围为协议 1–4。具体配置、Compose 示例、备份和回退步骤见[启用与升级](https://github.com/SamuelSupe/graphdb/blob/v2.6.0-preview.1/docs/partition-wal-ingestion.zh-CN.md#显式启用与升级)。

## 实测与边界

九个 Raft 节点和两个 Router 在同一宿主机上以每秒 2 批运行，保持 45 秒 GC 间隔和 500 条日志的快照阈值。

- 纯冷写为 28,800 个不重复实体首次建立采集身份，提交 P99 为 **84.16 秒**。末五分钟提交 1.71 批/秒，队列从 65 升至 151 批。七轮写入期 GC 中末四轮结束后剩余 9,592–9,845 个可回收历史页。
- 原负载冷阶段提交 P99 为 **67.11 秒**。末五分钟队列从 111 降至 86 批。覆盖写提交 P99 为 **1.26 秒**，队列峰值 3 批。
- 最终候选共 5,400 批全部提交，图内容摘要一致。持续冷写稳态、真实多主机容量、跨版本大图滚动升级和在线整图备份恢复仍待验收。[验证记录](https://github.com/SamuelSupe/graphdb/blob/v2.6.0-preview.1/docs/validation-v2.6.0-preview.1.md)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.0-preview.1-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.0-preview.1-linux-arm64.tar.gz` |

将所选归档和对应的 `.sha256` 文件放在同一目录。以 Linux AMD64 为例：

```sh
archive=graphdb-v2.6.0-preview.1-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.0-preview.1-linux-amd64/graphdb version
```

完整版还包含 macOS ARM64、源码、SDK、部署示例和门禁证据。Go 工具链为 1.27.3。包内 `BUILD-METADATA.json` 记录版本和提交，`release/evidence/raft-gate/partitioned/` 记录协议 11 分片恢复结果。
