# GGraphDB v2.6.0-preview.3

大图分片和分片 WAL 继续作为显式启用的预览。Raft 默认协议为 1，home group 支持显式启用协议 12。稳定版下载见 [2.5.1](https://github.com/SamuelSupe/graphdb/releases/tag/v2.5.1)。

## WAL 恢复与分片读取

- 分片 WAL 在复制检查点中记录待处理收据索引。首次读取没有索引的旧卷时，从原收据重建。历史收据的原身份、状态 URL、受理 LSN 和结果继续可用，已配置保留期的回执按原期限回收。
- 单机 WAL 检查点按待处理请求数预分配列表，沿用 v1 持久化格式。分片页加载省去解码后重复扫描每行 JSON 的开销。
- 协议 6 对照中，保留一万张终态收据且还有一批待处理时，队列重建的收据读取由 10,001 次降为 1 次。跨 GC 持续写入的提交 P99 在两档负载下均上升约 9%。[性能对照及测试范围](https://github.com/SamuelSupe/graphdb/blob/v2.6.0-preview.3/docs/performance-wal-gc-2026-11-18.zh-CN.md)

从 preview.2 升级时沿用各组现有协议。首次激活协议 12 按维护窗口流程执行，激活后的 home 目录依照原协议下限管理。[启用、升级及回退步骤](https://github.com/SamuelSupe/graphdb/blob/v2.6.0-preview.3/docs/partition-wal-ingestion.zh-CN.md#显式启用与升级)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.0-preview.3-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.0-preview.3-linux-arm64.tar.gz` |

将归档与对应的 `.sha256` 放在同一目录。以下命令校验 Linux AMD64 归档并检查版本。

```sh
archive=graphdb-v2.6.0-preview.3-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.0-preview.3-linux-amd64/graphdb version
```

完整版还包含 macOS ARM64 硬件对应的二进制、源码、SDK、部署示例和门禁证据。`BUILD-METADATA.json` 记录版本和提交。`release/evidence/raft-gate/partitioned-protocol12/` 记录协议 11 到 12 的升级和恢复结果。
