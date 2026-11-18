# GGraphDB v2.6.0

2.6.0 是 2.6 系列正式版。大图分片按租户启用，分片 WAL 由 home group 显式配置。Raft 默认协议为 1，协议 12 在 home group 单独激活。

## 稳定性修复

- 按租户命名空间中的路径位置识别分片快照记录。与内部目录同名的租户和存储前缀可以正常生成和恢复快照。
- 副本在下次独占打开数据目录时清理中断的快照捕获和安装文件，释放遗留的硬链接。
- 副本持久化本地 GC 待删页清单，完成删除和目录同步后再发布应用检查点。重启会完成中断的清单，后续写入可以安全复用相同内容的页。
- 租户归属分配和迁移分块暂存保留分片 WAL 待处理索引。无索引的旧检查点从原收据重建，历史收据继续提供原身份、状态 URL、受理 LSN 和结果。

## 持续写入验证

以 `v2.6.0-preview.3` 为基线，在同一台 Linux ARM64 虚拟机上对照运行。每次使用九个 Raft 副本，目录组使用协议 5，两个数据组使用协议 12。每次写入期间完成三轮 GC。表中的两组数字对应交换运行顺序后的两次对照。

| 输入 | 写入时长 | 提交 P99 (ms)，基线 → 2.6.0 | 峰值队列（批），基线 → 2.6.0 |
| --- | ---: | --- | --- |
| 6 批/秒 | 约 105 秒 | 46.56 → 56.85 / 63.60 → 48.94 | 1 → 1 / 2 → 1 |
| 24 批/秒 | 约 60 秒 | 124.10 → 86.74 / 82.10 → 77.98 | 7 → 4 / 4 → 3 |

八次运行停写时均只剩一批，最后一批在 30 ms 内可见。修复版逐条核验了 4,140 份收据，按批抽读实体并核对图版本和实体总数。这些结果适用于上述同机负载和数据规模。[标签工作流及发布门禁](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.0)

## 升级

在各组沿用原协议的情况下替换 Router 和副本二进制。首次激活协议 12 需要单独的维护窗口。协议 12 持久化后，原目录要求支持协议 12 的二进制。旧版本遗留的无标记快照目录需离线确认归属后再清理。[启用、升级及回退步骤](https://github.com/SamuelSupe/graphdb/blob/v2.6.0/docs/partition-wal-ingestion.zh-CN.md#显式启用与升级)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.0-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.0-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.6.0.tar.gz` |

将归档与对应的 `.sha256` 文件放在同一目录。以下命令校验 Linux AMD64 归档并检查版本。

```sh
archive=graphdb-v2.6.0-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.0-linux-amd64/graphdb version
```

完整版同时包含源码、SDK、部署示例和门禁证据。`BUILD-METADATA.json` 记录版本和提交。`release/evidence/raft-gate/partitioned-protocol12/` 保存协议 11 到 12 的升级和恢复证据。
