# GGraphDB v2.3.8

## 中文

本补丁修复 v2.3.7 后续审查复现的三处写入推进问题：

- 后台索引只对明确的瞬时错误最多尝试三次。重试前释放读视图、构建内存和队列位置，
  重新读取当前图并有序重建。永久失败不再无限占用资源，索引 stale 状态可由维护修复。
- 合并等待和前驱等待响应 ctx 取消，同时保留发布顺序。取消已发布提交的等待不会回滚图数据。
- WAL HTTP 受理只获取解码／并发槽，图发布背压交给 flush；积压期间新批次可缓冲，
  已受理批次重发仍可取回确认。WAL 磁盘、队列上限和生命周期检查继续生效。
- 未准备组按剩余 commit-tail 容量拆分，避免有空位仍整组重试；tail 满时等待 compact，
  PREPARED 组不拆分。回归覆盖顺序、重启恢复和幂等性。

[版本验证记录](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.8.md)
分别记录本地回归、确切标签门禁和下载后资产验证。自动发布须等待所有必需门禁通过，
包含单机和 Raft 各 30 分钟混合负载，无门禁豁免。

HTTP `/v1`、Go 模块 `/v2`、WAL/Parquet 格式和 Raft 协议编号不变。升级前备份，
单机停止旧进程后复用目录；Raft 按滚动升级手册执行。持续磁盘／索引故障仍需运维修复；
本补丁不把长测通过解释为生产容量或跨主机容灾资格。

## English

This patch fixes three write-progress regressions confirmed after v2.3.7:

- Background index work attempts only recognized transient failures, at most three
  times. It releases views, build memory and its queue position before retrying
  from a fresh ordered snapshot. Persistent failures remain visible as stale
  indexes and eligible for maintenance repair.
- Joined and predecessor waits honor cancellation while preserving publication
  order. Cancellation after graph publication does not roll back committed data.
- WAL HTTP acceptance retains bounded decoding/concurrency but leaves publication
  pressure to flushing. New requests and duplicate acknowledgements remain
  available during tail, object-store or CAS pressure, within WAL limits.
- Unprepared groups split to use remaining commit-tail capacity. Full tails wait
  for compaction, and prepared cohorts keep atomic publication. Regression tests
  cover persistent faults, resource release, cancellation, ordering and recovery.

The linked validation record separates local results, required exact-tag gates,
including both thirty-minute soaks, and independent downloaded-asset checks.
Data formats and protocol numbers are unchanged. No production capacity,
latency or cross-host recovery claim is made.

## Verified publication / 发布核验

已发布为稳定 Latest。完整标签门禁及单机、Raft 各 30 分钟长测通过。
独立下载的源码／构建身份、双层 SHA256 及 GitHub digest 一致；Linux arm64 与 macOS arm64
真实写入、重启恢复、旧版积压 WAL 升级、背压下确认重放和损坏 catalog 后的写入／purge 检查通过。
损坏 catalog 下手动 rebuild 返回 409，未被误记为修复成功。实际长测指标与限制见版本验证记录。
最终验证文档位于 main；标签、发行二进制及资产保持不变。

Published as stable Latest after all required gates and both thirty-minute soaks.
Independent Linux/macOS downloads passed identity, checksums, recovery, old-WAL
upgrade and persistent-index-failure probes. A corrupt catalog still needs repair;
its explicit rebuild conflict is recorded, not counted as successful recovery.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.8.tar.gz.sha256
tar -xzf graphdb-v2.3.8.tar.gz
cd v2.3.8
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
```
