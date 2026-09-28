# GGraphDB v2.1.1

## 中文

2.1 增加可选的 S3 备份自动化，并收敛本地运行时的任务、缓存和退出流程。
兼容 2.0 本地数据格式；停止旧进程后可以沿用数据目录。首次启用前建议创建并验证备份。
不支持 1.x 数据迁移、跨实例复制或高可用；HTTP `/v1` 与 Go 模块 `/v2` 保持不变。

- 按租户定时备份，持久化调度状态，失败指数退避并复用原捕获版本。
- 完整 SHA-256/长度校验、可选恢复演练、数量/期限保留策略及可恢复删除。
- 自动化状态和重置 API，Go/Python SDK 同步升级至 2.1.1。
- 关闭策略后仍处理已有任务终态并回收成功捕获；调度文件受租户代次保护。
- GC 延迟回收被旧查询引用的文件，新查询可以继续；文件发布不再遍历其他租户视图。
- 统一任务准入、读缓存发布和退出，修复任务已完成却拒绝后续操作等并发窗口。
- 修复强制 WAL 落盘与失败重试交错导致恢复超时的问题，同时保留持续故障退避。

2.1.0 候选在 CI 发现此恢复缺陷后取消，未发行；2.1.1 包含修复并重新执行发布门禁。

自动备份默认关闭。配置 S3 后，按租户设置 `backup.enabled=true`；默认每日一次、
保留 30 份且不超过 30 天。手动备份不会自动删除，每个运行自动清理的实例须独占桶/前缀。
操作与权限见 [中文备份指南](https://github.com/SamuelSupe/graphdb/blob/v2.1.1/docs/object-backup.zh-CN.md)。

性能结论仅限已验证的局部成本减少，不承诺整体吞吐或长尾提升。
检查范围和限制见包内 `docs/validation-v2.1.1.md`，CI 原始记录在 `release/evidence/`。

## English

2.1 adds opt-in automated S3 snapshots and simplifies local task, cache and shutdown
ownership. Existing 2.0 data directories remain usable after stopping the old
process. Take and verify a backup before upgrading. There is no 1.x migration,
distributed replication or high availability. HTTP `/v1` and Go module `/v2` remain.

Schedules persist their task identity, retry the original capture with exponential
backoff, verify the complete payload, optionally rehearse restoration, and prune
only eligible automatic snapshots with resumable deletion. Both SDKs are 2.1.1.
Disabling scheduling still finalizes existing cycles and reclaims successful local
captures. Stale tenant generations cannot write schedule state.

GC protects older read views without excluding new queries. File publication
avoids unrelated tenant scans; retention stops pagination at its deletion budget.
Unified lifecycle and task admission also fix terminal-state and shutdown races.
Forced WAL flushes survive in-flight publication failures without disabling
backoff for repeated faults. The unpublished 2.1.0 candidate was cancelled after
CI exposed this recovery timeout; 2.1.1 includes the fix and reruns release gates.

Automation remains disabled until explicitly enabled per tenant. Defaults are daily,
30 snapshots and 30 days; manual snapshots are protected. Each installation running
retention must own its writable bucket/prefix. See the
[backup guide](https://github.com/SamuelSupe/graphdb/blob/v2.1.1/docs/object-backup.md).

This release makes no overall throughput or tail-latency claim. Validation scope
and limitations are recorded in `docs/validation-v2.1.1.md`; release-gate evidence
is included in the archive.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.1.1.tar.gz.sha256
tar -xzf graphdb-v2.1.1.tar.gz
cd v2.1.1
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, source, SDKs, OpenAPI,
bilingual documentation, deployment examples, build metadata and gate evidence.
