# GGraphDB v2.3.10

## 中文

v2.3.9 的资格标签未生成发行资产。本版承接其全部修复，并补充重试期间换主的回归与滚动失败诊断；v2.3.9 和 v2.3.8 标签均保持不变。

本补丁修复 v2.3.8 审查中稳定复现的安全、查询及资源生命周期问题：

- 存储层默认只恢复同租户备份，校验 legacy key 与记录的租户身份。私有平台跨租户迁移
  必须显式声明 `source_tenant_id`；受限租户调用方不能覆盖来源。Go/Python SDK 同步支持。
- 写接口在 Raft 提案前拒绝跨站 Origin 和不支持的 Content-Type，包括不读 body 的 purge。
  NGINX、Follower 和分片路由保留公开 Host 及端口，继续接受已有 JSONL/CSV 媒体类型。
- 未分类的任务准备错误保留排队，释放锁后公平重试；只有明确的永久错误才记为失败。
- commit/ingest 已发布图数据后的结果记录错误隐藏内部路径，保留 HTTP 500 和
  `retryable:false`，避免客户端因依赖暂时不可用而盲目重放已经发布的写入。
- 数字分页游标必须提供与实际图版本匹配的 `min_version`；编码游标必须包含版本。
- WAL 结果等待在服务故障或强制关闭时退出，同时保留已受理批次供重启恢复。
- 延迟索引在捕获最新图后复核内存预算；并发增长使原预留不足时释放资源后重新准入。

- 确定性 HTTP 应用 5xx 完整回滚后记录失败响应，后续命令继续；批量失败不丢成功邻居。
  真实文件／完整性／取消错误仍停止应用并等待修复。过长的对象路径组件直接返回 400。
- WAL 在 sync 组确认前持久化带校验和的 `.durable` 边界；只自动裁掉边界之后的坏末尾。
  已确认前缀及无法证明未确认的旧坏帧继续保留，边界随整个 WAL 目录备份。
- 文件层在不区分大小写的卷上检查真实目录拼写，封住 legacy 无身份配置的租户别名读取。

本版还修复后续审查确认的写放大及性能／稳定性问题：

- 实体、正向边和反向边在 64 个逻辑桶内按 ID 范围分页，增量更新只重写触及的物理页。
  每页最多 1024 条，以实际编码后 8 MiB 为目标上限，单条超大记录仍合法。
  完整目录的编码与写入仍随页数增长；首次拆分旧大页也需要处理原页。
- `GRAPHDB_INDEX_RANGE_PAGES` 默认关闭。先升级全部相关读写节点及副本，再统一开启并重建
  大租户索引。旧程序不能安全读取范围目录；降级索引须先由新程序关闭开关并重建旧布局。
  [实现、测量及兼容边界](https://github.com/SamuelSupe/graphdb/blob/main/docs/entity-range-paging-implementation.zh-CN.md)。
- WAL 可将未完成的受理／PREPARED 状态折叠到校验过的 `.checkpoint`，让已完成租户的历史
  可以回收，避免一个故障租户永久钉住共享日志。未完成状态仍计入容量，真实持久化故障仍须修复。
- 默认 Raft 快照在后台构建文件，持久化循环处理描述符；准备及临时写入失败按五秒间隔重试，
  不因瞬时准备错误直接停节点。成员地址持久化不跨 peer 读锁，并保留并发更新。
- match 和 neighbors 分页按游标定位，过滤工作计入成本且支持取消；恢复 ReaderCache TTL。
  迁移导出不跨网络输出持锁，构建和输出均有并发上限；全量二级索引逐字段写入释放，
  已有目录安全检查允许并行读取。旧视图仍保护其物理页，释放后完整 GC 可回收无引用旧页。

[版本验证记录](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.10.md)
区分本地回归、确切标签 CI、单机和 Raft 各 30 分钟长测，以及独立下载验证。
发布资产由全部必需门禁通过后自动生成，无门禁豁免；包内保留标签时的候选记录与门禁证据，
最终下载核验记录更新于 main。v2.3.8 标签和资产保持不变。

升级前备份；单机停止旧进程后复用目录，Raft 遵循滚动升级手册。混合版本期间暂停恢复和
恢复演练等生命周期任务。客户端应使用 JSON 写入类型，并在跨租户平台迁移时声明来源。
HTTP `/v1`、Go 模块 `/v2` 和 Raft 协议编号不变。完整备份必须保留 WAL 的 `.durable`、
已生成的 `.checkpoint` 及 Raft `snapshots/`；本地快照现在使用已有 format-2 文件布局。
不能把索引布局转换验证当作整个 WAL／Raft 目录的降级资格。

## English

The v2.3.9 qualification tag produced no release assets. This patch carries its fixes forward with snapshot-retry leadership coverage and rolling-failure diagnostics; both earlier tags remain unchanged.

This patch fixes reproduced v2.3.8 restore, browser-write, preparation-retry,
cursor and resource-lifecycle defects. Restore defaults to the target tenant;
private platform migrations explicitly declare the backup source. Scoped callers
cannot override it, and legacy backup keys must match their records.

Browser mutations reject cross-site origins and unsafe media before proposal;
proxies retain the public Host and imports retain existing JSONL/CSV aliases.
Opaque preparation failures remain queued for fair retry. Post-publication
commit/ingest metadata errors retain a generic non-retryable HTTP 500.
Legacy numeric cursors require matching `min_version`; encoded cursors carry
their own version. WAL waits stop on service failure without discarding accepted
batches, and deferred indexing rechecks memory admission after graph growth.

Deterministic HTTP application failures roll back and durably record a failed
response so later commands continue; file failures and unsuccessful rollback still
require repair/replay. Object path components over 255 bytes return HTTP 400.
A checksummed durable WAL boundary is published before sync-mode ACKs, allowing
recovery to truncate only proven unacknowledged corrupt suffixes. Acknowledged
prefixes and ambiguous legacy damage remain preserved. Filesystem directory
spelling checks also reject aliases of legacy tenant records without identity.

Entity and forward/reverse edge indexes can now use bounded ID ranges within the
existing 64 logical buckets: at most 1,024 records and an 8 MiB encoded target per
page, allowing oversized single records. Incremental writes replace touched pages;
the flat catalog still grows with page count, and initial legacy-page conversion
still reads the old page. `GRAPHDB_INDEX_RANGE_PAGES` defaults to false. Upgrade
every reader and replica before enabling it; old binaries cannot safely paginate
range catalogs. Use the new binary with the flag disabled to rebuild the legacy
index layout before an index downgrade.

WAL checkpoints retain accepted/PREPARED work while reclaiming completed history,
preventing a faulted tenant from pinning all shared log segments indefinitely.
Pending work still counts against capacity. Default Raft snapshots build files in
the background and retry preparation errors; durable Raft and restore failures
remain fatal. Peer persistence preserves concurrent address changes without
holding the peer read lock across disk I/O. Cursor-seeking queries, TTL expiry,
bounded migration exports, one-field-at-a-time index builds, parallel safe directory
checks and old-view GC reclamation also have regression coverage.

The linked validation record separates local results, exact-tag gates, both
thirty-minute soaks and independent downloaded-asset checks. HTTP/module and Raft
protocol numbers are unchanged. Full backups must retain WAL `.durable` and any
`.checkpoint`, plus Raft `snapshots/`; local snapshots now use the existing format-2
file layout. Index conversion does not qualify a complete WAL/Raft directory
downgrade. No production-capacity, day-scale stability or physical cross-host
recovery claim is made.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.10.tar.gz.sha256
tar -xzf graphdb-v2.3.10.tar.gz
cd v2.3.10
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
```
