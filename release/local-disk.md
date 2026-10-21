# GGraphDB v2.4.0

## 中文

2.4.0 承接 2.3.10，重点改进大租户索引的读取成本、局部整理，以及快照失败和缓存发布时的恢复行为。

- 新增可选的邻接 ID 树。完整边数据继续按 edge.ID 排列，稀疏的正向／反向遍历按端点定位邻接项，避免逐页扫描完整边桶。邻接更新只复制触及的叶页和祖先路径，字段更新不改端点时复用原根；不以整桶重写换取读优化。
- 实体、边和邻接页在删除或缩小时有界地合并相邻小页，保留拆分／合并滞回。单机后台整理只有一个 worker，按片释放资源并限制平均 I/O；Raft 不执行独立的非复制目录发布。完整目录的成本仍随页数增长。
- 范围扫描使用编译好的边界、保守的 Parquet 行组裁剪和最多一页的网络预取。已解码的热页不会因原始字节缓存淘汰而重新 GET；反向目录复用一次性指纹。续页按 afterID 之后的候选精确计费，取消、失败与提前结束均释放预取名额。
- 快照持续失败时，Leader 在原五秒重试和拒写保护下探测健康候选，再有条件让位。候选可按需生成并持久化快照，避免默认 1000 条间隔及副本快照错位阻止恢复；身份、任期、成员、应用屏障和持久化证明均受检查。共享磁盘故障不会触发无依据的反复切主。
- 缓存发布与失败加载完成按租户排序；元数据读取、排队、异步加载和显式刷新都能响应新图发布。先释放加载名额、ReadView 再唤醒等待者；独立调用方的取消和期限继续生效，有效等待者不继承首个调用方的取消。只有有效且满足版本的替代图才能消化旧读取错误。
- 快照构建先关闭捕获源，再发布完成结果，确保单构建限制覆盖捕获目录和读视图的释放阶段。

### 启用与兼容

`GRAPHDB_INDEX_RANGE_PAGES` 与 `GRAPHDB_INDEX_ADJACENCY_PAGES` 仍默认关闭，后者依赖前者。先升级全部相关读者、写者、GC 节点及 Raft 副本，再统一开启；已有邻接索引须显式重建，无根旧桶不会在业务写入时自动整桶转换。

旧布局继续读写。全局 `ListEdges` 的顺序、游标编码、版本、查询绑定及 generation 契约不变。旧二进制不支持新目录；索引降级须先使用新程序关闭相关开关并重建旧布局。此流程不构成整个 WAL／Raft 数据目录的降级许可。升级前保留已验证备份，滚动期间暂停生命周期／恢复操作。HTTP `/v1`、Go 模块 `/v2`、逻辑 `data_hash` 和独立 Raft 协议编号不变。

[索引实现与成本边界](https://github.com/SamuelSupe/graphdb/blob/main/docs/entity-range-paging-implementation.zh-CN.md) · [升级手册](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md) · [2.4.0 验证记录](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.4.0.md)

原发行门禁保持不变：全量 unit／vet／race／SDK、direct／WAL 恢复、S3／网关、真实 v2.3.0 滚动、增强及协议 3、单机与 Raft 各 30 分钟长测、构建解包与校验。资产仅在全部必需门禁通过后生成；最终独立下载和新目录恢复记录更新于 main。既有标签和资产不变，不作生产容量、跨宿主机或天级稳定性承诺。

## English

2.4.0 builds on 2.3.10 with bounded adjacency reads and local page merging, more efficient range scans, and recovery fixes around snapshots and cache publication.

- Optional copy-on-write adjacency ID trees seek sparse outgoing/incoming edges without scanning every full data page. Data pages remain ordered by edge ID. Updates copy touched leaves and ancestor paths; payload-only updates reuse the adjacency root.
- Local page merging uses bounded neighbor work and split/merge hysteresis. A single standalone background worker limits average I/O and releases resources between slices; Raft replicas do not publish independent catalogs. Flat-directory work still grows with page count.
- Compiled range boundaries, conservative Parquet pruning and one-page network prefetch reduce read work. Decoded hot pages avoid unnecessary object GETs after raw-byte eviction. Reverse directories reuse a cached fingerprint. Continuation cost excludes rows before afterID; cancellation and early termination release prefetch capacity.
- Persistent snapshot failure can trigger a fenced candidate probe and controlled leadership transfer while preserving five-second retries and write refusal. A caught-up candidate may persist an on-demand snapshot, including with the default 1,000-entry interval and skewed snapshot positions. Identity, membership, term, applied barrier and persistence proof remain required; shared capacity failure does not cause blind leadership rotation.
- Tenant-ordered cache publication covers metadata, admission, asynchronous loading and explicit refresh. Old work releases its slot and read view before completing. Caller cancellation and deadlines remain independent; live waiters retry an abandoned initiating caller. Real storage errors remain errors unless a valid resident replacement satisfies the requested version.
- Snapshot workers close captured sources before reporting completion, so the single-build limit also bounds checkpoint and read-view lifetimes.

Both `GRAPHDB_INDEX_RANGE_PAGES` and `GRAPHDB_INDEX_ADJACENCY_PAGES` remain disabled by default; adjacency requires range pages. Upgrade all readers, writers, GC nodes and replicas before activation, then explicitly rebuild existing adjacency indexes. Legacy buckets without roots do not undergo automatic whole-bucket conversion on business writes.

Legacy layouts remain readable and writable. Global `ListEdges` ordering and cursor encoding, version/query binding and generation contracts are unchanged. Old binaries cannot read new catalogs. Use the new program with the relevant flags disabled to rebuild the legacy layout before an index downgrade; this does not qualify a complete WAL/Raft directory downgrade. Keep verified backups and pause lifecycle/restore operations during rolling upgrades. HTTP `/v1`, Go module `/v2`, logical `data_hash` and Raft protocol identifiers remain unchanged.

All original gates remain required, including both thirty-minute soaks. The validation record separates local results, exact-tag gates, publication and independent downloaded-asset/new-directory recovery checks. Existing tags and assets remain immutable. No production-capacity, cross-host or day-scale stability claim is made.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.4.0.tar.gz.sha256
tar -xzf graphdb-v2.4.0.tar.gz
cd v2.4.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
```
