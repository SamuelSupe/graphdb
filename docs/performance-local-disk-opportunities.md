# 本地磁盘版：下一轮性能优化空间

2026-09-16。基于当前 `codex/local-disk-v2` 未提交实现重新检查；本轮未修改生产代码。

后续实现和实测结果见[第二轮优化记录](performance-local-disk-optimization-2.md)；本文保留优化前的诊断证据。

## 证据范围

重新构建后的二进制 SHA-256 为
`d59dde0b007c4eeb6c6731faa64831926f674a5f64c80acdad4ba205222ba32a`，与上一轮最终测量版本相同。
在 OrbStack 的独立 Linux 数据副本上运行 10002 实体 / 5001 边、4 写客户端 / 16 查询客户端，
WAL 同步落盘，每客户端写入间隔 2 秒。容器上限 8 CPU / 8 GiB。
预热 5 秒，负载 22 秒，其中 CPU 采样 20 秒；另采集负载前后分配差值及结束后主动 GC 的堆快照。
为隔离请求与增量索引开销，此次关闭自动维护。44 批写入对应版本增加 44，所有请求无错误。
这是热点诊断，不能和上一轮包含维护的吞吐/RSS 数字直接比较，也不是新的性能收益证明。

证据保存在 [capacity-runs/local-disk-opportunities-20260916](../capacity-runs/local-disk-opportunities-20260916)：
`cpu.txt`、`alloc.txt`、`heap.txt`、`profile-current/` 及两个行组检查 JSONL。
`run.log` 是首次诊断脚本访问错误 metrics 端口的失败记录；有效采样为 `profile-current`。

## 优先级

### 1. 分页重复扫描与目录哈希：优先处理

- `selectBoundedScanCandidates` 调用栈占 CPU 样本 **17.25%**。
  [scan_fallback.go](../internal/storage/scan_fallback.go) 在只返回 100 条时仍遍历整个实体 map，
  为符合条件的实体计算分片和位置，再用堆挑选页面。
- `PinScanCursor → indexCatalogContentHash` 占累计分配约 **11.3% / 2.7 GB**。
  上轮去掉了大字符串拼接，但仍在每个游标上规范化目录、生成目录行并重新计算哈希。
  [scan_cursor.go](../internal/storage/scan_cursor.go)、[scan_catalog_cache.go](../internal/storage/scan_catalog_cache.go)。
- 建议先复用已验证、不可变内部目录的哈希；再按租户代次与版本保存有界的分页有序 ID 视图，
  用游标定位起点。必须保留恢复同版本后的失效、旧游标版本固定和公开对象可修改的边界，
  不能简单让所有 `indexCatalogContentHash` 调用都返回已有字段。

### 2. HTTP 实体重复 JSON 编码：收益范围明确

- `httpapi.writeJSON` 调用栈占 CPU 样本 **25.90%**，`Entity.MarshalJSON` 占 **12.81%**；
  后者占累计分配 **18.16%**。这些比例有调用栈重叠，不能相加。
- [entity_labels.go](../internal/graph/entity_labels.go) 的 `MarshalJSON` 内部再调用 `json.Marshal`，
  外层编码器随后检查和压缩返回的 JSON。已有 `JSONValue()` 能表示相同字段而不经过嵌套 marshaler。
- 建议把现有表示复用到实体列表和 query 响应，先保留公共 API、labels、来源字段、转义和空值语义。
  不改持久化哈希编码，不以更换整套 JSON 库作为第一步。

### 3. Parquet 打包的行组局部性：冷读与解码有空间

- [index_build_entities.go](../internal/storage/index_build_entities.go) 合并逻辑分片后按实体 ID 全局排序。
  [parquet_entity_scan.go](../internal/storage/parquet_entity_scan.go) 却按 shard 列的 min/max 筛选行组。
  哈希分片被混排后，行组的 shard 范围覆盖了大部分打包分片。
- 对种子数据 5 个 pack 的元数据和实际 shard 列检查：每文件包含 12–13 个逻辑分片、23–24 个行组，
  按当前筛选规则查询任一实际分片，**100% 行组都会保留**。
  当前版本新写入的 v67 的 5 个 pack 也检查并留存：1345 个“实际分片 × 行组”组合中保留 1344 个，
  即 **99.93%**；说明写入后的新文件仍存在这个问题。
  这是该数据集的行组筛选结果，不是全库所有查询的读放大倍数。
- 建议以 `(shard, entity ID)` 安排物理行顺序，保持每个逻辑页及对外结果顺序不变；
  验证行组过滤、随机读、分页、旧格式读取、备份恢复和目录一致性。
  先修复布局局部性，再判断是否需要改变默认 pack 大小。

### 4. WAL 索引更新的分配与写放大：继续降低内存峰值

- 后台增量索引工作占 CPU **9.70%**、累计分配 **25.71% / 6.2 GB**；
  实体页写入子路径约占分配 **14.7%**。上述比例同样存在包含关系。
- [index_incremental_pages.go](../internal/storage/index_incremental_pages.go) 即使只改少量实体，
  仍扫描 `after.Entities`，并深复制受影响分片的所有实体。增量页也未像全量构建那样传播
  `hashCanonical`，会走额外的 JSON 规范化。可先复用内部不可变实体值、准确传递规范化状态，
  再评估按分片维护实体 ID 集合，避免每批扫描整图。
- 实体 pack 写入目前串行执行；可评估复用已有有界文件任务和目录同步合并，保留文件同步与
  manifest/catalog 发布顺序。后台工作除了数量上限，还可以按预计保留字节控制并发。
- `WriterObjectCache.cachePositive` 在结束后的堆快照里保留约 **34 MB**，包含完整文件副本。
  本地 Parquet 读取已经直接使用文件句柄，可按文件类型检查该字节缓存的实际命中收益。

## 判断与边界

负载期间累计分配约 24 GB，结束后主动 GC 的采样存活堆约 102 MB；测量窗口 RSS 峰值约 403 MiB。
三者含义与采样时点不同，不能相减当作可节省内存。当前证据支持优先减少请求级重复分配和
索引重写临时对象，再调整缓存上限；不能把上轮 WAL 的 RSS 增长全部归因于缓存容量。

另一个静态发现是远端快照上传期间持有租户读视图，慢网络会延长 GC、清空或恢复的等待。
后续可将保护范围缩小到已落盘的快照文件与持久任务引用，但必须保留取消、崩溃重试和删除保护。
本次没有运行慢网络备份场景，未量化此项影响。

建议实施顺序：**目录哈希复用 → HTTP JSON 编码 → 分页有序视图 → pack 行顺序与索引写入**。
这些是已定位的成本和改造候选，百分比不是承诺可直接获得的整体加速比例。
本轮未重新运行全量测试、十万实体容量测试或长期 soak；生产代码未变化。
