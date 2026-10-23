# GGraphDB v2.4.1

## 中文

2.4.1 是 2.4.0 的稳定性与成本修复版，发行二进制统一使用 **Go 1.27.3** 构建。

- 修复复制 Apply 把本机超时等非确定性失败当作已完成业务拒绝的问题。已提交复制命令不再使用单机写超时；未知失败保留 fail-stop 与回滚保护，不能通过跳过日志继续运行。确定性校验失败和配额拒绝仍保留原有语义。
- 不可变图发布与读取并行；替换或回收文件前撤销活跃读取并打断阻塞的 socket 写。流式响应使用绝对总写期限。WAL 发布按租户公平推进，达到批量阈值立即唤醒，一个受阻租户不再让其他租户等待下一个周期。
- Raft 快照按原始状态字节预算校验，CLI 默认启用按远端能力协商的流式快照，旧端或显式关闭仍可回退内联模式。统一单机/HA 的 32MiB 写请求上限，并为两层编码预留有界 proposal 空间。已提交但丢失响应的写入明确返回结果未知、不可盲重试；应查询状态或复用相同幂等身份。
- sync WAL 每组使用段同步和双槽持久边界同步，避免每次新建、重命名边界文件。checkpoint 按完成字节摊销、合并并发压力触发并逐请求流式编码；Prepared 计划计入队列压力，关停保留重试退避。checkpoint 仍扫描 WAL 并占用写协程，不承诺无暂停或进程 RSS 硬上限。
- 死信在预提交失败时保持 pending，过期认领继续复用原批次身份；手工本地备份不再被任务 TTL 破坏。GC 在独立阶段继续清理并保留损坏错误，历史邻接目录同轮只标记一次；孤儿快照续跑使用独立阶段游标，保护保留、当前、未来版本及读视图。
- 修复 fence 覆盖回滚、读 singleflight、刷新预留、取消 memo、panic 清理、旧 256 桶边游标与缓存租户校验。被裁剪边页只缓存有界 footer。查询规划维护标量覆盖与 kind 索引，冷排序移出全局锁并限制邻居缓存；路径字段排序/聚合按终点取值，默认 identity 顺序和旧游标保留。
- 路由管理操作校验原调用方凭证；未知写结果不自动重放；指标标签和 leader 探测有界。补充损坏 Parquet ordinal、磁盘容量、字节/期限溢出、chunked S3 下载与任务准入的保护。

### 升级与降级

**WAL 边界格式发生迁移。** 新程序启动时可读旧36字节 v1 `.durable`，迁移为双槽 v2。旧程序不能直接读取新边界。降级前先排空 WAL，并保留升级前已验证备份；备份须包含整个 WAL 目录，不能只复制或替换某个 marker。必要时用新程序完成重放或恢复到新目录，不承诺整个 WAL/Raft 目录直接降级。

HTTP `/v1`、Go 模块 `/v2`、逻辑 `data_hash`、Raft 协议编号及 `ListEdges` 的全局顺序/旧游标契约保留。范围页与邻接页仍默认关闭；先升级全部读写/GC节点和副本，再显式开启并重建。索引降级须先用新程序重建旧布局。数字/文本比较、缺失字段及 `timeout_ms=0` 的既定查询语义保留。

[2.4.1 验证记录](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.4.1.md) · [升级手册](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md) · [本地持久化边界](https://github.com/SamuelSupe/graphdb/blob/main/docs/local-disk.zh-CN.md)

原门禁保持：完整 unit/vet/race/SDK、direct/WAL/恢复、S3/网关、真实 v2.3.0 滚动、增强/协议3、单机及 Raft 各30分钟长测、构建解包、校验和与独立下载后新目录恢复。只有实际通过才记录 PASS，已有标签与资产不变。不作生产容量、跨宿主机或天级稳定性承诺。

## English

2.4.1 is a reliability and cost repair for 2.4.0, built with **Go 1.27.3**.

- Replicated Apply no longer turns local deadlines or unknown server failures into durable business rejections. Immutable publication runs beside reads; replacement/reclamation revokes readers and blocked socket writes. Tenant-fair WAL flushing drains eligible batches without starving healthy tenants.
- Raw snapshot limits include separate encoded transport bounds. Streaming snapshots are enabled by default with capability negotiation and legacy fallback. Standalone and HA accept the same32MiB request contract. Lost results after proposal submission are explicitly unknown and must be resolved by identity before repeating mutations.
- Preallocated checksummed two-slot WAL boundaries reduce sync/metadata work. Byte-amortized, generation-coalesced checkpoints stream pending requests; checkpoint construction still scans WAL and serializes appends. Queue accounting includes durable prepared plans; it is not a process RSS guarantee.
- Dead-letter claims reuse durable replay identities and retain precommit failures. Local backup results survive task TTL. GC retains independent error diagnostics, caches historical adjacency marks per run and uses a separate orphan-scan continuation phase that cannot resume whole-version deletion.
- Fence overwrite rollback, cache publication/cancellation, legacy edge cursors, query planning/ordering, bounded labels/discovery and malformed persisted/input boundaries have focused regressions. Numeric/text comparison, missing-field behavior and zero query timeout keep their documented semantics.

**WAL downgrade boundary:** opening an old v1 durable marker migrates it to the new two-slot v2 format. Old binaries cannot read the new marker. Drain WAL before downgrade, keep verified pre-upgrade backups and preserve the whole WAL directory. This does not qualify a whole WAL/Raft directory downgrade. Index flags remain off by default; upgrade all readers/writers/GC/replicas before explicit activation and rebuild. Global `ListEdges` ordering/cursors, logical hashes, HTTP/module namespaces and separate Raft protocols remain compatible.

All original gates, including both thirty-minute soaks, remain required. [Validation](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.4.1.md) separates candidate qualification, exact-tag checks, publication and independently downloaded assets. Existing tags and assets remain immutable.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.4.1.tar.gz.sha256
tar -xzf graphdb-v2.4.1.tar.gz
cd v2.4.1
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
```
