# GGraphDB v2.0.0

## 中文

本地磁盘版成为主版本：单进程、多租户并发读写，面向持续更新图数据和在线查询最新状态。
S3 兼容对象存储用于快照备份与按需恢复；服务运行不依赖对象存储或 PostgreSQL。

2.0 是不兼容版本，使用全新的数据目录，不提供 1.x 迁移或跨大版本回滚。
提交结果的 `data_md5` 替换为 `data_hash`，格式为 `sha256-shards-v2:<64 位十六进制>`。
Go SDK 路径为 `github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb`，Python SDK 为 2.0.0。
HTTP `/v1` 路径保持不变。

实体、边、邻接表及字段索引改为分片写时复制；摘要只重算受影响分片。小集合保留紧凑表示，避免为每个单值索引分配分片目录。增量索引复用分片目录，
GC、文件发布和维护队列增加扫描、时间、字节与共享内存预算。目录同步、manifest 发布顺序、
同步 WAL、幂等恢复、版本固定读视图和对象备份校验继续保留。

这仍是单机单进程产品。单机容量、磁盘空间与可用性需要自行规划，S3 备份不提供实时复制或高可用。
维护预算是估算准入额度，不是进程 RSS 硬上限。单个大文件、全量重建和冷加载仍有全量成本。
性能与验证范围见包内 `docs/performance-v2.0.md`，发布门禁记录在 `release/evidence/`。
本轮 10 万实体维护负载的写入 P95 为 16.6 秒、查询 P95 为 92.3 ms，较本地基线改善；
1 万实体 WAL 混合查询及部分导出、冷读结果仍有退化，整体性能目标未获认证。
原始数据和复现步骤另附 `graphdb-v2.0.0-performance.tar.gz`，不据此承诺所有场景提速。

## English

Local disk is now the main, stable edition. One process owns the data directory
and serves concurrent tenants. S3-compatible storage provides snapshot backup
and on-demand restore. There is no PostgreSQL or object-storage dependency for
online reads and writes.

Start with a fresh 2.0 data directory. There is no 1.x migration or cross-major
rollback. Commit responses replace `data_md5` with `data_hash`, using
`sha256-shards-v2:<64 hex digits>`. The Go module is
`github.com/SamuelSupe/graphdb/v2`; both SDKs are version 2.0.0.

Updates copy changed graph and field-index buckets and recompute changed digest buckets.
Small sets retain a compact representation instead of allocating a shard directory.
Incremental indexes reuse partition membership; maintenance uses bounded scan,
file publication and shared queued/active memory admission. Durability and
recovery ordering remain enforced. This release does not provide replication
or high availability; maintenance estimates are not hard RSS limits.

The 100K maintenance workload improved, but 10K WAL mixed queries and several
export/cold-read observations still regressed. Overall performance targets remain
unqualified. See `docs/performance-v2.0.md` and the separately checksummed
performance evidence archive for results, limitations and reproduction steps.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.0.0.tar.gz.sha256
tar -xzf graphdb-v2.0.0.tar.gz
cd v2.0.0
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/new-v2-data bin/graphdb-linux-amd64 serve
```

The archive includes Linux amd64/arm64 and macOS arm64 binaries, build metadata,
source, SDKs, bilingual documentation, deployment examples and release evidence.
