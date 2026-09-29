# GGraphDB v2.1.2

## 中文

本版减少后台 GC 引起的写入长尾：仍被活跃查询保护的孤儿索引文件先登记延迟回收，
不在租户锁内反复解码；可回收时仍校验文件内容和租户，删除前再次检查引用保护。
保留退休代次直到文件实际变更，避免新读者反复推迟已经符合回收条件的文件。

固定的 4 写入、16 查询客户端加后台维护负载中，写入 P95 从 11.77–12.34 秒降至
8.08 秒，P99 从 12.79–15.56 秒降至 8.70 秒。样本仅有 80 次写入，P99 等于最大值；
维护完成数量和重叠不同，压实耗时出现退化信号，尚未稳定归因。
**不宣称整体性能达标或生产环境固定降幅，秒级写入长尾仍然存在。**
完整方法与边界见[性能报告](https://github.com/SamuelSupe/graphdb/blob/v2.1.2/docs/performance-write-tail.md)。

兼容 2.0/2.1 本地数据目录；停止旧进程后沿用原目录和 prefix。
磁盘格式、HTTP `/v1`、Go 模块 `/v2`、同步持久化默认值和 S3 备份机制保持不变。
SDK 版本同步为 2.1.2。升级步骤见[部署指南](https://github.com/SamuelSupe/graphdb/blob/v2.1.2/docs/user/release-deployment.zh-CN.md)。

完整单元/vet/race、SDK、HTTP direct/WAL、重启恢复和 S3 备份恢复检查已通过。
按本次发布要求，30 分钟混合负载提前停止，不计为通过；归档和二进制仍执行校验。
验证范围见包内 `docs/validation-v2.1.2.md`，原始 CI 记录在 `release/evidence/`。

## English

This patch reduces GC-induced write stalls by deferring orphan index validation
while active read views prevent reclamation. Eligible files still undergo content
and tenant validation, followed by a deletion-time protection check. Keeping the
retirement epoch until the file changes prevents newer readers from repeatedly
postponing an otherwise eligible orphan.

In a focused four-writer/sixteen-reader workload with background maintenance,
write P95 fell from 11.77–12.34 s to 8.08 s and P99 from 12.79–15.56 s to 8.70 s.
There were only 80 writes, so P99 equals the maximum. Maintenance completion and
overlap differed, and compaction showed an unresolved regression signal.
**This is not an overall performance qualification or a production latency
guarantee; second-scale write tails remain.** See the
[measurement report](https://github.com/SamuelSupe/graphdb/blob/v2.1.2/docs/performance-write-tail.md).

Existing 2.0/2.1 directories remain compatible after stopping the old process.
Data formats, HTTP `/v1`, Go module `/v2`, synchronous durability defaults and
S3 backups are unchanged. Both SDKs are version 2.1.2. See the
[upgrade guide](https://github.com/SamuelSupe/graphdb/blob/v2.1.2/docs/user/release-deployment.md).

Unit/vet/race, SDK, direct/WAL HTTP and restart checks, and S3 backup/restore
passed. The 30-minute mixed workload was stopped by explicit release decision
and is not counted as passed. Archive and binary verification remain required. Scope and limits are in `docs/validation-v2.1.2.md`; raw CI gate
evidence is included under `release/evidence/`.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.1.2.tar.gz.sha256
tar -xzf graphdb-v2.1.2.tar.gz
cd v2.1.2
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, source, SDKs, OpenAPI,
bilingual documentation, deployment examples, build metadata and gate evidence.
