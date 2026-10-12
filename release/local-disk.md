# GGraphDB v2.3.1

## 中文

本版已发布为稳定 Latest，完整标签门禁及独立下载资产校验、运行验证全部通过，无门禁豁免。
正式单机三十分钟长测原始错误为 0，但写入 P99 22.028 秒、最大 36.052 秒，
索引 24/59 次 stale，最后采样 stale。Raft 原始错误为 0、59 次索引样本均 ready，
但主要查询 P99 3.509–4.003 秒，单项索引查询最大 9.642 秒。秒级长尾、stale 和
极端查询延迟仍存在；本版不提供统一低延迟 SLO 或生产容量认证。


本版修复并缓解单机写入长尾、索引追平和 Raft 维护期间的延迟：合并同一写入栅栏内
排队的同步索引更新；为控制元数据和发布装载保留 Parquet 解码容量；减少文件打开、
图重放、快照解码、稠密分片扫描和准备维护中的重复工作。恢复回滚仍在修改前持久化
全部前镜像。保持 fsync、多数派确认、逻辑摘要、Parquet/WAL 和 Raft 协议契约。

同一部署分别从相同停止状态数据、资源和请求计划做一次 6 分钟前后对照；每组均为
4 writer / 16 reader、288 次写入和 5,760 次查询。单机批量 20、Raft 批量 1，
两种部署的结果不能直接比较：

| 指标 | 单机：基线 → 修复 | Raft：基线 → 修复 |
| --- | --- | --- |
| 写入 P99 | 12.123 → 9.658 秒 | 1.885 → 0.908 秒 |
| 查询 P99 | 104–116 → 66–81 毫秒 | 323–329 → 101–107 毫秒 |
| 索引 stale 样本 | 7/36 → 3/36 | 0/36 → 0/36 |
| 写入／查询错误 | 0 → 0 | 0 → 0 |

冷启动相同 4 个写入由全部 120 秒超时变为全部成功，耗时 13.425–13.431 秒；
480 次查询的 5 秒超时由 318 次降到 22 次。单机瞬时 stale 和冷查询超时仍存在。
旧约 6 秒 Raft 极端查询没有在固定基线中复现，不能声称已完全消除。
详见[固定负载报告](https://github.com/SamuelSupe/graphdb/blob/main/docs/maintenance-tailfix-2026-10-13.zh-CN.md)。
这些对照使用修复后的诊断构建；确切发行二进制由标签门禁另行验证。

发行流程要求全量 unit/vet/race、SDK、direct/WAL、S3、TLS 网关、Raft 协议 1–3、
已发布 v2.3.0 到本版的协议 1 滚动，以及单机和 Raft 各 30 分钟维护长测全部通过后
才创建稳定发行，无门禁豁免。发行证据随包提供，最终状态见
[2.3.1 验证](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.1.md)。
单宿主机测试不认证跨宿主机故障域、日级稳定性、生产容量或统一低延迟 SLO。

产品、SDK 和 OpenAPI 为 2.3.1，Go 模块 `/v2` 和 HTTP `/v1` 不变。
单机先备份、停止旧进程，再以原目录和前缀启动；Raft 遵循
[滚动升级手册](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md)。
协议激活仍是全组升级后的独立操作，不提供任意版本混部或降级保证。

## English

Published as stable Latest after all exact-tag gates and independent downloaded-asset
checksum/runtime checks passed, without a waiver. The standalone thirty-minute soak
had zero raw errors, but write P99 was 22.028 seconds (maximum 36.052) and 24/59 index
samples were stale, including the last sample. Raft had zero raw errors and 59 ready
index samples, but main-query P99 was 3.509–4.003 seconds and one indexed-query maximum
was 9.642 seconds. Seconds-long tails, staleness and extreme query delays remain;
this patch does not certify a general latency SLO or production capacity.


This patch reduces standalone write tails, index catch-up work and Raft maintenance
latency. It coalesces queued synchronous index updates, reserves decode capacity
for control/publication loads, reuses verified immutable graphs, improves dense
Parquet row groups and removes redundant snapshot/index preparation. Rollback
before-images remain durable before mutation; fsync, quorum, logical hashes,
Parquet/WAL formats and Raft protocol contracts are preserved.

Separate fixed-data six-minute pairs used identical resources and request plans
within each topology: four writers, sixteen readers, 288 writes and 5,760 queries.
Standalone batches contain twenty items; Raft batches contain one, so the two
topologies cannot be compared directly. Standalone write P99 fell from 12.123 to
9.658 seconds and stale samples from 7/36 to 3/36. Raft write P99 fell from 1.885
to 0.908 seconds, with zero stale samples. Both final warm runs had zero write or
query errors. These comparisons qualify the diagnostic build, not a differently
stamped release binary.

Cold writes changed from four 120-second timeouts to four successes in about
13.43 seconds; five-second query timeouts fell from 318/480 to 22/480. Transient
standalone staleness and cold-query failures remain. The older six-second Raft
extreme was not reproduced in the fixed baseline. One shared host and one pair
do not establish statistical confidence, cross-host reliability or production SLOs.

The tag workflow publishes only after all unit/vet/race, SDK, direct/WAL, S3,
TLS gateway, Raft protocol, released-v2.3.0 rolling and both thirty-minute soak
gates pass. Evidence is packaged under `release/evidence/`; the validation record
tracks the exact release binary and downloaded assets. SDK/OpenAPI versions are
2.3.1; module `/v2` and HTTP `/v1` retain their existing contracts.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.1.tar.gz.sha256
tar -xzf graphdb-v2.3.1.tar.gz
cd v2.3.1
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, matching SDKs/OpenAPI,
console sources, bilingual guides, deployment/backup examples, build identity and
gate evidence. No container image publication is implied.
