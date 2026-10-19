# GGraphDB v2.3.5

## 中文

本版发布本地维护窗口和 Raft 文件系统路径的优化。确切标签门禁及下载核验尚待完成；
全部门禁通过后由既有流水线发布，无门禁豁免。最终结果见
[2.3.5 验证](https://github.com/SamuelSupe/graphdb/blob/main/docs/validation-v2.3.5.md)。

- 本地重建在短暂前台锁内捕获不可变图，进入有序索引队列；并发 WAL 推进图头时，
  不再因旧版本发布检查触发 `manifest ... changed while rebuilding indexes`。
  保留 writer fence、读取视图和 catalog CAS。
- compact／重建期间合并有界索引追平需求，仅保存首次变更前的标量值和边端点，
  不排队保留中间图。最多 8192 个变更引用，元数据预算不超过
  `min(维护预算 / 64, 8 MiB)`；超过预算、schema 变化或基准不匹配时回退全量构建。
  获得原有内存预算后才捕获最新图，WAL 确认可继续推进；显式 commit 仍等待自身索引。
- WAL 调度在过期派发前有界排空已缓冲请求；compact 在原有共享编码限额内准备
  分片快照和兼容全量记录，两者持久化后才发布。减少冗余图复制和重复分区扫描。
- Raft 维护只在同一次应用屏障内复用祖先检查，有界并行同步独立目录并等待全部完成
  后写入提交标记。流式快照仅为实际文件创建父目录，减少 GC 空目录的重复工作。
  保留 before-image 回滚、持久化屏障和 Raft 强一致读取约束。

相同数据和逻辑计划的候选 C3 → C7 对照：

| 指标 | 优化前 | 优化后 |
| --- | --- | --- |
| 单机 6 分钟写入 P99 | 9.470 秒 | 3.948 秒 |
| 单机 stale 采样 | 131/327 | 134/327 |
| 原始 30 分钟增长计划下 Raft 查询最大值 | 13.132 秒 | 5.584 秒 |
| Raft 接入最大值，含 429 重试 | 50.137 秒 | 27.789 秒 |

**stale 频率未改善，秒级长尾仍在。** 单机 stale 样本均在更新中，静默后追平；
并发重建版本冲突有独立回归验证。单机容器内存峰值增加 6.4%，Raft 基线资源计数缺失，
不比较其 CPU/RSS。上述是工作区候选之间的优化对照，包含相同私有 pprof 初始化，
不是已发布 v2.3.4 与 v2.3.5 二进制对照。两种部署各自保持输入一致，不能直接跨部署比较。
详见[完整对照与边界](https://github.com/SamuelSupe/graphdb/blob/main/docs/performance-tail-validation-2026-10-20.zh-CN.md)。

发布要求确切标签全量 unit/vet/race、SDK、控制台边界、direct/WAL、S3、TLS 网关、
Raft 协议 1–3、已发布 v2.3.0 到本版的协议 1 滚动，以及单机和 Raft 各 30 分钟维护长测。
证据随发行包提供。此前候选的 1827 项 Go 测试／子测试通过、7 项跳过，vet、聚焦 race、
单机索引待追平时 SIGKILL 恢复及六份 Raft 磁盘冷读通过；候选检查与确切标签门禁分开记录。

产品、SDK 和 OpenAPI 为 2.3.5，Go 模块 `/v2`、HTTP `/v1`、Parquet/WAL 格式及
Raft 协议编号不变。单机先备份、停止旧进程，再沿用原目录和前缀；Raft 按
[滚动升级手册](https://github.com/SamuelSupe/graphdb/blob/main/docs/raft-rolling-upgrade.zh-CN.md)操作，
协议激活仍是全组升级后的独立步骤。跨宿主机故障域、日级稳定性、生产容量及统一延迟 SLO 未验收。

## English

This patch orders local maintenance rebuilds through the existing index queue,
allows WAL publication while bounded catch-up waits for existing build capacity,
and fixes concurrent rebuild publication conflicts. It retains writer fences,
read views, catalog CAS and full-rebuild fallbacks. Compact prepares its two durable
representations within the shared encoder limit. WAL dispatch drains buffered
admissions; graph encoding avoids redundant copies and partition scans.

Replicated maintenance reuses ancestry checks only within its application barrier,
syncs independent directories with bounded concurrency before the commit marker,
and creates snapshot parents only for retained files. Rollback and durability
barriers remain enforced.

Matched C3/C7 candidate replays reduced standalone write P99 from 9.470 to 3.948
seconds and the thirty-minute Raft growth-plan query maximum from 13.132 to 5.584
seconds. Standalone stale frequency did not improve (131/327 to 134/327), container
memory peak rose 6.4%, and Raft baseline resource counters are unavailable.
Seconds-long tails remain. These measured workspace candidates share private pprof
instrumentation; they are not published v2.3.4/v2.3.5 binary measurements. Each
deployment has its own matched inputs; cross-topology comparisons are invalid.

Publication requires all exact-tag gates, including both thirty-minute soaks and
released-v2.3.0 rolling qualification, without a waiver. Tag-source records are
pending plans; the linked validation record will retain final downloaded-asset
checks. Module `/v2`, HTTP `/v1`, persisted formats and protocol numbering are
unchanged. Cross-host reliability, day-scale stability, production capacity and a
general latency SLO remain unqualified.

## Download and verify / 下载与校验

```sh
sha256sum -c graphdb-v2.3.5.tar.gz.sha256
tar -xzf graphdb-v2.3.5.tar.gz
cd v2.3.5
sha256sum -c SHA256SUMS
bin/graphdb-linux-amd64 version
GRAPHDB_DATA_DIR=/path/to/v2-data bin/graphdb-linux-amd64 serve
```

Includes Linux amd64/arm64 and macOS arm64 binaries, matching SDKs/OpenAPI,
console sources, bilingual guides, deployment/backup examples, build identity and
gate evidence. No container image publication is implied.
