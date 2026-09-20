# GGraphDB v1.3.4-local.8 — 本地磁盘版 / Local disk edition

这是 `codex/local-disk-v2` 的独立预发布版本，基于 `ffa85414`。
原 `main` 分支、稳定版和 Latest 设置保持不变。

## 本版变化

在 `v1.3.4-local.1` 的本地存储和对象快照备份功能上：

- 死信内部扫描只枚举一次目录，保留游标、提前停止和新鲜读取。
- 深度索引检查一次分组图数据，共用当前/历史分片哈希，保留内容校验。
- 删除不可达 PostgreSQL 协调实现，简化候选的生产 Go 代码净减少 7,150 行；保留本地锁、写入围栏、WAL 恢复和持久化格式。
- 本地 GC 每批最多删除 256 个文件，重新读取当前引用，并在批次之间让排队的查询和 compact 执行；每批重新申请全局执行名额，保留并发上限。
- 同批删除共享目录同步，失败或取消时也在释放维护锁前同步；删除总预算、游标和 dry-run 保持有效。
- 修复 WAL 批次内重复实体和新索引分片触发不必要全量重建的问题。
- 固定 GC 候选集合，避免持续写入延长最后一页；存活任务不会仅因持久化心跳延迟而被误判过期。
- 修复持续负载工具的正常结束取消记录，保留独立请求超时和真实维护错误。候选验证过程及失败记录详见性能报告。
- 发行包补齐容器构建源码，发布前从解压后的包构建并启动容器。

单组预热基准中，1 万实体的深度索引检查从 1.65 秒降至 1.49 秒，分配次数减少 5.08%。
这是有限样本，不代表整体吞吐或冷读性能。详见 `docs/performance-local-disk-code-simplification.md`。

## 下载与运行

下载 `graphdb-v1.3.4-local.8.tar.gz` 和对应 `.sha256`，先校验压缩包，再校验包内 `SHA256SUMS`。
包内提供 Linux amd64、Linux arm64 和 macOS arm64 二进制，以及文档、SDK、部署示例和发布验证证据。
例如 Linux arm64：

```sh
sha256sum -c graphdb-v1.3.4-local.8.tar.gz.sha256
tar -xzf graphdb-v1.3.4-local.8.tar.gz
cd v1.3.4-local.8
sha256sum -c SHA256SUMS
bin/graphdb-linux-arm64 version
GRAPHDB_DATA_DIR=./data bin/graphdb-linux-arm64 serve
```

macOS 使用 `shasum -a 256 -c` 校验并运行 `bin/graphdb-darwin-arm64`。
Go/Python SDK 包版本为 `1.3.4+local.8`，Python 使用符合 PEP 440 的本地版本号。
包内包含容器构建所需源码，可运行 `docker compose up -d --build`。需要执行依赖 Git 历史的兼容性验证时，检出此 Release 的 Git 标签。

## 兼容和验证边界

- 保留现有本地 Parquet/WAL 格式；仍支持 `expected_version`、`min_version`、分页游标和 WAL 状态查询契约。
- 不支持远端在线主存储、PostgreSQL 协调、独立 reader/writer、多实例和共享网络文件系统；带 PostgreSQL 协调标记的数据拒绝直接接管。本版不提供远端主存储迁移。
- 发布流水线要求完整单元测试、vet、race、旧版本数据兼容、SDK、真实 HTTP direct/WAL 恢复和重启、MinIO 快照恢复，以及带 compact/GC/索引重建的 30 分钟持续负载通过，才上传发行包。
- 2 CPU / 7 GiB、500 ms 写入间隔的额外高频诊断中，读写无错误，但手动 GC 用时 5 分 54 秒，超过工具的 5 分钟等待期限。该加压轮次未通过，详见性能报告；正式门禁使用原来的 5 秒写入间隔。
- 性能采用有限次重点对比，没有完整执行三方、多规模、多次轮换验收，因此不宣称整体性能达标或所有路径更快。本次定向基准及验证边界见 `docs/performance-local-disk-code-simplification.md`。

## English summary

An independent prerelease on `codex/local-disk-v2`, based on `ffa85414`. The default
`main` branch and stable Latest release are preserved. This edition runs concurrent
tenant workloads in one process on an exclusively locked local data directory,
with optional S3-compatible snapshot backup and on-demand restore.

The archive includes Linux amd64/arm64 and macOS arm64 binaries, checksums, exact
build metadata, SDKs, documentation, container-build source, deployment examples,
and release-gate evidence. This update removes unreachable coordination code,
batches all local GC with reference revalidation and a turn for queued readers, and reduces repeated directory scans and
deep-index validation work.
Local formats and API contracts are retained; remote primary storage, PostgreSQL
coordination and multi-process deployments are unsupported. Performance reports
describe focused samples and limitations, not a general speedup guarantee.
