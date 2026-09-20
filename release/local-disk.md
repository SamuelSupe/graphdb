# GGraphDB v1.3.4-local.4 — 本地磁盘版 / Local disk edition

这是 `codex/local-disk-v2` 的独立预发布版本，基于 `ffa85414`。
原 `main` 分支、稳定版和 Latest 设置保持不变。

## 本版变化

在 `v1.3.4-local.1` 的本地存储和对象快照备份功能上：

- 死信内部扫描只枚举一次目录，保留游标、提前停止和新鲜读取。
- 深度索引检查一次分组图数据，共用当前/历史分片哈希，保留内容校验。
- 删除不可达 PostgreSQL 协调实现，生产 Go 代码净减少 7,150 行；保留本地锁、写入围栏、WAL 恢复和持久化格式。
- 本地 GC 按最多 64 次删除分批，批次间释放锁并重新读取当前引用；索引孤儿清理支持预算、游标和 dry-run。
- 修复持续负载暴露的 GC 最后一页重复扫描、索引清理占住租户维护名额、存活 GC 任务被误判过期的问题。失败候选 `v1.3.4-local.2` 保留标签用于追溯，没有发布发行包。
- 补齐用量采样在持续负载正常结束时的取消分类；候选 `v1.3.4-local.3` 因报告工具误判而未发布，原始业务操作错误数为 0。
- 发行包补齐容器构建源码，发布前从解压后的包构建并启动容器。

单组预热基准中，1 万实体的深度索引检查从 1.65 秒降至 1.49 秒，分配次数减少 5.08%。
这是有限样本，不代表整体吞吐或冷读性能。详见 `docs/performance-local-disk-code-simplification.md`。

## 下载与运行

下载 `graphdb-v1.3.4-local.4.tar.gz` 和对应 `.sha256`，先校验压缩包，再校验包内 `SHA256SUMS`。
包内提供 Linux amd64、Linux arm64 和 macOS arm64 二进制，以及文档、SDK、部署示例和发布验证证据。
例如 Linux arm64：

```sh
sha256sum -c graphdb-v1.3.4-local.4.tar.gz.sha256
tar -xzf graphdb-v1.3.4-local.4.tar.gz
cd v1.3.4-local.4
sha256sum -c SHA256SUMS
bin/graphdb-linux-arm64 version
GRAPHDB_DATA_DIR=./data bin/graphdb-linux-arm64 serve
```

macOS 使用 `shasum -a 256 -c` 校验并运行 `bin/graphdb-darwin-arm64`。
Go/Python SDK 包版本为 `1.3.4+local.4`，Python 使用符合 PEP 440 的本地版本号。
包内包含容器构建所需源码，可运行 `docker compose up -d --build`。需要执行依赖 Git 历史的兼容性验证时，检出此 Release 的 Git 标签。

## 兼容和验证边界

- 保留现有本地 Parquet/WAL 格式；仍支持 `expected_version`、`min_version`、分页游标和 WAL 状态查询契约。
- 不支持远端在线主存储、PostgreSQL 协调、独立 reader/writer、多实例和共享网络文件系统；带 PostgreSQL 协调标记的数据拒绝直接接管。本版不提供远端主存储迁移。
- 发布流水线要求完整单元测试、vet、race、旧版本数据兼容、SDK、真实 HTTP direct/WAL 恢复和重启、MinIO 快照恢复，以及带 compact/GC/索引重建的 30 分钟持续负载通过，才上传发行包。
- 性能采用有限次重点对比，没有完整执行三方、多规模、多次轮换验收，因此不宣称整体性能达标或所有路径更快。本次定向基准及验证边界见 `docs/performance-local-disk-code-simplification.md`。

## English summary

An independent prerelease on `codex/local-disk-v2`, based on `ffa85414`. The default
`main` branch and stable Latest release are preserved. This edition runs concurrent
tenant workloads in one process on an exclusively locked local data directory,
with optional S3-compatible snapshot backup and on-demand restore.

The archive includes Linux amd64/arm64 and macOS arm64 binaries, checksums, exact
build metadata, SDKs, documentation, container-build source, deployment examples,
and release-gate evidence. This update removes unreachable coordination code,
batches GC with reference revalidation, and reduces repeated directory scans and
deep-index validation work.
Local formats and API contracts are retained; remote primary storage, PostgreSQL
coordination and multi-process deployments are unsupported. Performance reports
describe focused samples and limitations, not a general speedup guarantee.
