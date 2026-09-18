# GGraphDB v1.3.4-local.1 — 本地磁盘版 / Local disk edition

这是 `codex/local-disk-v2` 的独立预发布版本，基于 `ffa85414`。
原 `main` 分支、稳定版和 Latest 设置保持不变。

## 本版内容

- 单机、单进程、多租户并发读写；数据目录进程独占，默认 direct 写入，WAL 默认同步落盘。
- 保留图模型、查询、HTTP/GraphQL、Go/Python SDK、导入导出、来源治理、幂等、索引、任务和租户生命周期。
- 本地 Parquet 随机读取、有界缓存和批量文件发布；活跃读视图保护 GC、删除和恢复。
- 可选 S3 兼容快照备份，可从空目录发现、校验并按需恢复远端快照。
- 包含本地磁盘改造后的正确性修复及十轮重点性能优化；实测结果和局限保留在 `docs/performance-local-disk-*.md`。

## 下载与运行

下载 `graphdb-v1.3.4-local.1.tar.gz` 和对应 `.sha256`，先校验压缩包，再校验包内 `SHA256SUMS`。
包内提供 Linux amd64、Linux arm64 和 macOS arm64 二进制，以及文档、SDK、部署示例和发布验证证据。
例如 Linux arm64：

```sh
sha256sum -c graphdb-v1.3.4-local.1.tar.gz.sha256
tar -xzf graphdb-v1.3.4-local.1.tar.gz
cd v1.3.4-local.1
sha256sum -c SHA256SUMS
bin/graphdb-linux-arm64 version
GRAPHDB_DATA_DIR=./data bin/graphdb-linux-arm64 serve
```

macOS 使用 `shasum -a 256 -c` 校验并运行 `bin/graphdb-darwin-arm64`。
Go/Python SDK 包版本为 `1.3.4+local.1`，Python 使用符合 PEP 440 的本地版本号。
需要从源码运行验证脚本或构建容器时，检出此 Release 的 Git 标签。

## 兼容和验证边界

- 保留现有本地 Parquet/WAL 格式；仍支持 `expected_version`、`min_version`、分页游标和 WAL 状态查询契约。
- 不支持远端在线主存储、PostgreSQL 协调、独立 reader/writer、多实例和共享网络文件系统；带 PostgreSQL 协调标记的数据拒绝直接接管。本版不提供远端主存储迁移。
- 发布流水线要求完整单元测试、vet、race、旧版本数据兼容、SDK、真实 HTTP direct/WAL 恢复和重启、MinIO 快照恢复，以及带 compact/GC/索引重建的 30 分钟持续负载通过，才上传发行包。
- 性能采用有限次重点对比，没有完整执行三方、多规模、多次轮换验收，因此不宣称整体性能达标或所有路径更快。第十轮深度索引校验的单组基准约快 4.4%，分配少约 160 MB；实际恢复耗时没有改善。详见 `docs/performance-local-disk-optimization-10.md`。

## English summary

An independent prerelease on `codex/local-disk-v2`, based on `ffa85414`. The default
`main` branch and stable Latest release are preserved. This edition runs concurrent
tenant workloads in one process on an exclusively locked local data directory,
with optional S3-compatible snapshot backup and on-demand restore.

The archive includes Linux amd64/arm64 and macOS arm64 binaries, checksums, exact
build metadata, SDKs, documentation, deployment examples, and release-gate evidence.
Local formats and API contracts are retained; remote primary storage, PostgreSQL
coordination and multi-process deployments are unsupported. Performance reports
describe focused samples and limitations, not a general speedup guarantee.
