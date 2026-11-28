# GGraphDB v2.7.0

从 2.6.5 升级到 2.7.0，获得架构收敛及全链路稳定性、持久化、并发安全与性能加固。

- **存储、WAL 与 Parquet 持久化加固**：重构并收敛底层存储后端、Catalog、Lease、Task、Backup 与 WAL 子系统；引入分片写时复制（Sharded COW）邻接与分区映射降低写放大和锁争用；加固 manifest 保护、稀疏 Parquet 写入、行列裁剪扫描、回执过期索引、本地/对象缓存并发一致性以及 S3 备份与恢复容错。
- **Raft 高可用、分片与协议升级**：新增 Raft ReadIndex 线性一致读、内联快照传输与压缩后的 Raft 日志尾保留索引；支持 Raft 协议 1 至 14（默认仍为协议 1，协议 13 涵盖保留检查点、采集队列检查点、快照载荷摘要、JSON 载荷帧、大图转换取消/回滚与托管 WAL 回执迁移）；修复并发维护、切主、分区 2PC 与迁移恢复路径上的锁序、数据竞争与恢复边界问题。
- **查询引擎、HTTP API、控制台与 SDK**：新增查询 IR 能力校验、物化最短路径执行、优化的来源扫描与差分模糊测试；强化查询与流式响应的取消/资源预算、HTTP 准入与错误码契约、AdminUI 边界校验，以及 Go/Python SDK 的连接池、错误码同步、重试和流完整性检查。

## 升级

单机 2.x 升级前请先保留完整的运行状态备份（包含数据目录与 WAL 目录），停止旧进程后替换为 2.7.0 并沿用原 `GRAPHDB_DATA_DIR` 与 prefix。替换各 Raft 节点时沿用已配置的协议，按组滚动替换并在副本追平后继续；需要启用新协议时使用独立维护窗口。使用流式 SDK 的调用方应读取到流结束并检查错误。[滚动升级](https://github.com/SamuelSupe/graphdb/blob/v2.7.0/docs/raft-rolling-upgrade.zh-CN.md) · [SDK 文档](https://github.com/SamuelSupe/graphdb/blob/v2.7.0/docs/user/sdk.zh-CN.md) · [发布验证](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.0)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.7.0-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.7.0-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.7.0.tar.gz` |

将归档与对应的 `.sha256` 放在同一目录。以下命令校验 Linux AMD64 归档并检查版本。

```sh
archive=graphdb-v2.7.0-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.7.0-linux-amd64/graphdb version
```

完整版包含源码、SDK、部署示例和发布门禁证据。`BUILD-METADATA.json` 记录版本和构建提交。

## 本次发布验证范围

本次发布已在本地完成工作区卫生检查、发布冻结契约校验、`go vet -mod=readonly ./...`、全仓库 18 个 Go 包的 `-race -count=1` 单元测试与多轮回归测试、管理控制台边界测试、Python SDK 测试及自动化脚本测试。推送 `v2.7.0` 标签后，GitHub Actions 发布流水线将自动执行静态与集成门禁、对象备份门禁、网关门禁、30 分钟本地磁盘混合负载长测、Raft 门禁，并在原生 Linux AMD64/ARM64 环境验证发行包后自动发布资产。[查看发布流水线](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.0)
