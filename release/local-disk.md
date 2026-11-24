# GGraphDB v2.6.5

从上一正式版 2.6.2 升级到 2.6.5，可获得以下 GC、流式查询和 Raft 恢复修复。

- 本地磁盘 GC 扫描索引时，若并发写入更新了索引或 manifest，当前文件留待后续 GC 重新检查。本轮扫描从该文件之后继续，避免因游标重复而导致维护任务失败。
- Prometheus 指标 `graphdb_http_interrupted_streams_total` 补全 HELP 说明，保留原有计数语义。
- Go 和 Python SDK 校验 NDJSON 完成标记并报告流中的终止错误。服务同时记录中断流的 HTTP 指标和访问日志。
- 快照写入保留已持久化的最高 Raft 协议。首次升级读取旧快照目录时，根据仍保留的日志修正协议下限。
- Router 将请求取消与 Catalog 故障分开处理，业务版本冲突保留租户路由。写入背压使用租户的 CAS 冲突窗口。
- 对象缓存阻止并发旧读覆盖新值，超预算对象不会挤掉其他热键。普通 WAL 刷新一次按租户归组，独占文件存储的 HA 维护调度复用待执行任务索引。单机 WAL 恢复复用已有检查点。
- 审计改进涵盖输入规范化与图校验、查询资源限制、读缓存并发、复制回滚和备份调度。

分片 GC 识别数据组返回的精简批处理回执，保留已完成分片的进度。v2.6.3 和 v2.6.4 是未发布的候选标签。

## 升级

替换各 Raft 节点时沿用已配置的协议。按组滚动替换，各副本追平后再继续。需要启用新协议时使用独立维护窗口。使用流式 SDK 的调用方应读取到流结束并检查错误。首次从旧目录核对协议下限的耗时取决于保留的日志数量。[滚动升级](https://github.com/SamuelSupe/graphdb/blob/v2.6.5/docs/raft-rolling-upgrade.zh-CN.md) · [SDK 文档](https://github.com/SamuelSupe/graphdb/blob/v2.6.5/docs/user/sdk.zh-CN.md) · [发布验证](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.5)

## 下载和校验

| 平台 | 归档 |
| --- | --- |
| Linux x86_64 | `graphdb-v2.6.5-linux-amd64.tar.gz` |
| Linux aarch64 | `graphdb-v2.6.5-linux-arm64.tar.gz` |
| 完整版（含 macOS ARM64 二进制） | `graphdb-v2.6.5.tar.gz` |

将归档与对应的 `.sha256` 放在同一目录。以下命令校验 Linux AMD64 归档并检查版本。

```sh
archive=graphdb-v2.6.5-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
./graphdb-v2.6.5-linux-amd64/graphdb version
```

完整版包含源码、SDK、部署示例和发布门禁证据。`BUILD-METADATA.json` 记录版本和构建提交。
