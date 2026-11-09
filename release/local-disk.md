# GGraphDB v2.5.1

多租户属性图数据库，支持单机、Raft 高可用和按租户分片，内置中英文管理控制台。

## 本版更新

- **单机 Compose 默认异步接入**。采集批次在 WAL 同步落盘后返回 HTTP 202。轮询 `status_url` 获取终态，需要等待提交时发送 `Prefer: wait=committed`。该请求会主动调度批次，同时保留同租户顺序和失败退避。
- **分段追平索引**。合并增量超出预算时，按 manifest 版本封存该段并继续排队。后台按版本依次提交增量索引，跟踪分段、历史图加载和全量重建。积压期间图提交可以继续，查询可能看到索引滞后。

同一大图的开发阶段 A/B 压测中，WAL 受理 P99 从 263 ms 降至 190 ms，受理后到提交终态 P99 从 11.30 s 降至 569 ms。新版停写后约 95 秒索引追平。负载与验证边界见[版本验证](https://github.com/SamuelSupe/graphdb/blob/v2.5.1/docs/validation-v2.5.1.md)。

## 升级与回退

依赖 HTTP 200/207 同步响应的 Compose 客户端，可设置 `GRAPHDB_INGEST_MODE=direct` 继续使用原同步接入方式。升级前请验证完整运行状态备份，包含数据、WAL 和协调状态。使用同一数据目录切换接入模式前，请先停止旧进程并处理待提交的 WAL。Raft 协议激活和版本滚动窗口见[升级手册](https://github.com/SamuelSupe/graphdb/blob/v2.5.1/docs/raft-rolling-upgrade.zh-CN.md)。

## Linux 下载

| 平台 | 发行包 |
| --- | --- |
| x86_64（AMD64） | `graphdb-v2.5.1-linux-amd64.tar.gz` |
| aarch64（ARM64） | `graphdb-v2.5.1-linux-arm64.tar.gz` |

下载对应架构的发行包和同名 `.sha256` 文件。在同一目录执行：

```sh
archive=graphdb-v2.5.1-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
cd "${archive%.tar.gz}"
./graphdb version
GRAPHDB_DATA_DIR=./data ./graphdb serve
```

完整包 `graphdb-v2.5.1.tar.gz` 还包含 macOS ARM64、源码、SDK、部署示例和门禁证据。发行二进制使用 Go 1.27.3。

## Validation

Publication requires the exact-tag unit, vet, race, SDK, direct/WAL recovery, S3 backup, gateway, Raft and 30-minute soak gates. The full archive contains runtime qualification evidence. Protocol 5 is an opt-in preview. Cross-host recovery, day-scale stability and production capacity remain unqualified.

[Deployment](https://github.com/SamuelSupe/graphdb/blob/v2.5.1/docs/user/release-deployment.md) · [中文部署指南](https://github.com/SamuelSupe/graphdb/blob/v2.5.1/docs/user/release-deployment.zh-CN.md) · [Validation](https://github.com/SamuelSupe/graphdb/blob/v2.5.1/docs/validation-v2.5.1.md)
