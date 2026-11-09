# GGraphDB v2.5.0

多租户属性图数据库，支持单机、Raft 高可用和按租户分片，内置中英文管理控制台。

## 本版更新

- **单机 inode 回收**。按租户打包接入回执和直接提交的幂等记录，后台逐批迁移旧回执并清理空目录。自动 GC 支持配置每轮删除量、运行预算、续扫节拍和 inode 压力检查。
- **持久性与诊断**。回执提交前同步待持久化的图对象目录。首次建库使用临时文件并原子发布。`receipt-check` 在独立进程中检查记录、页面和空闲页表。开发阶段完成五个虚拟机断电恢复场景，覆盖共享图文件复用、首次建库、WAL 重放和幂等重试。
- **大图跨组分片预览**。Raft 协议 5 可显式启用单图跨组分片。改进续页、读 pin、搬迁、GC 和事务恢复。Raft 默认使用协议 1。

## 升级与回退

单机首次运行 `serve` 时先持久化本地目录格式 v2，再启用回执打包。升级前请验证完整运行状态备份，包含数据、WAL 和协调状态。回退旧版时将升级前的备份恢复到独立目录。Raft 协议激活和版本滚动窗口见[升级手册](https://github.com/SamuelSupe/graphdb/blob/v2.5.0/docs/raft-rolling-upgrade.zh-CN.md)。

## Linux 下载

| 平台 | 发行包 |
| --- | --- |
| x86_64（AMD64） | `graphdb-v2.5.0-linux-amd64.tar.gz` |
| aarch64（ARM64） | `graphdb-v2.5.0-linux-arm64.tar.gz` |

下载对应架构的发行包和同名 `.sha256` 文件。在同一目录执行：

```sh
archive=graphdb-v2.5.0-linux-amd64.tar.gz
sha256sum -c "$archive.sha256"
tar -xzf "$archive"
cd "${archive%.tar.gz}"
./graphdb version
GRAPHDB_DATA_DIR=./data ./graphdb serve
```

完整包 `graphdb-v2.5.0.tar.gz` 还包含 macOS ARM64、源码、SDK、部署示例和门禁证据。发行二进制使用 Go 1.27.3。

## Validation

Publication requires the exact-tag unit, vet, race, SDK, direct/WAL recovery, S3 backup, gateway, Raft and 30-minute soak gates. The full archive contains runtime qualification evidence. Protocol 5 is an opt-in preview. Cross-host recovery, day-scale stability and production capacity remain unqualified.

[Deployment](https://github.com/SamuelSupe/graphdb/blob/v2.5.0/docs/user/release-deployment.md) · [中文部署指南](https://github.com/SamuelSupe/graphdb/blob/v2.5.0/docs/user/release-deployment.zh-CN.md) · [Validation](https://github.com/SamuelSupe/graphdb/blob/v2.5.0/docs/validation-v2.5.0.md)
