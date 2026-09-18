# 对象存储快照备份验证记录

日期：2026-09-16。工作区：`GraphDB-local-disk-v2`，分支：`codex/local-disk-v2`，
基于 `ffa854149b7d481cd56da58f4a1f2bf61a58af92` 的未提交实现。
本记录仅对应新增对象快照备份功能，不替代此前的本地磁盘性能报告。

## 环境与产物

- OrbStack Linux，`golang:1.25-bookworm`；验证容器上限 8 CPU / 8 GiB。
- 独立 MinIO：`RELEASE.2025-09-07T16-13-09Z`；独立测试桶和 Linux 命名卷。
- 最终 HTTP 验证二进制 SHA-256：
  `2b6f3d6e6228c8ab817d672d27ea3fc3618b72256490093ef20f4fdd51935efa`。
- 本机证据目录：[capacity-runs/object-backup-20260916](../capacity-runs/object-backup-20260916)。
  测试二进制和恢复后的本地数据保留在 `graphdb-local-disk-v2-validation` 卷的
  `/validation/object-backup-20260916-final3`。

## 已执行检查

| 检查 | 结果 |
| --- | --- |
| `go test -mod=readonly ./...` | 通过 |
| `go vet -mod=readonly ./...`，最终修改涉及包补充 vet | 通过 |
| 对象存储和恢复边界测试，启用 `-race` | 通过 |
| HTTP、配置和 Go SDK 包的完整 `-race` 测试 | 通过 |
| Python SDK 单元检查 | 11 项，1 项因未设置旧 E2E 环境变量跳过；其余通过 |
| Python SDK 实际 HTTP/WAL 备份恢复流程 | 通过，独立于上述跳过项 |
| OpenAPI、Compose、CI/release YAML 解析，Compose 配置展开、Shell 语法及 diff 检查 | 通过 |

对象传输边界测试使用 20 MiB 文件，确认触发多分片上传；注入清单发布失败后，列表中没有
完整备份；重试能够发布相同快照，重复发布相同内容成功，不同版本不能覆盖同一备份 ID。
分页不会越过租户边界；非法桶、前缀或 URI 在网络请求前拒绝。
取消上传后验证没有遗留 multipart upload；下载被取消或内容发生等长篡改时失败。

存储集成测试模拟“捕获文件已落盘、后续检查点尚未写入”的中断边界，关闭并重新打开
独占数据目录。源租户继续提交后，重试仍上传原始版本。随后清空源租户，确认远端快照
可独立列出、dry-run 和恢复；图内容、租户配置、来源策略、名称与捕获时一致。
损坏远端快照后执行覆盖恢复，任务失败且目标图不变，没有可见暂存文件遗留。

真实 HTTP 流程使用同步 WAL 写入并等待提交，创建两个不同时间的对象快照，停止服务后
改用全新数据目录。由远端列表找到快照，完成 dry-run、新租户恢复、覆盖恢复和再次重启，
校验导出图、实体查询和租户名称。比较导出时只调整目标租户 ID，完整图快照与版本保持相同。
另通过真实 HTTP 执行 `restore-drill`，结果为可恢复，并完成演练租户清理。

## 复现与边界

配置独立测试桶以及 `GRAPHDB_TEST_BACKUP_S3_ENDPOINT`、`_BUCKET`、`_ACCESS_KEY_ID`、
`_SECRET_ACCESS_KEY`，在 Linux/OrbStack 执行 `scripts/object_backup_gate.sh`。
完整操作示例见 [中文指南](object-backup.zh-CN.md) / [English guide](object-backup.md)。
CI 与 release 复用独立 MinIO 验证流程并保留日志。

本轮未连接 AWS 生产账号、未实测实例角色凭据或 OSS/OBS/COS 原生接口，未运行大规模
备份性能验收或重复对比矩阵。AWS S3 兼容能力由 SDK 接入，当前运行证据来自 MinIO。
快照只覆盖已提交的逻辑图和明确列出的租户配置，不包含未发布 WAL 或完整数据目录历史。
未提交、合并、推送或发布本分支。
