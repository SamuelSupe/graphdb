# GGraphDB 2.3.0 文档

[English](README.md)

当前稳定版本为 **[2.3.0](https://github.com/SamuelSupe/graphdb/releases/tag/v2.3.0)**。
本文档入口及下列使用指南描述当前实现，覆盖单机、Raft、租户分片与内嵌双语控制台。
Go/Python SDK 和 OpenAPI 为 2.3.0；Go 模块 `/v2`、HTTP `/v1` 和独立 Raft 协议编号保持原契约。
用户指南位于 [user/](user/README.zh-CN.md)，每份都有对应英文文件与语言切换链接。

## 用户指南

- [管理控制台](management-ui.zh-CN.md) · [English](management-ui.md)

- [2.3.0 发行说明](../release/local-disk.md) · [发布验证](validation-v2.3.0.md) · [JSON](validation-v2.3.0.json)

- [本地磁盘部署与持久化](local-disk.zh-CN.md) · [English](local-disk.md)
- [对象存储快照备份与恢复](object-backup.zh-CN.md) · [English](object-backup.md)
- [单机与 Raft 自动备份](backup-automation.zh-CN.md) · [English](backup-automation.md)

- [用户指南](user/README.zh-CN.md) · [English](user/README.md)
- [快速开始](user/quickstart.zh-CN.md) · [English](user/quickstart.md)
- [使用手册](user/usage-manual.zh-CN.md) · [English](user/usage-manual.md)
- [发行版部署](user/release-deployment.zh-CN.md) · [English](user/release-deployment.md)
- [部署与运维](user/deploy-ops.zh-CN.md) · [English](user/deploy-ops.md)
- [数据模型](user/data-model.zh-CN.md) · [English](user/data-model.md)
- [写入与采集](user/write-ingest.zh-CN.md) · [English](user/write-ingest.md)
- [读取与查询](user/read-query.zh-CN.md) · [English](user/read-query.md)
- [扫描与导出](user/scan-export.zh-CN.md) · [English](user/scan-export.md)
- [租户与配置](user/tenant-config.zh-CN.md) · [English](user/tenant-config.md)
- [任务与维护](user/tasks-maintenance.zh-CN.md) · [English](user/tasks-maintenance.md)
- [错误与故障排查](user/errors-troubleshooting.zh-CN.md) · [English](user/errors-troubleshooting.md)
- [Go 与 Python SDK](user/sdk.zh-CN.md) · [English](user/sdk.md)
- [API Map](user/api-map.zh-CN.md) · [English](user/api-map.md)

## 集群与生产运维

- [三副本 Raft](raft-ha.zh-CN.md) · [租户分片与迁移](sharding.zh-CN.md)
- [Raft 运维](raft-operations.zh-CN.md) · [滚动升级窗口](raft-rolling-upgrade.zh-CN.md) · [发布与部署验收](raft-release-readiness.zh-CN.md)
- [生产安全](security-deployment.zh-CN.md) · [产品运维](product-operations.zh-CN.md) · [诊断指标](diagnostics-metrics.zh-CN.md)

## 参考文档

- [数据库简介](database-introduction.zh-CN.md) · [English](database-introduction.md)
- [GraphQL](graphql.zh-CN.md) · [English](graphql.md)
- [旧文本 DSL 兼容入口](gql.md)
- [命名与兼容](naming-and-compatibility.zh-CN.md) · [English](naming-and-compatibility.md)
- [查询能力](query_capabilities.md)
- [错误码](error_codes.md)
- [整体架构](architecture.md)
- [OpenAPI](openapi.yaml)
- [功能缺口](product_function_gaps.md)

## 历史设计与验证记录

以下记录固定于各自日期和构建，保留原有 PASS、FAIL、跳过和未发布状态。
当前发行资格以[2.3.0 验证](validation-v2.3.0.md)为准；历史性能结果不构成当前容量保证。

- [2.2.2 发布后的产品 P0/P1 审核](product-p0-p1-review-2026-10-03.zh-CN.md)
- [后续审核：manifest、图摘要与来源身份](product-p0-p1-review2-2026-10-03.zh-CN.md)
- [后续审核：迁移完整性与本地覆盖复制](product-p0-p1-review3-2026-10-03.zh-CN.md)
- [后续审核：迁移分块、Raft 快照与数据前缀完整性](product-p0-p1-review4-2026-10-03.zh-CN.md)
- [后续审核：完整恢复路径与 WAL/Raft 目录占用](product-p0-p1-review5-2026-10-03.zh-CN.md)
- [未发布候选完整测试与修复](full-validation-2026-10-03.zh-CN.md)
- [未发布候选可用性与滚动升级修复验证](availability-rolling-validation-2026-10-04.zh-CN.md)
- [Raft 三副本三十分钟长测复验：FAIL](raft-long-validation-2026-10-04.zh-CN.md)
- [Raft 可用性缺陷修复与三十分钟复验：PASS](raft-availability-fixes-2026-10-04.zh-CN.md)
- [Raft 快照与维护传输优化及性能验收](performance-raft-batching-2026-10-04.zh-CN.md)
- [2.2.4 发布验证](validation-v2.2.4.md) · [JSON](validation-v2.2.4.json)
- [历史 2.1.2 写入长尾实测](performance-write-tail.md)
- [备份深度故障测试与修复](backup-deep-validation-2026-10-04.zh-CN.md)
- [历史记录：本地磁盘 v2 验证与性能抽查](performance-local-disk-v2.md)
- [历史记录：本地磁盘第二轮优化：分页、JSON 编码与 Parquet 布局](performance-local-disk-optimization-2.md)
- [历史 HA 设计](high-availability-design.zh-CN.md) · [HA 验证](raft-ha-validation.zh-CN.md) · [分片验证](sharding-validation.zh-CN.md)
