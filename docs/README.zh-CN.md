# GGraphDB 2.4.1 文档中心

[English](README.md) · [项目首页](../README.zh-CN.md) · [下载发行版](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.1)

当前发行版本为 **2.4.1**，标签门禁、发布状态与独立下载核验详见本版验证记录。从下面的任务入口进入指南，或先按推荐路径阅读。
用户指南与控制台文档均提供中英文版本；部分集群运维专题目前仅有中文。

## 推荐阅读路径

| 你的目标 | 推荐顺序 |
| --- | --- |
| 首次体验 | [快速开始](user/quickstart.zh-CN.md) → [控制台](management-ui.zh-CN.md) → [使用手册](user/usage-manual.zh-CN.md) |
| 接入业务数据 | [数据模型](user/data-model.zh-CN.md) → [写入与采集](user/write-ingest.zh-CN.md) → [查询](user/read-query.zh-CN.md) → [SDK](user/sdk.zh-CN.md) |
| 准备部署 | [发行版部署](user/release-deployment.zh-CN.md) → [生产安全](security-deployment.zh-CN.md) → [备份恢复](object-backup.zh-CN.md) → [发布验证](validation-v2.4.1.md) |

## 用户指南

### 开始使用

| 任务 | 中文指南 | English |
| --- | --- | --- |
| 运行第一个示例 | [快速开始](user/quickstart.zh-CN.md) | [Quick start](user/quickstart.md) |
| 安装发行程序 | [发行版部署](user/release-deployment.zh-CN.md) | [Release deployment](user/release-deployment.md) |
| 通过浏览器操作 | [管理控制台](management-ui.zh-CN.md) | [Management console](management-ui.md) |
| 了解完整使用流程 | [使用手册](user/usage-manual.zh-CN.md) | [Usage manual](user/usage-manual.md) |
| 查找 API 约定 | [用户指南](user/README.zh-CN.md) | [User guide](user/README.md) |

### 建模、写入与查询

| 任务 | 中文指南 | English |
| --- | --- | --- |
| 设计实体与关系 | [数据模型](user/data-model.zh-CN.md) | [Data model](user/data-model.md) |
| 提交数据与采集批次 | [写入与采集](user/write-ingest.zh-CN.md) | [Write and ingest](user/write-ingest.md) |
| 读取、遍历与聚合 | [读取与查询](user/read-query.zh-CN.md) | [Read and query](user/read-query.md) |
| 扫描或导出数据 | [扫描与导出](user/scan-export.zh-CN.md) | [Scan and export](user/scan-export.md) |
| 接入 Go 或 Python 客户端 | [SDK 指南](user/sdk.zh-CN.md) | [SDK guide](user/sdk.md) |

### 运维与恢复

| 任务 | 中文指南 | English |
| --- | --- | --- |
| 配置租户生命周期与分页 | [租户与配置](user/tenant-config.zh-CN.md) | [Tenants and configuration](user/tenant-config.md) |
| 安排服务运维 | [部署与运维](user/deploy-ops.zh-CN.md) | [Deployment and operations](user/deploy-ops.md) |
| 管理任务与维护 | [任务与维护](user/tasks-maintenance.zh-CN.md) | [Tasks and maintenance](user/tasks-maintenance.md) |
| 排查 API 故障 | [故障排查](user/errors-troubleshooting.zh-CN.md) | [Troubleshooting](user/errors-troubleshooting.md) |
| 了解磁盘持久化 | [本地磁盘指南](local-disk.zh-CN.md) | [Local disk guide](local-disk.md) |
| 备份与恢复快照 | [对象备份](object-backup.zh-CN.md) | [Object backups](object-backup.md) |
| 调度单机或 Raft 备份 | [自动备份](backup-automation.zh-CN.md) | [Backup automation](backup-automation.md) |

## 集群与生产运维

| 任务 | 文档 |
| --- | --- |
| 配置副本或分片 | [三副本 Raft](raft-ha.zh-CN.md) · [租户分片与迁移](sharding.zh-CN.md) |
| 运维或升级集群 | [Raft 运维](raft-operations.zh-CN.md) · [滚动升级窗口](raft-rolling-upgrade.zh-CN.md) |
| 保护与诊断服务 | [生产安全](security-deployment.zh-CN.md) · [产品运维](product-operations.zh-CN.md) · [诊断指标](diagnostics-metrics.zh-CN.md) |
| 核对部署验收范围 | [发布与部署验收](raft-release-readiness.zh-CN.md) |

## 参考文档

| 主题 | 文档 |
| --- | --- |
| 定位与架构 | [数据库简介](database-introduction.zh-CN.md) · [English](database-introduction.md) · [整体架构](architecture.md) |
| API 契约 | [API 地图](user/api-map.zh-CN.md) · [English](user/api-map.md) · [OpenAPI](openapi.yaml) · [错误码](error_codes.md) |
| 查询语言 | [查询能力](query_capabilities.md) · [GraphQL](graphql.zh-CN.md) · [English](graphql.md) · [旧文本 DSL](gql.md) |
| 版本与缺口 | [命名与兼容](naming-and-compatibility.zh-CN.md) · [English](naming-and-compatibility.md) · [功能缺口](product_function_gaps.md) |
| 2.4.1 发行证据 | [发行说明](../release/local-disk.md) · [发布验证](validation-v2.4.1.md) |
| 2.3.6 优化候选性能对照 | [维护窗口与 30 分钟 Raft 增长负载对照](performance-tail-validation-2026-10-20.zh-CN.md) · [此前六分钟对照](performance-tail-validation-2026-10-19.zh-CN.md) |

SDK/OpenAPI 版本为 2.4.1；Go 模块 `/v2`、HTTP `/v1` 和独立 Raft 协议编号
各自遵循兼容契约，不能用产品版本号替换。

## 历史设计与验证记录

<details>
<summary>展开历史设计、审核与验证档案</summary>

以下记录固定于各自日期和构建，保留原有 PASS、FAIL、跳过和未发布状态。
当前发行资格以[2.4.1 验证](validation-v2.4.1.md)为准；历史性能结果不构成当前容量保证。

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

</details>
