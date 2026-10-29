# GGraphDB 用户指南

适用于 **GGraphDB 2.4.3**；[English](README.md) · [文档中心](../README.zh-CN.md) · [发布验证](../validation-v2.4.3.md)

面向服务负责人、客户端开发者和运维人员，从一次完整的数据流程开始使用。
GGraphDB 是通用属性图数据库；CMDB 是支持的场景之一。

## 从一个完整流程开始

1. [启动服务](quickstart.zh-CN.md)，通过[控制台](../management-ui.zh-CN.md)或 API 创建租户。
2. 按[数据模型](data-model.zh-CN.md)定义实体和关系，通过[写入与采集](write-ingest.zh-CN.md)提交数据。
3. 使用[查询](read-query.zh-CN.md)或[扫描导出](scan-export.zh-CN.md)读取结果。
4. 配置[租户策略](tenant-config.zh-CN.md)，通过[任务与维护](tasks-maintenance.zh-CN.md)管理运行状态。

## 文档地图

| 阶段 | 阅读入口 |
| --- | --- |
| 上手与浏览器操作 | [快速开始](quickstart.zh-CN.md) · [管理控制台](../management-ui.zh-CN.md) · [使用手册](usage-manual.zh-CN.md) |
| 建模与写入 | [数据模型](data-model.zh-CN.md) · [写入与采集](write-ingest.zh-CN.md) |
| 查询与集成 | [读取与查询](read-query.zh-CN.md) · [扫描与导出](scan-export.zh-CN.md) · [Go/Python SDK](sdk.zh-CN.md) |
| 配置与运维 | [租户与配置](tenant-config.zh-CN.md) · [任务与维护](tasks-maintenance.zh-CN.md) · [故障排查](errors-troubleshooting.zh-CN.md) |
| 安装与部署 | [发行版部署](release-deployment.zh-CN.md) · [部署与运维](deploy-ops.zh-CN.md) |
| API 参考 | [API 地图](api-map.zh-CN.md) · [OpenAPI](../openapi.yaml) · [错误码](../error_codes.md) |

每份指南顶部均有对应英文链接。完整的双语对照见[文档中心](../README.zh-CN.md#用户指南)。

## GGraphDB 提供什么

| 领域 | 行为 |
| --- | --- |
| 数据隔离 | `X-Tenant-ID` 选择租户；有向边以 `(type, from, to)` 为规范身份 |
| 模型与治理 | 无模式实体、可选类型及 CI type；实体字段、边字段和边存在性的来源优先级 |
| 查询与管理 | JSON DSL、GraphQL、保存查询、运行中查询控制、索引、租户生命周期和结构化控制台 |
| 持久化 | 单进程独占本地目录；Parquet manifest、commit、snapshot、entity page、edge shard 和索引落盘；支持 direct/WAL |
| 扩展与恢复 | 可选独立 Raft 副本、租户分片、S3 兼容快照备份与恢复 |

单机内置备份调度、重试、保留清理和演练；Raft 通过集群 API 使用外部调度。
提交使用 `data_hash` 契约，已有 2.0/2.1/2.2 单机目录兼容；不提供 1.x 迁移。
详见[版本边界](../naming-and-compatibility.zh-CN.md)。

## HTTP 约定

### 租户与请求头

租户范围的数据 API 使用以下请求头；JSON 请求须设置内容类型：

```http
X-Tenant-ID: <tenant-id>
Content-Type: application/json
```

### 读取新鲜度

| 位置 | 参数 |
| --- | --- |
| JSON body | `min_version`、`allow_stale` |
| URL query | `?min_version=123&allow_stale=true` |
| HTTP header | `X-GraphDB-Min-Version`、`X-GraphDB-Allow-Stale` |

查询需要观察某次写入时，使用写入响应的 `version` 作为 `min_version`。

### 示例地址与监听器

以下三个变量指向同一个进程，示例默认使用合并监听器：

```sh
export WRITER=http://127.0.0.1:8080
export READER=http://127.0.0.1:8080
export BASE=http://127.0.0.1:8080
```

仅支持 `GRAPHDB_MODE=all`。独立 reader/writer、远端在线存储和 PostgreSQL 协调
配置会报错。设置 `GRAPHDB_ADMIN_ADDR` 后，管理请求发送到该监听器或对应的
已认证网关路径，见[监听职责](../security-deployment.zh-CN.md#listener-职责)。

## 进一步阅读

| 主题 | 文档 |
| --- | --- |
| 查询语言 | [GraphQL](../graphql.zh-CN.md) · [查询能力](../query_capabilities.md) · [旧文本 DSL](../gql.md) |
| 集群 | [Raft 运维与升级](../raft-operations.zh-CN.md) · [租户分片与迁移](../sharding.zh-CN.md) |
| 观测 | [诊断指标](../diagnostics-metrics.zh-CN.md) |
