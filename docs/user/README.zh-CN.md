# GraphDB 用户指南

[English](README.md)

本指南面向服务负责人、采集器作者和使用 GraphDB 作为内部 CMDB 图数据库的
运维人员。

## GraphDB 提供什么

- 通过 `X-Tenant-ID` 隔离多租户图数据；
- 无模式实体和可选 CI type 定义；
- 以 `(type, from, to)` 作为规范身份的有向类型化边；
- 每租户一个活跃 writer，reader 独立从对象存储重新加载；
- 基于 Parquet manifest、commit、snapshot、entity page、edge shard 和索引
  对象的对象存储持久化；
- JSON Query DSL、文本 GQL、scan/export、saved query 和运行中查询控制；
- 对实体字段、边字段和边存在性的 source priority 治理；
- 租户生命周期、source policy、tenant config、索引、统一 task、维护、
  完整性审计和 reader freshness。

## HTTP 约定

所有租户范围的数据 API 都需要：

```http
X-Tenant-ID: <tenant-id>
Content-Type: application/json
```

读取新鲜度控制：

- body：`min_version`、`allow_stale`；
- query：`?min_version=123&allow_stale=true`；
- header：`X-GraphDB-Min-Version`、`X-GraphDB-Allow-Stale`。

运行模式：

- `GRAPHDB_MODE=writer`：启用写入/控制 API，读取 API 可用于检查；
- `GRAPHDB_MODE=reader`：写入、配置和 task 变更返回 `405`，读取、查询、
  scan、指标和 freshness API 仍可用；
- `GRAPHDB_MODE=all`：本地或小规模单进程模式。

示例变量：

```sh
export WRITER=http://127.0.0.1:38080
export READER=http://127.0.0.1:38081
export BASE=http://127.0.0.1:8080
```

## 文档地图

- [快速开始](quickstart.zh-CN.md) · [English](quickstart.md)
- [发行版部署](release-deployment.zh-CN.md) · [English](release-deployment.md)
- [使用手册](usage-manual.zh-CN.md) · [English](usage-manual.md)
- [数据模型](data-model.zh-CN.md) · [English](data-model.md)
- [写入与采集](write-ingest.zh-CN.md) · [English](write-ingest.md)
- [读取与查询](read-query.zh-CN.md) · [English](read-query.md)
- [扫描与导出](scan-export.zh-CN.md) · [English](scan-export.md)
- [租户与配置](tenant-config.zh-CN.md) · [English](tenant-config.md)
- [部署与运维](deploy-ops.zh-CN.md) · [English](deploy-ops.md)
- [任务与维护](tasks-maintenance.zh-CN.md) · [English](tasks-maintenance.md)
- [错误与故障排查](errors-troubleshooting.zh-CN.md) · [English](errors-troubleshooting.md)
- [Go 与 Python SDK](sdk.zh-CN.md) · [English](sdk.md)
- [API Map](api-map.zh-CN.md) · [English](api-map.md)

参考文档：

- [GQL](../gql.md)
- [查询能力](../query_capabilities.md)
- [错误码](../error_codes.md)
- [OpenAPI](../openapi.yaml)
