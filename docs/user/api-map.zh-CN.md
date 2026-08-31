# API Map

[English](api-map.md)

这是面向用户的 endpoint 速查表，详细 schema 见
[../openapi.yaml](../openapi.yaml)。

## 系统

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/v1/health` | 进程存活、运行模式和 coordinator 状态；降级时仍返回 HTTP 200。 |
| `GET` | `/v1/readiness` | 流量就绪状态；对象存储或 PostgreSQL coordinator 不可用时返回 HTTP 503。 |
| `GET` | `/metrics` | Prometheus 指标。 |
| `GET` | `/openapi.yaml` | OpenAPI 合同。 |
| `GET` | `/debug/pprof/` | 可选的仅管理 listener profiling 入口，默认关闭。 |
| `GET` | `/debug/pprof/profile?seconds=30` | 可选的仅管理 listener CPU profile。 |
| `GET` | `/debug/pprof/trace?seconds=1` | 可选的仅管理 listener execution trace。 |

## 租户生命周期

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/v1/tenants` | 列出租户。 |
| `POST` | `/v1/tenants` | 创建租户。 |
| `GET` | `/v1/tenants/{tenant}` | 获取租户信息。 |
| `PUT` | `/v1/tenants/{tenant}` | 更新租户元数据。 |
| `DELETE` | `/v1/tenants/{tenant}` | 软删除租户。 |
| `POST` | `/v1/tenants/{tenant}/disable` | 禁止租户写入。 |
| `POST` | `/v1/tenants/{tenant}/enable` | 启用租户。 |
| `POST` | `/v1/tenants/{tenant}/purge` | 清理租户对象。 |
| `POST` | `/v1/tenants/{tenant}/clone` | 克隆租户。 |
| `POST` | `/v1/tenants/{tenant}/backup` | 启动备份任务。 |
| `POST` | `/v1/tenants/{tenant}/restore` | 启动恢复任务。 |
| `POST` | `/v1/tenants/{tenant}/restore-drill` | 启动恢复演练任务。 |

## 写入与采集

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `POST` | `/v1/commits` | 原子图变更提交。 |
| `POST` | `/v1/ingest/batches` | 采集批次写入；1.3 WAL 模式返回带 `writer_id` 和 owner 路由状态 URL 的 durable `202`。 |
| `GET` | `/v1/ingest/writers/{writer_id}/{source}/{collector_id}/{batch_id}` | 1.3 owner 路由的活跃/已完成采集状态。 |
| `POST` | `/v1/imports` | 提交可恢复的 CSV 或 JSONL 批量导入。 |
| `GET` | `/v1/ingest/batches/{source}/{collector_id}/{batch_id}` | 活跃/已完成采集状态；1.3 WAL 状态必须路由给 owner writer。 |
| `GET` | `/v1/ingest/collectors/{source}/{collector_id}` | 采集器状态。 |
| `GET` | `/v1/ingest/deadletters/{source}` | 列出死信。 |
| `POST` | `/v1/ingest/deadletters/{source}/replay` | 重放死信。 |
| `GET` | `/v1/source-policy` | 获取租户 source policy。 |
| `PUT` | `/v1/source-policy` | 更新租户 source policy。 |
| `PUT` | `/v1/relation-schemas/{relation_type}` | 创建或替换关系属性 schema。 |
| `DELETE` | `/v1/relation-schemas/{relation_type}` | 删除关系属性 schema。 |
| `GET` | `/v1/tenant-config` | 获取租户配置。 |
| `PUT` | `/v1/tenant-config` | 更新租户配置。 |
| `GET` | `/v1/tenant-usage` | 租户对象和字节用量。 |

## 读取与查询

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/v1/entities/{id}` | 按 ID 获取实体。 |
| `GET` | `/v1/entity-types` | 列出领域中立的实体类型。 |
| `GET` | `/v1/ci-types` | 通过 1.0 兼容路由列出实体类型。 |
| `GET` | `/v1/relation-types` | 列出关系类型。 |
| `GET` | `/v1/relation-schemas` | 列出关系属性 schema。 |
| `POST` | `/v1/query` | JSON Query DSL。 |
| `POST` | `/v1/query/stream` | JSON Query DSL NDJSON 流。 |
| `POST` | `/v1/query/graphql` | 执行带变量的 GraphQL document。 |
| `POST` | `/v1/query/gql` | 已弃用的 1.0 `FIND`/`MATCH` 文本 DSL。 |
| `POST` | `/v1/query/gql/stream` | 已弃用文本 DSL 的 NDJSON 流。 |
| `GET` | `/v1/queries/running` | 列出进程内运行中的查询。 |
| `DELETE` | `/v1/queries/running/{query_id}` | 取消运行中的查询。 |
| `GET` | `/v1/query/templates` | 列出保存的查询。 |
| `POST` | `/v1/query/templates` | 保存查询模板。 |
| `POST` | `/v1/query/templates/{name}/run` | 执行保存的查询。 |

## 扫描与导出

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/v1/entities` | 按 kind/source/shard 分页实体。 |
| `GET` | `/v1/entities/stream` | 以 NDJSON 流输出实体。 |
| `GET` | `/v1/edges` | 按 type/from/source/shard 分页边。 |
| `GET` | `/v1/edges/stream` | 以 NDJSON 流输出边。 |
| `GET` | `/v1/export/snapshot` | 内联返回当前 snapshot。 |
| `GET` | `/v1/export/snapshot/stream` | 流式返回 snapshot 行或 shard 引用。 |

## 任务与维护

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/v1/tasks` | 列出任务。 |
| `POST` | `/v1/tasks` | 启动任务。 |
| `GET` | `/v1/tasks/{task_id}` | 获取任务。 |
| `POST` | `/v1/tasks/{task_id}/cancel` | 取消任务。 |
| `POST` | `/v1/tasks/{task_id}/retry` | 重试任务。 |
| `POST` | `/v1/compact` | 同步 compact snapshot。 |
| `GET` | `/v1/indexes` | 获取索引目录。 |
| `POST` | `/v1/indexes` | 创建二级索引并启动重建。 |
| `GET` | `/v1/indexes/definitions` | 列出索引定义。 |
| `DELETE` | `/v1/indexes/definitions/{name}` | 删除索引并启动重建。 |
| `GET` | `/v1/indexes/health` | 索引健康状态。 |
| `GET` | `/v1/indexes/tasks/{task_id}` | 兼容旧索引任务查询。 |
| `POST` | `/v1/indexes/rebuild` | 重建索引。 |
| `GET` | `/v1/control/integrity-audit` | 全链完整性审计。 |
| `POST` | `/v1/control/recover` | 恢复孤儿 commit。 |
| `POST` | `/v1/control/repair` | repair dry-run/apply。 |
| `POST` | `/v1/control/cleanup-commits` | 清理过期 commit。 |
| `POST` | `/v1/control/gc` | 支持 checkpoint/dry-run 的 GC。 |

## Reader 与 Writer 控制

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/v1/control/writer-lease` | 查看 writer lease。 |
| `GET` | `/v1/control/reader-freshness` | reader 新鲜度报告。 |
| `GET` | `/v1/control/reader-lag` | freshness 兼容别名。 |
| `GET` | `/v1/control/reader-fleet-readiness` | reader 集群就绪报告。 |
| `GET` | `/v1/control/reader-traffic-gate` | 部署检查使用的流量闸门结果。 |
