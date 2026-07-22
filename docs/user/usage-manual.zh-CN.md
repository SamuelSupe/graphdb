# GraphDB 使用手册

[English](usage-manual.md)

本文档给出从创建租户、写入通用图数据到查询和维护的最短操作路径；其中的
CMDB 数据治理是可选场景能力。HTTP API 的完整契约见 [openapi.yaml](../openapi.yaml)，更细的模型说明见
[data-model.md](data-model.zh-CN.md)。

## 1. 启动与变量

本地单进程：

```sh
go run ./cmd/graphdb serve
```

Docker Compose：

```sh
docker compose up --build
```

设置 API 地址和租户：

```sh
export BASE=http://127.0.0.1:8080
export TENANT=demo
export TENANT_HEADER="X-Tenant-ID: $TENANT"
```

所有租户数据 API 都需要 `X-Tenant-ID`；JSON 请求还需要
`Content-Type: application/json`。

## 2. 租户生命周期

创建租户：

```sh
curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

查看租户：

```sh
curl -fsS "$BASE/v1/tenants/demo" -H "$TENANT_HEADER"
curl -fsS "$BASE/v1/tenants" -H "$TENANT_HEADER"
```

CLI 等价命令：

```sh
go run ./cmd/graphdb create-tenant demo
go run ./cmd/graphdb tenant demo
go run ./cmd/graphdb list-tenants
```

禁用租户、删除租户和 purge 属于破坏性操作，先执行备份并确认租户 ID，
再使用 CLI 的 `disable-tenant`、`delete-tenant` 或 `purge-tenant`。

## 3. 写入实体和边

仓库自带示例 commit：

```sh
curl -fsS -X POST "$BASE/v1/commits" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json
```

也可以使用 CLI：

```sh
go run ./cmd/graphdb commit demo examples/commit.json
```

写入响应中的 `version` 是这次提交的图版本。批量采集使用：

```sh
curl -fsS -X POST "$BASE/v1/ingest/batches" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: application/json' \
  --data @examples/ingest-cmdb.json
```

采集器应为同一逻辑批次复用 `batch_id` 和幂等键。收到 `429` 时遵守
`Retry-After`，使用指数退避和抖动重试，不要为同一批次生成新幂等键。

## 4. 查询

JSON DSL 查询：

```sh
curl -fsS -X POST "$BASE/v1/query" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json
```

GQL 文本查询：

```sh
curl -fsS -X POST "$BASE/v1/query/gql" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: text/plain' \
  --data-binary 'FIND person WHERE name = "Alice" LIMIT 10'
```

常见查询类型：

- `match`：按 kind、字段、排序、投影和分页查实体。
- `neighbors`：查询一个实体的入边、出边或双向邻居。
- `traverse`：执行有深度上限的路径遍历。
- `impact`：按照关系类型的影响方向传播。
- `shortest_path`：查询两个实体之间的最短路径。
- `scan` 和 snapshot export：导出当前实体或边集合。

字段过滤支持 `eq`、`neq`、`in`、`exists`、比较、前缀、包含和 fuzzy；
可用 [query_capabilities.md](../query_capabilities.md) 查看完整结构。

## 5. 读后写一致性

单进程 `all` 模式通常可直接读取。writer/reader 分离时，使用写入响应的
版本号：

```sh
curl -fsS "$BASE/v1/entities/person:alice?min_version=1" \
  -H "$TENANT_HEADER"
```

如果 reader 尚未追上版本，会返回可重试的 `reader_not_fresh`。只有业务
明确接受最终一致性时，才使用：

```sh
curl -fsS "$BASE/v1/entities/person:alice?allow_stale=true" \
  -H "$TENANT_HEADER"
```

## 6. CMDB 场景能力（可选）

CI type 可声明字段类型、必填、枚举、默认值、索引和唯一约束；source
policy 用于控制多个采集源对同一字段的优先级。常见关系类型包括
`contains`、`runs_on`、`depends_on`、`owned_by` 和 `connects_to`。

仓库示例：

```sh
go run ./cmd/graphdb commit demo examples/commit-cmdb.json
go run ./cmd/graphdb query demo examples/query-cmdb-host.json
go run ./cmd/graphdb query demo examples/query-cmdb-runs-on.json
go run ./cmd/graphdb set-source-policy demo examples/source-policy.json
go run ./cmd/graphdb source-policy demo
```

## 7. 查询模板与索引

保存并执行模板：

```sh
go run ./cmd/graphdb save-query demo examples/query-template-hosts.json
go run ./cmd/graphdb list-queries demo
go run ./cmd/graphdb run-saved-query demo hosts-by-region
```

索引维护：

```sh
go run ./cmd/graphdb index-health demo
go run ./cmd/graphdb index-inspect demo
go run ./cmd/graphdb rebuild-indexes demo
```

HTTP 客户端可以使用 `POST /v1/indexes/rebuild?async=true`，再通过任务接口
轮询状态。深度健康检查使用 `GET /v1/indexes/health?deep=true`，不建议在
高峰期频繁执行。

## 8. 维护、备份和故障排查

常用维护命令：

```sh
go run ./cmd/graphdb compact demo
go run ./cmd/graphdb gc demo
go run ./cmd/graphdb integrity-audit demo
go run ./cmd/graphdb backup-tenant demo
go run ./cmd/graphdb recover demo
```

服务指标和 OpenAPI：

```sh
curl -fsS "$BASE/metrics"
curl -fsS "$BASE/openapi.yaml"
```

排查顺序建议是：先查 `/v1/health`，再查对象存储连通性、bucket/prefix、
`/metrics` 中的写入 backpressure、CAS 冲突、reader 可见版本和索引健康。
错误响应中的 `code`、`retryable` 和 `detail` 比展示文本更适合程序处理；
错误码表见 [errors-troubleshooting.md](errors-troubleshooting.zh-CN.md)。

## 9. SDK

Go SDK 和 Python SDK 的安装、写入、查询、流式读取与重试示例见
[sdk.md](sdk.zh-CN.md)。生产 collector 应优先使用批量 ingest、稳定幂等键和
`Retry-After`，并把 tenant、source、external ID 和 identity key 作为可追踪
字段保存。
