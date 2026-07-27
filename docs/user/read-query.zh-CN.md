# 读取与查询

[English](read-query.md)

查询 API 用于图遍历和过滤读取；运维抽取使用 scan/export API，详见
[扫描与导出](scan-export.zh-CN.md)。

## 实体查询

```sh
curl -sS "$READER/v1/entities/host:aws:i-001?min_version=12" \
  -H 'X-Tenant-ID: demo'
```

成功返回实体对象，不存在时返回 `404`。

## JSON Query DSL

入口：

- `POST /v1/query`
- `POST /v1/query/stream`

支持的 `op`：

- `match`
- `pattern`
- `neighbors`
- `traverse`
- `impact`
- `shortest_path`
- `explain`
- `profile`

示例：

```json
{
  "op": "match",
  "kind": "host",
  "where": [
    {"field": "hostname", "op": "prefix", "value": "app-"},
    {"field": "region", "op": "in", "value": ["us-east-1", "eu-west-1"]}
  ],
  "project": ["id", "hostname", "region"],
  "sort": [{"field": "hostname"}],
  "limit": 100,
  "timeout_ms": 3000,
  "cost_limit": 100000
}
```

响应：

```json
{
  "version": 12,
  "results": [],
  "next_cursor": "",
  "stats": {"scanned": 0, "visited": 0, "returned": 0, "cost": 0}
}
```

完整 JSON DSL 见 [query_capabilities.md](../query_capabilities.md)。

## GraphQL

GraphQL 接收标准 document 和 `QueryRequest` 变量：

```sh
curl -sS -X POST "$READER/v1/query/graphql" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"query Find($request: QueryRequest!) { graph(request: $request) { version results stats nextCursor } }",
    "operationName":"Find",
    "variables":{"request":{"op":"match","kind":"host","limit":100}}
  }'
```

schema、响应 envelope、错误、fragment 和 1.1 边界见
[graphql.zh-CN.md](../graphql.zh-CN.md)。

## 旧文本 DSL

1.0 `FIND`/`MATCH` 文本 DSL 会编译为 JSON DSL。它过去使用的 `GQL` 名称
已经弃用；该入口不是 GraphQL。

```sql
FIND host
WHERE hostname PREFIX "app-" AND region IN ["us-east-1", "eu-west-1"]
PROJECT id, hostname, region
ORDER BY hostname ASC
LIMIT 100
```

HTTP：

```sh
curl -sS -X POST "$READER/v1/query/gql" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: text/plain' \
  --data-binary @query.gql
```

CLI：

```sh
go run ./cmd/graphdb gql demo query.gql
```

兼容语法见 [gql.md](../gql.md)。

## 图操作

有界图模式（本例精确匹配两步）：

```json
{
  "op": "pattern",
  "kind": "document",
  "where": [{"field": "labels", "op": "contains", "value": "article"}],
  "path": {
    "steps": [
      {
        "direction": "out",
        "relation_types": ["cites"],
        "node_kinds": ["document"],
        "where": [{"field": "status", "op": "eq", "value": "published"}]
      },
      {
        "direction": "in",
        "relation_types": ["authored_by"],
        "node_kinds": ["person"]
      }
    ]
  },
  "limit": 20
}
```

`pattern` 必须包含 1 到 8 步，返回精确满足该步数的完整路径。每一步可以
独立限制方向、关系类型、目标实体类型/属性和边属性；不支持无界重复、变量
绑定、可选模式或 join。

邻居：

```json
{
  "op": "neighbors",
  "id": "service:api",
  "direction": "out",
  "relation_type": "runs_on",
  "limit": 50
}
```

遍历：

```json
{
  "op": "traverse",
  "id": "service:api",
  "direction": "out",
  "relation_types": ["depends_on", "runs_on"],
  "depth": 3,
  "path": {
    "end_kind": "database",
    "max_paths": 100
  },
  "limit": 100
}
```

影响分析：

```json
{
  "op": "impact",
  "id": "database:orders",
  "depth": 4,
  "path": {"end_kind": "service"},
  "limit": 100
}
```

最短路径：

```json
{
  "op": "shortest_path",
  "id": "service:checkout",
  "target_id": "database:orders",
  "direction": "out",
  "depth": 6
}
```

当索引 catalog 与可见图版本一致时，持久化正向和反向邻接 shard 让 lazy
read 可以执行 `out`、`in`、`both` 三种方向，而不必加载整张图。

## 过滤、投影、排序和聚合

支持：

- `eq`、`neq`、`in`
- `gt`、`gte`、`lt`、`lte`
- `exists`
- `prefix`、`contains`、`fuzzy`
- 使用 `and`、`or`、`not` 的 `where_expr`
- 路径/邻居边过滤的 `edge_where` 和 `edge_where_expr`

投影可以减少返回字段：

```json
"project": ["id", "kind", "hostname", "fields.owner"]
```

聚合：

```json
"aggregate": [
  {"op": "count"},
  {"name": "avg_cpu", "op": "avg", "field": "cpu"},
  {"name": "by_region", "op": "count_by", "field": "region"}
]
```

分组：

```json
"group_by": ["region"]
```

## 分页

使用 `limit` 和 `next_cursor`：

```sh
curl -sS -X POST "$READER/v1/query" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"op":"match","kind":"host","limit":100}'
```

下一页提交带有返回 cursor 的相同查询：

```json
{"op":"match","kind":"host","limit":100,"cursor":"..."}
```

cursor 绑定查询结构和 snapshot 版本。使用不同查询或不兼容版本复用 cursor
会被拒绝。无显式排序的 match cursor 还会绑定内部扫描顺序，客户端必须将
cursor 视为不透明值。

## 流式查询

使用 `POST /v1/query/stream` 或 `POST /v1/query/gql/stream` 获取
NDJSON。响应依次输出 meta 行、结果行和最终 done 行。

## Explain 与 Profile

```json
{"op":"explain","target_op":"match","kind":"host","where":[{"field":"hostname","op":"eq","value":"app-01"}]}
```

```json
{"op":"profile","target_op":"match","kind":"host","where":[{"field":"region","op":"eq","value":"us-east-1"}]}
```

普通查询也可以设置 `profile=true`，返回计划和算子耗时。

## 保存的查询

保存：

```sh
curl -sS -X POST "$WRITER/v1/query/templates" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/query-template-hosts.json
```

列出：

```sh
curl -sS "$READER/v1/query/templates" -H 'X-Tenant-ID: demo'
```

执行：

```sh
curl -sS -X POST "$READER/v1/query/templates/hosts-by-region/run" \
  -H 'X-Tenant-ID: demo'
```

## 运行中查询控制

列出当前进程查询：

```sh
curl -sS "$READER/v1/queries/running" -H 'X-Tenant-ID: demo'
```

取消：

```sh
curl -sS -X DELETE "$READER/v1/queries/running/<query-id>" \
  -H 'X-Tenant-ID: demo'
```
