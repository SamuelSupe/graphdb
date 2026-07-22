# 查询能力说明

本文说明当前 GraphDB 的用户侧查询能力。查询对象是租户当前可见快照，不提供历史版本查询；读一致性通过 `min_version` 和 `allow_stale` 控制。

## 入口

HTTP 查询入口：

- `POST /v1/query`: 返回 JSON 结果页。
- `POST /v1/query/stream`: 返回 `application/x-ndjson`，先输出 meta，再输出结果行，最后输出 done meta。
- `POST /v1/query/gql`: 执行文本 GQL 查询，先编译成 JSON DSL。语法见 [gql.md](gql.md)。
- `POST /v1/query/gql/stream`: 执行文本 GQL 查询并返回 NDJSON。
- `GET /v1/queries/running`: 列出当前进程内该租户正在执行的查询。
- `DELETE /v1/queries/running/{query_id}`: 取消当前进程内指定查询。

已实现但 OpenAPI 合同还需要补齐的模板入口：

- `GET /v1/query/templates`: 列出 saved query。
- `POST /v1/query/templates`: 保存 saved query。reader mode 禁止写。
- `POST /v1/query/templates/{name}/run`: 执行 saved query。

当前态运维扫描和导出不走查询 DSL，使用：

- `GET /v1/entities`
- `GET /v1/entities/stream`
- `GET /v1/edges`
- `GET /v1/edges/stream`
- `GET /v1/export/snapshot`
- `GET /v1/export/snapshot/stream`

所有租户查询都需要 `X-Tenant-ID`。读新鲜度可以放在 body，也可以放在 query/header：

- body: `min_version`, `allow_stale`
- query: `min_version`, `allow_stale`
- header: `X-GraphDB-Min-Version`, `X-GraphDB-Allow-Stale`

## 请求结构

```json
{
  "op": "match",
  "kind": "host",
  "where": [
    {"field": "hostname", "op": "prefix", "value": "app-"}
  ],
  "project": ["id", "hostname", "cpu"],
  "sort": [{"field": "cpu", "desc": true}],
  "aggregate": [{"op": "count"}, {"name": "avg_cpu", "op": "avg", "field": "cpu"}],
  "limit": 100,
  "cursor": "",
  "timeout_ms": 3000,
  "cost_limit": 100000,
  "profile": false,
  "min_version": 123,
  "allow_stale": false
}
```

通用字段：

- `op`: 查询操作。支持 `match`、`neighbors`、`traverse`、`impact`、`shortest_path`、`explain`、`profile`。
- `kind`: entity kind，主要用于 `match`。
- `filters`: 兼容字段，等价于多个 `eq` 条件。
- `where`: 结构化过滤条件。
- `where_expr`: 布尔过滤表达式，支持 `and/or/not`。
- `edge_where`: edge 字段过滤条件，用于邻居和路径查询。
- `edge_where_expr`: edge 布尔过滤表达式。
- `id`: 起点 entity id，用于 `neighbors`、`traverse`、`impact`、`shortest_path`。
- `target_id`: 终点 entity id，用于 `shortest_path`。
- `direction`: `out`、`in`、`both`；未传时按 `both`。
- `direction_strategy`: 当前支持 `impact`，用于按关系影响方向展开。
- `relation_type`: 单个关系类型。
- `relation_types`: 多个关系类型。
- `path`: 路径过滤条件。
- `project`: 投影字段。
- `sort`: 排序字段。
- `aggregate`: 聚合定义。
- `group_by`: 分组字段列表。
- `having`: 分组后的过滤条件。
- `having_expr`: 分组后的布尔过滤表达式。
- `limit`: 单页结果数，默认 100，最大 1000。
- `cursor`: 下一页游标。
- `timeout_ms`: 查询超时，0 表示不设置请求级超时。
- `cost_limit`: 成本限制，默认 100000。
- `profile`: 是否返回执行计划和 operator 耗时。

## 返回结构

```json
{
  "version": 123,
  "results": [],
  "next_cursor": "",
  "stats": {
    "scanned": 10,
    "visited": 20,
    "returned": 5,
    "cost": 30,
    "timed_out": false,
    "truncated": false
  },
  "aggregates": {},
  "groups": [],
  "plan": {},
  "profile": []
}
```

字段说明：

- `version`: 本次查询读取的快照版本。
- `results`: 结果列表。可能包含 `entity`、`edge`、`direction`、`path`、`fields`。
- `next_cursor`: 下一页游标，空字符串表示没有下一页。
- `stats.scanned`: entity 扫描或过滤计数。
- `stats.visited`: 图遍历访问计数。
- `stats.returned`: 本页返回数量。
- `stats.cost`: 执行期间累计成本。
- `stats.truncated`: 当前页被 `limit` 截断。
- `aggregates`: 全局聚合结果。
- `groups`: `group_by` 分组结果，每组包含 `key` 和 `aggregates`。
- `plan`: `profile=true` 或 `op=explain` 时返回。
- `profile`: `profile=true` 或 `op=profile` 时返回 operator 耗时。

## 字段引用规则

entity 查询支持以下字段：

- 元字段：`id`、`kind`、`source`、`external_id`、`confidence`、`source_priority`、`created_at`、`updated_at`。
- identity 字段：`identity.<name>`。
- schemaless 字段：`<name>` 或 `fields.<name>`。

如果 schemaless 字段名与元字段冲突，例如业务字段也叫 `id`，使用 `fields.id`。

edge 排序和聚合支持：

- 元字段：`id`、`type`、`relation_type`、`from`、`to`。
- edge schemaless 字段：直接使用字段名。

path 排序支持：

- `length` 或 `depth`: path 边数量。
- `end_id`: path 终点 entity id。

## 过滤条件

`where` 条件格式：

```json
{"field": "cpu", "op": "gte", "value": 8}
```

支持操作：

- `eq` 或空 `op`: 相等。
- `neq`: 不相等。
- `in`: 值在数组中。
- `gt`、`gte`、`lt`、`lte`: 范围比较。
- `prefix`: 字符串前缀。
- `contains`: 大小写不敏感包含。
- `fuzzy`: 简单模糊匹配，支持 Unicode 文本。
- `exists`: 字段是否存在，`value` 可传 boolean；不传时按 `true`。

数值比较会尽量按数字比较，包括 JSON number；无法按数字比较时退化为字符串比较。

布尔表达式：

```json
{
  "where_expr": {
    "op": "and",
    "children": [
      {
        "op": "or",
        "children": [
          {"field": "cpu", "op": "gte", "value": 16},
          {"field": "hostname", "op": "eq", "value": "db-01"}
        ]
      },
      {"op": "not", "children": [{"field": "owner", "op": "exists", "value": true}]}
    ]
  }
}
```

## 操作

### match

按 entity kind 和字段过滤查实体。

```json
{
  "op": "match",
  "kind": "host",
  "where": [
    {"field": "region", "op": "in", "value": ["us-east-1", "eu-west-1"]},
    {"field": "cpu", "op": "gte", "value": 8}
  ],
  "project": ["id", "hostname", "cpu"],
  "sort": [{"field": "cpu", "desc": true}],
  "limit": 100
}
```

执行策略：

- `where id eq ...` 使用 entity id lookup。
- `kind + eq/in` 字段过滤优先使用 persisted field index。
- `kind + gt/gte/lt/lte/prefix/exists(true)` 可使用 field index scan。
- 无可用索引时回退到 kind scan；未传 `kind` 会扫描所有实体。

### neighbors

查询一个实体的一跳邻居。

```json
{
  "op": "neighbors",
  "id": "service:api",
  "direction": "out",
  "relation_types": ["runs_on", "depends_on"],
  "path": {"node_kinds": ["host", "database"]},
  "project": ["id", "name"],
  "limit": 100
}
```

结果包含邻居 `entity`、连接 `edge` 和 `direction`。

可以通过 `edge_where` 过滤关系字段：

```json
{
  "op": "neighbors",
  "id": "service:api",
  "direction": "out",
  "edge_where": [{"field": "status", "op": "eq", "value": "active"}]
}
```

执行策略：

- `direction=out` 且有 edge shard/index 时，优先走 persisted out-edge shard。
- `direction=in` 或 `both` 当前可能需要内存图或快照回退路径。

### traverse

从起点做有界 BFS，返回路径。

```json
{
  "op": "traverse",
  "id": "service:api",
  "direction": "out",
  "depth": 3,
  "relation_types": ["depends_on", "runs_on"],
  "path": {
    "node_kinds": ["service", "host", "database"],
    "end_kind": "database",
    "end_where": [{"field": "engine", "op": "eq", "value": "mysql"}],
    "steps": [
      {"relation_types": ["depends_on"], "node_kinds": ["service"]},
      {
        "relation_types": ["runs_on"],
        "node_kinds": ["host"],
        "edge_where": [{"field": "status", "op": "eq", "value": "active"}]
      }
    ],
    "max_paths": 100
  },
  "limit": 50
}
```

说明：

- `depth` 默认 1，最大 16。
- 遍历会避免同一路径内重复访问同一 entity。
- `path.node_kinds` 会作为中间节点剪枝。
- `path.end_kind` 和 `path.end_where` 在最终路径匹配时生效。
- `path.steps` 可以按 hop 约束 relation type、目标节点 kind、目标节点字段和 edge 字段。
- 路径查询返回完整 `path`，不会对 path 内实体做字段级裁剪。

### impact

影响分析是 `traverse` 的语义化封装：

- 自动设置 `direction_strategy=impact`。
- 未传 `depth` 时默认 4。
- 按 `RelationType.impact_direction` 判断可传播方向。

```json
{
  "op": "impact",
  "id": "service:api",
  "relation_types": ["depends_on"],
  "path": {"end_kind": "host"},
  "limit": 100
}
```

### shortest_path

查询起点到终点的最短路径。

```json
{
  "op": "shortest_path",
  "id": "service:api",
  "target_id": "database:orders",
  "direction": "out",
  "depth": 6,
  "relation_types": ["depends_on"]
}
```

说明：

- 必须传 `id` 和 `target_id`。
- 使用 BFS，在 `depth` 范围内找到第一条满足 path 过滤的路径。
- 未找到时返回空 results。

### explain

只返回计划，不执行查询。

```json
{
  "op": "explain",
  "target_op": "match",
  "kind": "host",
  "where": [{"field": "hostname", "op": "prefix", "value": "app-"}]
}
```

### profile

执行目标查询，并返回 plan 和 operator 耗时。

```json
{
  "op": "profile",
  "target_op": "match",
  "kind": "host",
  "where": [{"field": "hostname", "op": "eq", "value": "app-01"}],
  "limit": 10
}
```

也可以直接在普通查询里设置 `"profile": true`。

## 排序、投影和聚合

排序：

```json
"sort": [{"field": "cpu", "desc": true}, {"field": "hostname"}]
```

未显式排序时，分页内部会按结果 identity 保持稳定顺序。

投影：

```json
"project": ["id", "kind", "hostname", "fields.id", "identity.provider"]
```

投影会：

- 在 `result.fields` 返回投影值。
- 裁剪 `result.entity.fields` 和 `field_sources`。
- 对 path 结果不做字段级裁剪，因为 path 需要保留完整路径实体。

聚合：

```json
"aggregate": [
  {"op": "count"},
  {"op": "count_by", "field": "region"},
  {"name": "avg_cpu", "op": "avg", "field": "cpu"},
  {"op": "min", "field": "cpu"},
  {"op": "max", "field": "cpu"},
  {"op": "sum", "field": "cpu"}
]
```

聚合结果放在 `aggregates`。`count_by` 返回 value 到 count 的 map；数值聚合会忽略非数值。

分组聚合：

```json
{
  "op": "match",
  "kind": "host",
  "group_by": ["owner", "region"],
  "aggregate": [
    {"op": "count"},
    {"name": "avg_cpu", "op": "avg", "field": "cpu"}
  ],
  "having": [
    {"field": "count", "op": "gte", "value": 2}
  ]
}
```

分组结果放在 `groups`，每组包含 `key` 和 `aggregates`。没有显式 `aggregate` 但设置了 `group_by` 时，默认每组计算 `count`。

## 分页和游标

`limit` 默认 100，最大 1000。返回 `next_cursor` 后，下一页需要复用完全相同的查询条件，只改 `cursor`。

游标特性：

- 新游标包含快照 `version`、上一条结果 identity 和查询 hash。
- 如果请求条件变化，游标会报错。
- 如果 reader 当前版本与游标版本不一致，游标会报错。
- 旧式数字 offset cursor 仍兼容，但不建议新调用方使用。

## 一致性和 reader 新鲜度

读请求默认读取当前 reader 可见版本。需要读己之写时，传入写入响应里的 manifest version：

```json
{
  "op": "match",
  "kind": "host",
  "min_version": 123
}
```

如果 reader 未追到 `min_version`，返回 `503 reader_not_fresh`。如果允许读本地可见旧版本，设置 `allow_stale=true`。

## Lazy Read 和索引下推

当 persisted index catalog 与目标版本一致时，查询可以走 lazy read：

- `match` 可通过 field index 或 entity page 找到候选 ID，再按需 materialize entity。
- `neighbors/traverse/impact/shortest_path` 在 `direction=out` 时可以读取 edge shard，再按需拉取目标 entity。
- `project`、`where`、`sort`、`aggregate`、`group_by` 相关字段会参与 materialize field 集合，减少 entity page 反序列化范围。
- 如果 lazy read 所需 index/page/shard 不可用，系统会回退到加载图快照；纯 lazy 场景下缺必要对象会返回 `persisted index unavailable`。

当前不会完全下推的场景：

- `direction=in` 和 `direction=both` 的复杂遍历。
- path 结果的字段级 projection。
- `contains/fuzzy/neq` 这类过滤通常需要候选 materialize 后再判断。

## Streaming

`POST /v1/query/stream` 和 `POST /v1/query/gql/stream` 输出 NDJSON。lazy match 且无 sort/aggregate/group 时可以边拉取边输出；其他查询会先执行得到当前页或聚合结果，再按 NDJSON 输出。stream meta 会包含 `aggregates` 和 `groups`。

典型输出：

```jsonl
{"version":123,"stream":true}
{"entity":{"id":"host:app-01","kind":"host"}}
{"version":123,"stats":{"scanned":1,"visited":0,"returned":1,"cost":1},"done":true}
```

## 错误和限制

常见错误：

- `422`: DSL 无效，例如未知 `op`、未知 filter op、缺少 `id`。
- `429`: 查询 admission、timeout 或 cost limit 超限。
- `503 reader_not_fresh`: reader 未达到要求版本。

限制：

- 不支持 Cypher/Gremlin。
- 不支持跨租户查询。
- 不支持历史版本查询。
- 不支持 join、子查询、复杂表达式计算。
- 单页最大 `limit=1000`。
- `depth` 最大 16。

## CMDB 场景示例

以下示例使用 CMDB 常见的实体和关系命名，展示的是通用查询能力的一种应用
方式，不限制查询对象必须是 CMDB 数据。

按主机名查 host：

```json
{
  "op": "match",
  "kind": "host",
  "where": [{"field": "hostname", "op": "eq", "value": "app-01"}],
  "limit": 1
}
```

查服务依赖：

```json
{
  "op": "neighbors",
  "id": "service:checkout",
  "direction": "out",
  "relation_types": ["depends_on", "runs_on"],
  "limit": 100
}
```

服务影响分析：

```json
{
  "op": "impact",
  "id": "database:orders",
  "depth": 4,
  "path": {"end_kind": "service"},
  "limit": 100
}
```

查询大规格主机并统计区域：

```json
{
  "op": "match",
  "kind": "host",
  "where": [{"field": "cpu", "op": "gte", "value": 16}],
  "aggregate": [
    {"op": "count"},
    {"op": "count_by", "field": "region"}
  ],
  "sort": [{"field": "cpu", "desc": true}],
  "project": ["id", "hostname", "cpu", "region"],
  "limit": 100
}
```
