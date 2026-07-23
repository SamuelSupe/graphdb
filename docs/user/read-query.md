# Read And Query

[中文](read-query.zh-CN.md)

Use query APIs for graph traversal and filtered retrieval. Use scan/export APIs
for operational extraction; see [Scan And Export](scan-export.md).

## Entity Lookup

```sh
curl -sS "$READER/v1/entities/host:aws:i-001?min_version=12" \
  -H 'X-Tenant-ID: demo'
```

Response is the entity object or `404`.

## JSON Query DSL

Entrypoints:

- `POST /v1/query`
- `POST /v1/query/stream`

Supported `op` values:

- `match`
- `pattern`
- `neighbors`
- `traverse`
- `impact`
- `shortest_path`
- `explain`
- `profile`

Example:

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

Response:

```json
{
  "version": 12,
  "results": [],
  "next_cursor": "",
  "stats": {"scanned": 0, "visited": 0, "returned": 0, "cost": 0}
}
```

See [../query_capabilities.md](../query_capabilities.md) for the full JSON DSL.

## GraphQL

GraphQL accepts a standard document plus a `QueryRequest` variable:

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

See [../graphql.md](../graphql.md) for the schema, response envelope, errors,
fragments, and 1.1 boundaries.

## Legacy text DSL

The 1.0 `FIND`/`MATCH` text DSL is compiled to the JSON DSL. Its old `GQL`
name is deprecated; this interface is not GraphQL.

```sql
FIND host
WHERE hostname PREFIX "app-" AND region IN ["us-east-1", "eu-west-1"]
PROJECT id, hostname, region
ORDER BY hostname ASC
LIMIT 100
```

HTTP:

```sh
curl -sS -X POST "$READER/v1/query/gql" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: text/plain' \
  --data-binary @query.gql
```

CLI:

```sh
go run ./cmd/graphdb gql demo query.gql
```

See [../gql.md](../gql.md) for the compatibility syntax.

## Graph Operations

Bounded graph pattern (exactly two steps in this example):

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

`pattern` requires 1 to 8 steps and returns complete paths matching that exact
step count. Each step can independently constrain direction, relation types,
destination entity kinds/properties, and edge properties. It does not provide
unbounded repetition, variable binding, optional patterns, or joins.

Neighbors:

```json
{
  "op": "neighbors",
  "id": "service:api",
  "direction": "out",
  "relation_type": "runs_on",
  "limit": 50
}
```

Traverse:

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

Impact:

```json
{
  "op": "impact",
  "id": "database:orders",
  "depth": 4,
  "path": {"end_kind": "service"},
  "limit": 100
}
```

Shortest path:

```json
{
  "op": "shortest_path",
  "id": "service:checkout",
  "target_id": "database:orders",
  "direction": "out",
  "depth": 6
}
```

Persisted forward and reverse adjacency shards let lazy reads execute `out`,
`in`, and `both` traversal directions without loading the whole graph when the
index catalog matches the visible graph version.

## Filters, Projection, Sort, Aggregate

Filtering supports:

- `eq`, `neq`, `in`
- `gt`, `gte`, `lt`, `lte`
- `exists`
- `prefix`, `contains`, `fuzzy`
- `where_expr` with `and`, `or`, `not`
- `edge_where` and `edge_where_expr` for path/neighbor edge filters

Projection reduces returned fields:

```json
"project": ["id", "kind", "hostname", "fields.owner"]
```

Aggregation:

```json
"aggregate": [
  {"op": "count"},
  {"name": "avg_cpu", "op": "avg", "field": "cpu"},
  {"name": "by_region", "op": "count_by", "field": "region"}
]
```

Group by:

```json
"group_by": ["region"]
```

## Pagination

Use `limit` and `next_cursor`.

```sh
curl -sS -X POST "$READER/v1/query" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"op":"match","kind":"host","limit":100}'
```

For the next page, submit the same query with returned `cursor`:

```json
{"op":"match","kind":"host","limit":100,"cursor":"..."}
```

Cursors are tied to query shape and snapshot version. Reusing a cursor with a
different query or incompatible version is rejected.

## Streaming

Use `POST /v1/query/stream` or `POST /v1/query/gql/stream` for NDJSON output.
The response emits one meta row, result rows, then a final done row.

## Explain And Profile

Explain:

```json
{"op":"explain","target_op":"match","kind":"host","where":[{"field":"hostname","op":"eq","value":"app-01"}]}
```

Profile:

```json
{"op":"profile","target_op":"match","kind":"host","where":[{"field":"region","op":"eq","value":"us-east-1"}]}
```

`profile=true` on normal queries also returns plan and operator timings.

## Saved Queries

Save:

```sh
curl -sS -X POST "$WRITER/v1/query/templates" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/query-template-hosts.json
```

List:

```sh
curl -sS "$READER/v1/query/templates" -H 'X-Tenant-ID: demo'
```

Run:

```sh
curl -sS -X POST "$READER/v1/query/templates/hosts-by-region/run" \
  -H 'X-Tenant-ID: demo'
```

## Running Query Control

List current in-process queries:

```sh
curl -sS "$READER/v1/queries/running" -H 'X-Tenant-ID: demo'
```

Cancel:

```sh
curl -sS -X DELETE "$READER/v1/queries/running/<query-id>" \
  -H 'X-Tenant-ID: demo'
```
