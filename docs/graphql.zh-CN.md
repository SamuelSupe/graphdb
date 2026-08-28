# GraphQL API

[English](graphql.md)

GGraphDB 在 `POST /v1/query/graphql` 提供 GraphQL。该入口接收标准
GraphQL document、`operationName`、变量、alias、fragment 和
`@skip`/`@include`，并返回标准 `data`/`errors` envelope。`graph` 根字段
仍复用已有 JSON Query DSL 的 planner、索引、限流和一致性语义；GGraphDB
1.2 新增一级 `evidenceSearch` 根字段，用于确定性的 GraphRAG 检索。

## 查询

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/query/graphql \
  -H 'X-Tenant-ID: tenant-a' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "query FindHosts($request: QueryRequest!) { graph(request: $request) { version results stats nextCursor } }",
    "operationName": "FindHosts",
    "variables": {
      "request": {
        "op": "match",
        "kind": "host",
        "where": [{"field": "hostname", "op": "prefix", "value": "app-"}],
        "limit": 100
      }
    }
  }'
```

根 schema：

```graphql
scalar JSON
scalar Long
scalar QueryRequest

type Query {
  graph(request: QueryRequest!): GraphQueryResult!
  evidenceSearch(input: EvidenceSearchInput!): EvidenceSearchResult!
}

type GraphQueryResult {
  version: Long!
  results: JSON!
  nextCursor: String
  stats: JSON!
  aggregates: JSON
  groups: JSON
  plan: JSON
  profile: JSON
}

input EvidenceExpansionInput {
  maxDepth: Int
  direction: String
  relationTypes: [String!]
  nodeKinds: [String!]
  maxSeeds: Int
  maxVisited: Int
}

input EvidenceSearchInput {
  query: String!
  kinds: [String!]
  filters: JSON
  vectorTopK: Int
  lexicalTopK: Int
  topK: Int
  minVersion: Long
  explain: Boolean
  expansion: EvidenceExpansionInput
}

type EvidenceSearchResult {
  version: Long!
  retrievalRevision: Long!
  embeddingGeneration: String!
  evidence: JSON!
  stats: JSON!
  plan: JSON
}
```

`QueryRequest` 字段见 [query_capabilities.md](query_capabilities.md)。文档中
列出的 camel-case 字段同时接受 JSON 风格的 `min_version` 和 GraphQL 风格
的 `minVersion`。

## GraphRAG 证据检索

`evidenceSearch` 返回版本一致的证据包，不调用 LLM 生成答案。请求强制限制
`topK <= 100`、每路候选数不超过 1,000、图扩展深度为 `0..2`。省略参数时，
默认使用 `topK=20`、200 个向量候选、200 个全文候选和两跳扩展。

```graphql
query Evidence($input: EvidenceSearchInput!) {
  evidenceSearch(input: $input) {
    version
    retrievalRevision
    embeddingGeneration
    evidence
    stats
    plan
  }
}
```

```json
{
  "input": {
    "query": "为什么 checkout 失败？",
    "kinds": ["TextChunk"],
    "topK": 20,
    "minVersion": 42,
    "explain": true,
    "expansion": {
      "maxDepth": 2,
      "direction": "both",
      "relationTypes": ["MENTIONS", "RELATED_TO"],
      "maxSeeds": 50,
      "maxVisited": 10000
    }
  }
}
```

retrieval worker 尚未发布包含向量、全文和图索引的完整快照时，执行返回
`retrieval_not_ready`；完整快照低于 `minVersion` 时返回
`index_not_fresh`。

## 响应与错误

执行成功：

```json
{
  "data": {
    "graph": {
      "version": 12,
      "results": [],
      "stats": {"scanned": 0, "visited": 0, "returned": 0, "cost": 0},
      "nextCursor": null
    }
  }
}
```

document、变量和 GraphQL 校验错误返回 HTTP `400` 及 `errors`。查询执行错误
返回 HTTP `200`，将选中的根字段置为 `null`，并在
`errors[].extensions.code` 中返回 GGraphDB 稳定错误码。

## 边界

- 每个请求必须且只能有一个受支持的查询根字段（`graph` 或
  `evidenceSearch`）。
- 支持 query；不支持 mutation 和 subscription。
- 禁用 schema introspection 和 GraphQL subscription。
- 动态实体属性和结果行通过 `JSON` scalar 返回；不会为每个租户生成一套
  强类型 GraphQL schema。
- GraphQL 返回物化 JSON；超大结果应使用 JSON DSL 的 scan/export 或 stream
  入口。

旧 `POST /v1/query/gql` 和 `graphdb gql` 执行 1.0 的 `FIND`/`MATCH`
文本 DSL。它们仅作为兼容入口保留，并不是 GraphQL。见 [gql.md](gql.md)。
