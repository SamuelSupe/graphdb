# GraphQL API

[中文](graphql.zh-CN.md)

GGraphDB exposes GraphQL at `POST /v1/query/graphql`. The endpoint accepts
GraphQL documents, `operationName`, variables, aliases, fragments, and
`@skip`/`@include`, and returns the standard `data`/`errors` response envelope.
The `graph` root uses the existing JSON Query DSL execution model. GGraphDB 1.2
also defines the first-class `evidenceSearch` root for deterministic GraphRAG
retrieval.

## Query

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

The root schema is:

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

`QueryRequest` accepts the fields documented in
[query_capabilities.md](query_capabilities.md). Both JSON-style
`min_version` and GraphQL-style `minVersion` names are accepted for the
documented camel-case fields.

## GraphRAG evidence search

`evidenceSearch` returns a version-consistent evidence package; it does not call
an LLM to generate an answer. The request is bounded to `topK <= 100`, candidate
counts of at most 1,000 per channel, and graph expansion depth `0..2`. Omitted
values default to `topK=20`, 200 vector candidates, 200 lexical candidates, and
two-hop expansion.

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
    "query": "Why is checkout failing?",
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

Until the retrieval worker publishes a complete vector, lexical, and graph
snapshot, execution returns `retrieval_not_ready`. A snapshot below
`minVersion` returns `index_not_fresh`.

## Response and errors

Successful execution:

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

Document, variable, and GraphQL validation errors return HTTP `400` with
`errors`. Query execution errors return HTTP `200`, set the selected root field
to `null`, and include GGraphDB's stable error code under
`errors[].extensions.code`.

## Boundaries

- Exactly one supported query root (`graph` or `evidenceSearch`) is allowed per
  request.
- Query operations are supported; mutations and subscriptions are not.
- Schema introspection and GraphQL subscriptions are disabled.
- Dynamic entity properties and result rows are returned through the `JSON`
  scalar. GGraphDB does not publish a generated strongly typed schema per
  tenant.
- GraphQL uses a materialized JSON response. Use the JSON DSL scan/export or
  stream endpoints for NDJSON.

The old `POST /v1/query/gql` and `graphdb gql` interfaces execute the
1.0 `FIND`/`MATCH` text DSL. They remain compatibility aliases but are not
GraphQL. See [gql.md](gql.md).
