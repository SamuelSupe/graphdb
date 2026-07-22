# GraphDB Usage Manual

[中文](usage-manual.zh-CN.md)

This manual gives the shortest path from creating a tenant and writing general
graph data to querying, optional CMDB governance, maintenance, and
troubleshooting. See the
[OpenAPI contract](../openapi.yaml) for the complete HTTP API and the
[Data Model](data-model.md) for deeper model details.

## 1. Start and configure variables

Local single process:

```sh
go run ./cmd/graphdb serve
```

Docker Compose:

```sh
docker compose up --build
```

Set the API address and tenant:

```sh
export BASE=http://127.0.0.1:8080
export TENANT=demo
export TENANT_HEADER="X-Tenant-ID: $TENANT"
```

All tenant data APIs require `X-Tenant-ID`; JSON requests also require
`Content-Type: application/json`.

## 2. Tenant lifecycle

Create:

```sh
curl -fsS -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

Inspect:

```sh
curl -fsS "$BASE/v1/tenants/demo" -H "$TENANT_HEADER"
curl -fsS "$BASE/v1/tenants" -H "$TENANT_HEADER"
```

CLI equivalents:

```sh
go run ./cmd/graphdb create-tenant demo
go run ./cmd/graphdb tenant demo
go run ./cmd/graphdb list-tenants
```

Disabling, deleting, and purging a tenant are destructive operations. Back up
first, confirm the tenant id, and then use `disable-tenant`,
`delete-tenant`, or `purge-tenant`.

## 3. Write entities and edges

The repository includes an example commit:

```sh
curl -fsS -X POST "$BASE/v1/commits" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-commit-001' \
  --data @examples/commit.json
```

CLI:

```sh
go run ./cmd/graphdb commit demo examples/commit.json
```

For collector ingestion:

```sh
curl -fsS -X POST "$BASE/v1/ingest/batches" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: application/json' \
  --data @examples/ingest-cmdb.json
```

Collectors should reuse `batch_id` and the idempotency key for the same
logical batch. On `429`, honor `Retry-After` and retry with exponential
backoff and jitter; do not generate a new key for the same batch.

## 4. Query

JSON DSL:

```sh
curl -fsS -X POST "$BASE/v1/query" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: application/json' \
  --data @examples/query-match.json
```

GQL:

```sh
curl -fsS -X POST "$BASE/v1/query/gql" \
  -H "$TENANT_HEADER" \
  -H 'Content-Type: text/plain' \
  --data-binary 'FIND person WHERE name = "Alice" LIMIT 10'
```

Common query operations:

- `match`: filter, sort, project, and paginate entities.
- `neighbors`: read incoming, outgoing, or bidirectional neighbors.
- `traverse`: traverse paths with a depth limit.
- `impact`: propagate through relation impact direction.
- `shortest_path`: find the shortest path between two entities.
- `scan` and snapshot export: export current entities or edge collections.

Field filters support `eq`, `neq`, `in`, `exists`, comparisons, prefix,
contains, and fuzzy matching. See [Query Capabilities](../query_capabilities.md)
for the complete structure.

## 5. Read-after-write consistency

The `all` mode normally reads its own writes. In a writer/reader deployment,
pass the write response version to a reader:

```sh
curl -fsS "$BASE/v1/entities/person:alice?min_version=1" \
  -H "$TENANT_HEADER"
```

If a reader has not caught up, it returns retryable `reader_not_fresh`. Use
`allow_stale=true` only when eventual consistency is acceptable:

```sh
curl -fsS "$BASE/v1/entities/person:alice?allow_stale=true" \
  -H "$TENANT_HEADER"
```

## 6. CMDB scenario capabilities (optional)

A CI type can declare field types, required/enum/default/index/unique
constraints. Source policy controls priority when multiple collectors write
the same field. Common relations include `contains`, `runs_on`,
`depends_on`, `owned_by`, and `connects_to`.

Examples:

```sh
go run ./cmd/graphdb commit demo examples/commit-cmdb.json
go run ./cmd/graphdb query demo examples/query-cmdb-host.json
go run ./cmd/graphdb query demo examples/query-cmdb-runs-on.json
go run ./cmd/graphdb set-source-policy demo examples/source-policy.json
go run ./cmd/graphdb source-policy demo
```

## 7. Query templates and indexes

Save and run a template:

```sh
go run ./cmd/graphdb save-query demo examples/query-template-hosts.json
go run ./cmd/graphdb list-queries demo
go run ./cmd/graphdb run-saved-query demo hosts-by-region
```

Maintain indexes:

```sh
go run ./cmd/graphdb index-health demo
go run ./cmd/graphdb index-inspect demo
go run ./cmd/graphdb rebuild-indexes demo
```

HTTP clients can call `POST /v1/indexes/rebuild?async=true` and poll the task
API. Deep health checks use `GET /v1/indexes/health?deep=true`; avoid running
them frequently during peak traffic.

## 8. Maintenance, backup, and troubleshooting

Common maintenance:

```sh
go run ./cmd/graphdb compact demo
go run ./cmd/graphdb gc demo
go run ./cmd/graphdb integrity-audit demo
go run ./cmd/graphdb backup-tenant demo
go run ./cmd/graphdb recover demo
```

Metrics and OpenAPI:

```sh
curl -fsS "$BASE/metrics"
curl -fsS "$BASE/openapi.yaml"
```

Recommended troubleshooting order: check `/v1/health`, object-store
connectivity, bucket/prefix, write backpressure, CAS conflicts, reader
visible version, and index health. For programs, use the response `code`,
`retryable`, and `detail` fields rather than display text. See
[Errors And Troubleshooting](errors-troubleshooting.md).

## 9. SDKs

See [Go And Python SDK](sdk.md) for installation, writes, queries, streaming,
and retry examples. Production collectors should use batch ingestion, stable
idempotency keys, and `Retry-After`, while keeping tenant, source, external
id, and identity key values traceable.
