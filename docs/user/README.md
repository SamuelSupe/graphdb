# GraphDB User Guide

This guide is for service owners, collector authors, and operators using
GraphDB as an internal CMDB graph database.

## What GraphDB Provides

- Multi-tenant graph data isolated by `X-Tenant-ID`.
- Schemaless entities with optional CI type definitions.
- Typed directed edges with `(type, from, to)` canonical identity.
- Single active writer per tenant; readers are independent and reload from
  object storage.
- Object-storage persistence using Parquet manifests, commits, snapshots,
  entity pages, edge shards, and index objects.
- JSON Query DSL, text GQL, scan/export APIs, saved queries, and running query
  control.
- Source-priority write governance for entity fields, edge fields, and edge
  existence.
- Tenant lifecycle, source policy, tenant config, index management, unified
  tasks, maintenance, integrity audit, and reader freshness checks.

## Common HTTP Conventions

All tenant-scoped data APIs require:

```http
X-Tenant-ID: <tenant-id>
Content-Type: application/json
```

Read APIs support freshness controls:

- Body: `min_version`, `allow_stale`
- Query: `?min_version=123&allow_stale=true`
- Headers: `X-GraphDB-Min-Version`, `X-GraphDB-Allow-Stale`

Mode behavior:

- `GRAPHDB_MODE=writer`: write/control APIs are enabled; read APIs can be used
  for checks.
- `GRAPHDB_MODE=reader`: write/config/task mutation APIs return `405`; read,
  query, scan, metrics, and freshness APIs remain available.
- `GRAPHDB_MODE=all`: local or small single-process mode.

Examples use these shell variables:

```sh
export WRITER=http://127.0.0.1:38080
export READER=http://127.0.0.1:38081
export BASE=http://127.0.0.1:8080
```

## Document Map

- [Quick Start](quickstart.md): start locally, create a tenant, write data, run
  queries.
- [Data Model](data-model.md): CI types, entities, relation types, edges,
  source governance, snapshots.
- [Write And Ingest](write-ingest.md): direct commits, collector ingestion,
  source policy, idempotency, delete semantics, 429 backpressure.
- [Read And Query](read-query.md): entity lookup, JSON DSL, GQL, streaming,
  saved queries, pagination, query kill.
- [Scan And Export](scan-export.md): operational list/export APIs for current
  state extraction.
- [Tenant And Config](tenant-config.md): lifecycle, source policy, quota,
  retention, maintenance and index settings.
- [Deployment And Operations](deploy-ops.md): local, MinIO, RustFS, writer and
  reader deployment, environment variables, readiness.
- [Tasks And Maintenance](tasks-maintenance.md): compact, GC, repair, export,
  backup, restore, index rebuild, task control.
- [Errors And Troubleshooting](errors-troubleshooting.md): stable error
  envelope, common failures, metrics and logs.
- [Go And Python SDK](sdk.md): SDK installation, read/write examples, streaming,
  and retry handling.
- [API Map](api-map.md): endpoint list grouped by domain.

Reference:

- [../gql.md](../gql.md)
- [../query_capabilities.md](../query_capabilities.md)
- [../error_codes.md](../error_codes.md)
- [../openapi.yaml](../openapi.yaml)
