# GraphDB User Guide

[中文](README.zh-CN.md)

This guide is for service owners, ingest-client authors, and operators using
GraphDB as a general-purpose graph database. CMDB is one supported scenario;
the same entity, relationship, query, and storage APIs can serve other
entity-relationship applications.

## What GraphDB provides

- Multi-tenant graph data isolated by `X-Tenant-ID`.
- Schemaless entities with optional type definitions, including CI types for
  CMDB-style modeling.
- Typed directed edges with `(type, from, to)` canonical identity.
- One active writer per tenant; readers independently reload from object storage.
- Object-storage persistence using Parquet manifests, commits, snapshots, entity
  pages, edge shards, and index objects.
- JSON Query DSL, text GQL, scan/export APIs, saved queries, and running-query
  control.
- Optional source-priority governance for entity fields, edge fields, and edge
  existence.
- Tenant lifecycle, source policy, tenant config, index management, unified
  tasks, maintenance, integrity audit, and reader freshness checks.

## Common HTTP conventions

All tenant-scoped data APIs require:

```http
X-Tenant-ID: <tenant-id>
Content-Type: application/json
```

Read APIs support freshness controls:

- body: `min_version`, `allow_stale`;
- query: `?min_version=123&allow_stale=true`;
- headers: `X-GraphDB-Min-Version`, `X-GraphDB-Allow-Stale`.

Mode behavior:

- `GRAPHDB_MODE=writer`: write/control APIs are enabled; read APIs remain
  available for checks.
- `GRAPHDB_MODE=reader`: write/config/task mutations return `405`; reads,
  queries, scans, metrics, and freshness APIs remain available.
- `GRAPHDB_MODE=all`: local or small single-process mode.

Examples use:

```sh
export WRITER=http://127.0.0.1:38080
export READER=http://127.0.0.1:38081
export BASE=http://127.0.0.1:8080
```

## Document map

- [Quick Start](quickstart.md) · [中文](quickstart.zh-CN.md)
- [Release Deployment](release-deployment.md) · [中文](release-deployment.zh-CN.md)
- [Usage Manual](usage-manual.md) · [中文](usage-manual.zh-CN.md)
- [Data Model](data-model.md) · [中文](data-model.zh-CN.md)
- [Write And Ingest](write-ingest.md) · [中文](write-ingest.zh-CN.md)
- [Read And Query](read-query.md) · [中文](read-query.zh-CN.md)
- [Scan And Export](scan-export.md) · [中文](scan-export.zh-CN.md)
- [Tenant And Config](tenant-config.md) · [中文](tenant-config.zh-CN.md)
- [Deployment And Operations](deploy-ops.md) · [中文](deploy-ops.zh-CN.md)
- [Tasks And Maintenance](tasks-maintenance.md) · [中文](tasks-maintenance.zh-CN.md)
- [Errors And Troubleshooting](errors-troubleshooting.md) · [中文](errors-troubleshooting.zh-CN.md)
- [Go And Python SDK](sdk.md) · [中文](sdk.zh-CN.md)
- [API Map](api-map.md) · [中文](api-map.zh-CN.md)

Reference documents:

- [GQL](../gql.md)
- [Query capabilities](../query_capabilities.md)
- [Error codes](../error_codes.md)
- [OpenAPI](../openapi.yaml)
