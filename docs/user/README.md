# GGraphDB User Guide

Applies to **GGraphDB 2.3.3**; [简体中文](README.zh-CN.md) · [Documentation](../README.md) · [Release validation](../validation-v2.3.3.md)

For service owners, client developers and operators. Start with a complete data
workflow. GGraphDB is a general-purpose property graph database; CMDB is one
supported scenario.

## Follow a complete workflow

1. [Start the service](quickstart.md) and create a tenant in the [console](../management-ui.md) or through the API.
2. Design entities and relationships with the [data model](data-model.md), then [write or ingest](write-ingest.md) data.
3. Read results with [queries](read-query.md) or [scan/export](scan-export.md).
4. Configure [tenant policies](tenant-config.md) and manage runtime state through [tasks and maintenance](tasks-maintenance.md).

## Document map

| Stage | Guides |
| --- | --- |
| First use and browser workflows | [Quick start](quickstart.md) · [Console](../management-ui.md) · [Usage manual](usage-manual.md) |
| Modeling and ingestion | [Data model](data-model.md) · [Write and ingest](write-ingest.md) |
| Queries and integration | [Read and query](read-query.md) · [Scan and export](scan-export.md) · [Go/Python SDKs](sdk.md) |
| Configuration and operations | [Tenants and configuration](tenant-config.md) · [Tasks and maintenance](tasks-maintenance.md) · [Troubleshooting](errors-troubleshooting.md) |
| Installation and deployment | [Release deployment](release-deployment.md) · [Deployment and operations](deploy-ops.md) |
| API reference | [API map](api-map.md) · [OpenAPI](../openapi.yaml) · [Error codes](../error_codes.md) |

Each guide links to its Chinese counterpart at the top. The [documentation index](../README.md#user-guides) lists both languages side by side.

## What GGraphDB provides

| Area | Behavior |
| --- | --- |
| Isolation | `X-Tenant-ID` selects a tenant; directed edges use `(type, from, to)` as canonical identity |
| Modeling and governance | Schemaless entities, optional types and CI types; source priority for entity fields, edge fields and edge existence |
| Query and management | JSON DSL, GraphQL, saved queries, running-query control, indexes, tenant lifecycle and a structured console |
| Persistence | One process owns each local directory; Parquet manifests, commits, snapshots, entity pages, edge shards and indexes persist on disk; direct/WAL supported |
| Expansion and recovery | Optional independent Raft replicas, tenant sharding, S3-compatible snapshots and restore |

Standalone has built-in backup scheduling, retries, retention and drills. Raft
uses externally scheduled cluster APIs. Commits follow the `data_hash` contract;
standalone 2.0/2.1/2.2 directories remain compatible. No 1.x migration is provided.
See [version boundaries](../naming-and-compatibility.md).

## Common HTTP conventions

### Tenant and request headers

Tenant-scoped data APIs use the tenant header; JSON requests also set the content type:

```http
X-Tenant-ID: <tenant-id>
Content-Type: application/json
```

### Read freshness

| Location | Parameters |
| --- | --- |
| JSON body | `min_version`, `allow_stale` |
| URL query | `?min_version=123&allow_stale=true` |
| HTTP headers | `X-GraphDB-Min-Version`, `X-GraphDB-Allow-Stale` |

When a query must observe a write, use the write response's `version` as `min_version`.

### Example addresses and listeners

These three variables name the same process. Examples assume a combined listener:

```sh
export WRITER=http://127.0.0.1:8080
export READER=http://127.0.0.1:8080
export BASE=http://127.0.0.1:8080
```

Only `GRAPHDB_MODE=all` is supported. Separate reader/writer modes, remote online
storage and PostgreSQL coordination are rejected. With `GRAPHDB_ADMIN_ADDR`, send
management requests to that listener or its authenticated gateway route. See
[listener responsibilities](../security-deployment.md#listener-responsibilities).

## Further reading

| Topic | Documentation |
| --- | --- |
| Query languages | [GraphQL](../graphql.md) · [Query capabilities](../query_capabilities.md) · [Legacy text DSL](../gql.md) |
| Clusters | [Raft operations and upgrades](../raft-operations.zh-CN.md) · [Tenant sharding and migration](../sharding.zh-CN.md) |
| Observability | [Diagnostic metrics](../diagnostics-metrics.zh-CN.md) |
