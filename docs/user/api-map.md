# API Map

[中文](api-map.zh-CN.md)

This is a user-facing endpoint map. The detailed schema contract is
[../openapi.yaml](../openapi.yaml).

## System

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/health` | Process health and mode. |
| `GET` | `/metrics` | Prometheus metrics. |
| `GET` | `/openapi.yaml` | OpenAPI contract. |
| `GET` | `/debug/pprof/` | Go runtime profiling index; serves heap, goroutine, block, mutex, and other profiles. |
| `GET` | `/debug/pprof/profile?seconds=30` | CPU profile for the requested duration. |
| `GET` | `/debug/pprof/trace?seconds=1` | Go execution trace for the requested duration. |

## Tenant Lifecycle

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/tenants` | List managed tenants. |
| `POST` | `/v1/tenants` | Create tenant. |
| `GET` | `/v1/tenants/{tenant}` | Get tenant info. |
| `PUT` | `/v1/tenants/{tenant}` | Update tenant metadata. |
| `DELETE` | `/v1/tenants/{tenant}` | Soft delete tenant. |
| `POST` | `/v1/tenants/{tenant}/disable` | Disable tenant writes. |
| `POST` | `/v1/tenants/{tenant}/enable` | Enable tenant. |
| `POST` | `/v1/tenants/{tenant}/purge` | Purge tenant objects. |
| `POST` | `/v1/tenants/{tenant}/clone` | Clone tenant. |
| `POST` | `/v1/tenants/{tenant}/backup` | Start backup task. |
| `POST` | `/v1/tenants/{tenant}/restore` | Start restore task. |
| `POST` | `/v1/tenants/{tenant}/restore-drill` | Start restore drill task. |

## Write And Ingest

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/commits` | Atomic graph mutation commit. |
| `POST` | `/v1/ingest/batches` | Collector ingestion batch. |
| `GET` | `/v1/ingest/collectors/{source}/{collector_id}` | Collector status. |
| `GET` | `/v1/ingest/deadletters/{source}` | List dead letters. |
| `POST` | `/v1/ingest/deadletters/{source}/replay` | Replay dead letters. |
| `GET` | `/v1/source-policy` | Get tenant source policy. |
| `PUT` | `/v1/source-policy` | Update tenant source policy. |
| `GET` | `/v1/tenant-config` | Get tenant config. |
| `PUT` | `/v1/tenant-config` | Update tenant config. |
| `GET` | `/v1/tenant-usage` | Tenant object and byte usage. |

## Read And Query

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/entities/{id}` | Get entity by id. |
| `GET` | `/v1/ci-types` | List CI types. |
| `GET` | `/v1/relation-types` | List relation types. |
| `POST` | `/v1/query` | JSON Query DSL. |
| `POST` | `/v1/query/stream` | JSON Query DSL NDJSON stream. |
| `POST` | `/v1/query/gql` | GQL text query. |
| `POST` | `/v1/query/gql/stream` | GQL NDJSON stream. |
| `GET` | `/v1/queries/running` | List in-process running queries. |
| `DELETE` | `/v1/queries/running/{query_id}` | Cancel running query. |
| `GET` | `/v1/query/templates` | List saved queries. |
| `POST` | `/v1/query/templates` | Save query template. |
| `POST` | `/v1/query/templates/{name}/run` | Run saved query. |

## Scan And Export

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/entities` | Page entities by kind/source/shard. |
| `GET` | `/v1/entities/stream` | Stream entities as NDJSON. |
| `GET` | `/v1/edges` | Page edges by type/from/source/shard. |
| `GET` | `/v1/edges/stream` | Stream edges as NDJSON. |
| `GET` | `/v1/export/snapshot` | Inline current snapshot. |
| `GET` | `/v1/export/snapshot/stream` | Stream snapshot rows or shard refs. |

## Tasks And Maintenance

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/tasks` | List tasks. |
| `POST` | `/v1/tasks` | Start task. |
| `GET` | `/v1/tasks/{task_id}` | Get task. |
| `POST` | `/v1/tasks/{task_id}/cancel` | Cancel task. |
| `POST` | `/v1/tasks/{task_id}/retry` | Retry task. |
| `POST` | `/v1/compact` | Compact snapshot synchronously. |
| `GET` | `/v1/indexes` | Get index catalog. |
| `POST` | `/v1/indexes` | Create secondary index and start rebuild. |
| `GET` | `/v1/indexes/definitions` | List index definitions. |
| `DELETE` | `/v1/indexes/definitions/{name}` | Drop index and start rebuild. |
| `GET` | `/v1/indexes/health` | Index health. |
| `GET` | `/v1/indexes/tasks/{task_id}` | Legacy index task lookup. |
| `POST` | `/v1/indexes/rebuild` | Rebuild indexes. |
| `GET` | `/v1/control/integrity-audit` | Full chain integrity audit. |
| `POST` | `/v1/control/recover` | Recover orphan commits. |
| `POST` | `/v1/control/repair` | Repair dry-run/apply. |
| `POST` | `/v1/control/cleanup-commits` | Cleanup obsolete commits. |
| `POST` | `/v1/control/gc` | GC with checkpoint/dry-run support. |

## Reader And Writer Control

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/control/writer-lease` | Inspect writer lease. |
| `GET` | `/v1/control/reader-freshness` | Reader freshness report. |
| `GET` | `/v1/control/reader-lag` | Compatibility alias for freshness. |
| `GET` | `/v1/control/reader-fleet-readiness` | Fleet readiness report. |
| `GET` | `/v1/control/reader-traffic-gate` | Traffic gate result for deployment checks. |
