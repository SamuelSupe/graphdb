# GraphDB

GraphDB is a small Go v1 graph database prototype backed by object storage.
It supports multi-tenant prefixes, schemaless entity fields, CMDB CI type
schemas, identity reconciliation, typed relation semantics, batch atomic
commits, manifest compare-and-swap publishing, snapshot compaction, reader
cache, ingestion batches, saved query templates, persisted index metadata with
planner statistics, query admission control, and a JSON query DSL.

User-facing docs start at [docs/user/README.md](docs/user/README.md). API and
query references are in [docs/openapi.yaml](docs/openapi.yaml),
[docs/query_capabilities.md](docs/query_capabilities.md), and
[docs/gql.md](docs/gql.md). Go and Python SDK examples are in
[docs/user/sdk.md](docs/user/sdk.md).

## Architecture

- Writes go through immutable commit envelope objects, then publish a tenant
  Parquet manifest.
- Manifest publishing uses object ETags and conditional writes, so stale writers
  fail instead of overwriting a newer manifest.
- Reads load the tenant manifest, restore the compacted snapshot if present, and replay visible commit objects.
- Tenant manifests and loose commit objects are Apache Arrow-backed Parquet
  objects with payload hashes; JSON commit envelopes and raw commit JSON are no
  longer valid data plane objects.
- Long visible commit tails are folded into immutable Parquet `commit_segments`
  while remaining loose Parquet commit objects are still replayed from the
  manifest tail.
- Compacted snapshot records, sharded snapshot catalog/schema/data, persisted
  indexes, entity pages, and entity by-id records are written as Parquet
  objects; JSON snapshot records are no longer valid data plane objects.
- The HTTP server uses a tenant reader cache and only reloads after
  `GRAPHDB_POLL_INTERVAL` or explicit invalidation after writes.
- Tenant isolation is request-scoped with `X-Tenant-ID`; each tenant maps to `GRAPHDB_PREFIX/tenants/<tenant>`.
- The v1 writer model is single-writer per tenant. There is no distributed transaction coordinator.
- HTTP queries pass through global and per-tenant admission gates before loading
  a snapshot.

## CMDB Model

- `CIType` defines kind-level field specs: type, required, enum, default,
  indexed, and unique.
- `IdentityKey` defines automatic dedupe rules across sources. `strategy=merge`
  merges duplicate identities; `strategy=reject` rejects them.
- Entities carry `source`, `external_id`, `identity_keys`, `confidence`,
  `source_priority`, field-level `field_sources`, and accumulated `sources`.
- Tenant source policies define source priority, so manually curated fields can
  override collector fields while lower-priority collectors are suppressed and
  reported instead of overwriting the value.
- Manual `merge_entities` and `split_entities` mutations are available for
  operator reconciliation workflows.
- Standard relation types are built in: `contains`, `runs_on`, `depends_on`,
  `owned_by`, and `connects_to`. They include reverse names, impact direction,
  and cross-kind semantics.

## Run Locally

Local file storage is the default:

```sh
go run ./cmd/graphdb serve
```

Object storage mode with MinIO:

```sh
docker compose up --build
```

If local ports are already taken, override the exposed ports:

```sh
MINIO_API_PORT=29000 MINIO_CONSOLE_PORT=29001 GRAPHDB_PORT=28080 docker compose up --build
```

## CLI

```sh
go run ./cmd/graphdb init-tenant demo
go run ./cmd/graphdb commit demo examples/commit.json
go run ./cmd/graphdb query demo examples/query-match.json
go run ./cmd/graphdb query demo examples/query-neighbors.json
go run ./cmd/graphdb commit demo examples/commit-cmdb.json
go run ./cmd/graphdb query demo examples/query-cmdb-host.json
go run ./cmd/graphdb query demo examples/query-cmdb-runs-on.json
go run ./cmd/graphdb query demo examples/query-cmdb-enhanced.json
go run ./cmd/graphdb query demo examples/query-explain.json
go run ./cmd/graphdb query demo examples/query-cmdb-impact.json
go run ./cmd/graphdb query demo examples/query-cmdb-shortest-path.json
go run ./cmd/graphdb ingest demo examples/ingest-cmdb.json
go run ./cmd/graphdb collector-status demo aws collector-a
go run ./cmd/graphdb set-source-policy demo examples/source-policy.json
go run ./cmd/graphdb source-policy demo
go run ./cmd/graphdb set-tenant-config demo examples/tenant-config.json
go run ./cmd/graphdb tenant-config demo
go run ./cmd/graphdb deadletters demo aws
go run ./cmd/graphdb replay-deadletters demo aws 10
go run ./cmd/graphdb save-query demo examples/query-template-hosts.json
go run ./cmd/graphdb list-queries demo
go run ./cmd/graphdb run-saved-query demo hosts-by-region
go run ./cmd/graphdb rebuild-indexes demo
go run ./cmd/graphdb index-catalog demo
go run ./cmd/graphdb index-inspect demo
go run ./cmd/graphdb index-health demo
go run ./cmd/graphdb writer-lease demo
go run ./cmd/graphdb recover demo
go run ./cmd/graphdb repair demo
go run ./cmd/graphdb repair demo --apply
go run ./cmd/graphdb cleanup-commits demo
go run ./cmd/graphdb compact demo
```

## Load Test

`tools/loadtest` exercises the full local HTTP path: health check, schema
commit, concurrent ingestion batches, match/traverse/stream queries, saved
query execution, index rebuild, index catalog, and collector status. Reader
traffic also reuses the latest write response version for `min_version` query,
stream, and entity lookup checks, while separately exercising
`allow_stale=true` list/query reads.

```sh
go run ./tools/loadtest \
  -base http://127.0.0.1:28080 \
  -reader-base http://127.0.0.1:28081 \
  -tenant loadtest \
  -writers 8 \
  -readers 16 \
  -batches 20 \
  -batch-size 200 \
  -http-timeout 2m \
  -maintenance-timeout 10m
```

`batch-size` is the number of host/service groups per batch; each group writes
one host, one service, and one edge. Keep collector batches in the 200-500
group range before adding writer concurrency; smaller batches amplify commit,
manifest, idempotency, and collector metadata object writes. v1 keeps one
in-process writer hot cache per tenant to avoid replaying object-storage commit
tails on every write. This matches the product boundary of one active writer
process per tenant. The object-storage lease and manifest CAS are
duplicate-writer guards, not a multi-writer coordination layer.

For overload tests, add `-allow-write-backpressure`; write-side `429` responses
are then counted in status totals as expected load shedding instead of failing
the run. Post-load maintenance uses `-maintenance-timeout` because large
RustFS-backed index rebuilds can legitimately exceed ordinary request latency.
For reader slow-object-read coverage, start the RustFS stack with
`GRAPHDB_FAULT_OBJECT_READ_DELAY=25ms`; the load test will then exercise the
same `min_version` and `allow_stale` read paths through delayed object fetches.

## Long Soak Test

`tools/soaktest` is the long-running production-readiness workload. It keeps
writing CMDB-shaped data, runs match/profile, range, aggregation, fuzzy,
neighbors, impact, shortest path, path-filter traverse, stream, scan, and saved
query templates, and periodically runs compact, GC, index rebuild, reader
freshness, reader fleet readiness, tenant usage, and index health sampling.

```sh
SOAK_DURATION=24h scripts/soak_rustfs.sh
SOAK_DURATION=72h SOAK_TENANT=soak-rustfs-72h scripts/soak_rustfs.sh
```

For unattended runs, start it in the background and inspect the latest run:

```sh
SOAK_DURATION=24h \
SOAK_MIN_DURATION=24h \
SOAK_REQUIRE_READER_RESTART=true \
SOAK_REQUIRED_OPERATIONS=profile-indexed-match,indexed-match-min-version,indexed-match-allow-stale,range-aggregate-match,fuzzy-service-match,cmdb-service-to-database-path,impact-service,stream-large-indexed-hosts-min-version,scan-entities-min-version,scan-entities-allow-stale,scan-edges-allow-stale,export-snapshot-stream,saved-service-impact \
scripts/soak_start_rustfs.sh
scripts/soak_status.sh
```

`scripts/soak_status.sh` prints the run container state, event counts for
warmup/query/write/maintenance/restart/done markers, and a partial soakreport.
The default status warmup is `45m`, matching the official gate window; before
that point the full query matrix is expected to be missing because
`SOAK_QUERY_START_DELAY` is still warming the reader. Use
`SOAK_COMPOSE_PROJECT` plus custom `GRAPHDB_PORT`, `GRAPHDB_READER_PORT`, and
`RUSTFS_API_PORT` when running an isolated long soak beside an existing local
stack.

The soak scripts use conservative OrbStack defaults (`SOAK_WRITERS=1`,
`SOAK_READERS=4`, `SOAK_BATCH_SIZE=10`) and delay reader queries for
`SOAK_QUERY_START_DELAY` (default `35m`) so compact and index rebuild can
establish a readable baseline before correctness pressure begins. During that
delay, one warmup reader issues a light indexed query every
`SOAK_READER_WARMUP_INTERVAL` (default `2s`) with
`SOAK_READER_WARMUP_TIMEOUT` (default `2m`) to avoid a cold reader stampede when
the query workers start. `SOAK_READER_MAX_STALENESS` (default `10m`) controls
the soak gate for eventually consistent indexed reads, and should stay aligned
with the index rebuild cadence. The soak HTTP client uses `SOAK_HTTP_TIMEOUT`
(default `2m`) so large scan/stream checks can run without hiding query-level
`timeout_ms` failures. Query, stream, and scan cases include dedicated
`min_version` checks based on the latest successful write version; under random
LB they must either reach that version or return retryable `reader_not_fresh`
with `Retry-After`. Separate allow-stale cases verify explicitly eventual reads.
Compact, GC, and index rebuild are serialized by the soak runner, run through
the unified task API, and use `SOAK_MAINTENANCE_TIMEOUT` (default `20m`).
Snapshot export is still exercised, but
`SOAK_SNAPSHOT_EXPORT_INTERVAL` (default `5m`) keeps it from continuously
occupying reader/object-store bandwidth. Increase those knobs only after a clean
24h baseline. `SOAK_REQUIRED_OPERATIONS` is optional, but
should be set for official 24h/72h evidence so `soakreport` fails if critical
query, stream, scan, export, or saved-query paths never ran after warmup. For
official evidence, set `SOAK_MIN_DURATION` to the target run length and
`SOAK_REQUIRE_READER_RESTART=true` so short or non-reload runs cannot pass.

For a quick smoke run:

```sh
SOAK_DURATION=30s \
SOAK_SAMPLE_INTERVAL=5s \
SOAK_COMPACT_INTERVAL=10s \
SOAK_GC_INTERVAL=15s \
SOAK_INDEX_REBUILD_INTERVAL=20s \
SOAK_READER_RESTART_INTERVAL=0 \
scripts/soak_rustfs.sh
```

The script writes NDJSON events to `soak-<tenant>.ndjson`, then runs
`tools/soakreport` to fail the run on error events, missing compact/GC/index
rebuild, excessive final commit tail, too many unhealthy index samples, or too
much reader fleet unready time after warmup. Use `usage_sample` to plot object
count, total bytes, category bytes, manifest version, snapshot version, and
commit tail; use `index_catalog_sample` and `index_health_sample` to track
index growth, catalog freshness, and health.

When `SOAK_READER_RESTART_INTERVAL` is enabled, the script restarts the direct
reader endpoint and pauses reader checks for `SOAK_READER_RESTART_GRACE`
(default `45s`). In production, traffic should be routed by fleet readiness
rather than sent to a restarting reader instance.

To analyze an existing run:

```sh
scripts/soak_final_report.sh soak-runs/<run-id>
```

## OrbStack RustFS Verification

Run the RustFS-backed writer/reader stack:

```sh
docker compose -p graphdb-rustfs -f docker-compose.rustfs.yml up -d --build
```

Run the black-box HTTP e2e check against the deployed writer and reader:

```sh
go run ./tools/e2echeck \
  -writer http://127.0.0.1:38080 \
  -reader http://127.0.0.1:38081 \
  -timeout 3m
```

The check creates a fresh tenant and covers reader write rejection, source
policy, atomic commits, cold reader `min_version` catch-up, `allow_stale`,
retryable `reader_not_fresh`, reader visibility, tenant isolation, source
priority suppression, match/neighbors/traverse/shortest path queries, ingestion
idempotency, partial failure dead-lettering, index rebuild, lazy query
pagination, stream pagination, compaction, and writer control endpoints.

Run the multi-reader cold-start check with slow object reads:

```sh
scripts/rustfs_reader_stateless_check.sh
```

It starts an isolated RustFS compose project with three reader processes,
injects `GRAPHDB_FAULT_OBJECT_READ_DELAY` into readers, writes a new version,
then concurrently asks each cold reader to serve the same `min_version` entity
and query request from object storage. It also verifies that each reader exposes
catch-up and object-store latency metrics.

## HTTP API

```sh
curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/commit.json \
  http://localhost:8080/v1/commits

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/entities/person:alice

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/entities?kind=host&source=aws&limit=100'

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/entities/stream?kind=host'

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/edges?type=runs_on&from_shard=73&limit=100'

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/edges/stream?type=runs_on'

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/export/snapshot

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/export/snapshot/stream

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/export/snapshot/stream?inline=true'

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/ci-types

curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -X PUT \
  --data @examples/source-policy.json \
  http://localhost:8080/v1/source-policy

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/source-policy

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/tenant-usage

curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -X PUT \
  --data @examples/tenant-config.json \
  http://localhost:8080/v1/tenant-config

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/tenant-config

curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-neighbors.json \
  http://localhost:8080/v1/query

curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/ingest-cmdb.json \
  http://localhost:8080/v1/ingest/batches

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/ingest/collectors/aws/collector-a

curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-explain.json \
  http://localhost:8080/v1/query

curl -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  --data @examples/query-template-hosts.json \
  http://localhost:8080/v1/query/templates

curl -H 'X-Tenant-ID: demo' \
  -X POST http://localhost:8080/v1/query/templates/hosts-by-region/run

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/queries/running

curl -H 'X-Tenant-ID: demo' \
  -X DELETE http://localhost:8080/v1/queries/running/<query-id>

curl -H 'X-Tenant-ID: demo' \
  -X POST http://localhost:8080/v1/indexes/rebuild

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/indexes/health

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/indexes/health?deep=true'

curl -H 'X-Tenant-ID: demo' \
  -X POST 'http://localhost:8080/v1/indexes/rebuild?async=true'

curl -H 'X-Tenant-ID: demo' \
  http://localhost:8080/v1/indexes/tasks/<task-id>

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/control/reader-fleet-readiness?min_ready=2&max_staleness_ms=30000'

curl -H 'X-Tenant-ID: demo' \
  'http://localhost:8080/v1/control/reader-traffic-gate'

curl http://localhost:8080/metrics

curl http://localhost:8080/openapi.yaml
```

`/v1/export/snapshot/stream` defaults to an object-store friendly refs stream
when the tenant has a sharded snapshot catalog: it returns schema, entity page,
and edge shard object references instead of materializing every row through the
HTTP response. Use `?inline=true` for row-by-row NDJSON export.

Write endpoints can return `429 Too Many Requests` when the writer is under
pressure. The JSON body includes `retry_after_ms` and machine-readable reasons
such as `object_store_latency_high`, `manifest_cas_conflicts_high`,
`index_rebuild_running`, `gc_running`, `commit_tail_too_long`, or tenant quota violations.
The response also sets the `Retry-After` header.

Default write admission is tuned for object storage: one active writer per
tenant, up to 32 global writes, a 2s queue wait, object-store latency shedding at
2s, CAS conflict shedding at 5 conflicts per 30s, and a commit-tail limit of
300. Collectors should treat `429` as retryable, honor `Retry-After`, preserve
the original idempotency key, and use exponential backoff with jitter. A
`commit_tail_too_long` response means the tenant needs compact/maintenance to
catch up; `index_rebuild_running` and `gc_running` normally clear when the
background task finishes.

Errors use a stable JSON envelope: `error` is kept for compatibility, and new
clients should read `code`, `message`, `retryable`, and optional `detail`.
The OpenAPI contract is in `docs/openapi.yaml` and is served at
`GET /openapi.yaml`.

## Query DSL

- `explain`: returns a planner result for `target_op` without running it.
- `profile`: runs `target_op` and includes the selected plan and runtime stats.
- `match`: entity lookup by `kind`, legacy equality `filters`, structured `where`, `sort`, `project`, `aggregate`, `limit`, `cursor`.
- `neighbors`: adjacent entities by `id`, `direction` (`out`, `in`, `both`), `relation_type` or `relation_types`, plus `where` filtering on neighbor entities.
- `traverse`: bounded path search by `id`, `depth`, direction/relation filters, and `path` constraints.
- `impact`: path search that follows each relation type's `impact_direction`.
- `shortest_path`: BFS shortest path from `id` to `target_id`.

Structured `where` supports `eq`, `neq`, `in`, `exists`, `gt`, `gte`, `lt`,
`lte`, `prefix`, `contains`, and `fuzzy`. Field names can target entity
metadata (`id`, `kind`, `source`, `external_id`, `confidence`) or schemaless
fields (`hostname` or `fields.hostname`).

Query responses include the graph snapshot `version`, stable cursor token,
basic `stats`, optional aggregates, optional operator-level `profile`, and an
optional query `plan`. `timeout_ms` and `cost_limit` bound expensive graph
walks. Scalar `match` equality filters use the planner-selected entity id
lookup or field index before exact filtering. The planner uses persisted index
catalog statistics when they match the loaded snapshot version, otherwise it
falls back to runtime index counts. The executor performs admission control from
estimated cost before running, and path queries prune disallowed node kinds and
impossible final nodes during BFS.

Each in-flight query receives an `X-GraphDB-Query-ID` response header. Operators
can list current in-process tenant queries with `GET /v1/queries/running` and
cancel one with `DELETE /v1/queries/running/{query-id}`; cancellation propagates
through the query context and returns the existing request-canceled envelope.
`/v1/query/stream` returns newline-delimited JSON. Indexed lazy `match` streams
write an opening metadata row, result rows as they are materialized, then a
final `done=true` row with `stats` and `next_cursor`; other queries keep the
legacy metadata row followed by result rows.

Current-state scan APIs are meant for operations and export, not graph
planning. `GET /v1/entities` supports `kind`, `source`, `shard`, `limit`, and
`cursor`; `GET /v1/edges` supports `type`, `from`, `from_shard`, `source`,
`limit`, and `cursor`. The `/stream` variants return NDJSON and page internally
over the same stable cursor. Read APIs accept `min_version` or
`X-GraphDB-Min-Version`; callers can pass the write response version to get
write-after-read behavior behind a plain load balancer. Readers first try to
catch up from object storage within `GRAPHDB_READER_CATCHUP_TIMEOUT`; if they
cannot, they return retryable `reader_not_fresh` with `Retry-After`,
`visible_version`, and `required_version`. `allow_stale=true` or
`X-GraphDB-Allow-Stale: true` permits a cached visible version when no
`min_version` is provided. When a persisted index catalog satisfies the required
version, scans read entity pages and edge shards directly; otherwise they fall
back to the current graph snapshot.

`GET /v1/tenant-usage` scans the tenant object prefix and reports object count,
total bytes, per-category bytes for commits, snapshots, indexes, ingestion,
dead-letters, task records/results, reader heartbeats, and the currently
configured retention policy.

## Ingestion

`POST /v1/commits` accepts direct atomic mutations. Set optional
`idempotency_key` when the caller may retry the same commit request; replaying
the same key and body returns the stored result with `idempotent_replay=true`.
Reusing a key with a different body is rejected. Direct commit idempotency
records are stored as Parquet objects.

`POST /v1/ingest/batches` accepts collector-oriented batches with `source`,
`collector_id`, `batch_id`, optional `idempotency_key`, optional `cursor`, and
mixed CI type, entity, relation type, or edge items. Valid items are committed
as one mutation batch while invalid items are returned in `failures`; a partial
batch returns HTTP `207`. Replaying an `idempotency_key` returns the previous
result with `skipped=true`. Ingest batch records, ingest idempotency records,
collector status, and dead-letter records are stored as Parquet control objects.

If a tenant source policy is configured, ingestion resolves each entity and
edge's effective `source_priority` from that policy before writing the immutable
commit. Lower-priority field updates are counted as `suppressed`, returned in
`conflicts`, and do not mark the batch failed or create a dead-letter record.

Edges use `(type, from, to)` as their canonical identity. Incoming collector
edge ids are stored as source aliases, so two collectors writing the same
relationship converge to one canonical edge. Collector batches can include
`delete_edge` items for source-aware deletes; low-priority deletes are reported
as suppressed conflicts instead of failures. The legacy `delete_edges` mutation
remains an administrator force-delete path and accepts canonical ids or source
aliases.

Collector progress is stored per tenant/source/collector and can be read with
`GET /v1/ingest/collectors/{source}/{collector_id}`.

Failed ingestion batches are stored in a per-source dead-letter queue:

- `GET /v1/ingest/deadletters/{source}` lists pending/resolved records.
- `POST /v1/ingest/deadletters/{source}/replay?limit=10` replays pending
  batches with a fresh batch id and no idempotency key.

## Write Reliability

Each tenant has an object-storage writer lease at
`tenants/<tenant>/control/writer-lease.parquet`. The production model is one
active writer process per tenant; the lease rejects accidental duplicate or
stale writers. If the active writer is replaced, the replacement process can
acquire the lease after the previous lease expires. Manifest publishing uses
conditional writes and retries conflicts, leaving any unpublished commit objects
for recovery.

Control endpoints:

- `GET /v1/control/writer-lease`
- `GET /v1/control/reader-freshness`
- `GET /v1/control/reader-lag` compatibility alias
- `GET /v1/control/reader-fleet-readiness`
- `GET /v1/control/reader-traffic-gate`
- `POST /v1/control/recover`
- `POST /v1/control/repair`
- `POST /v1/control/cleanup-commits`
- `POST /v1/control/profiling`

`reader-freshness` reports writer manifest version, reader-visible version,
version lag, lag age, cache state, and commit tail replay status. Readers return
`status=fresh` when the visible version matches the writer manifest, `stale`
when a cached graph is behind, `cold` before any tenant graph has been loaded,
and `loading` while a reload is in progress. `reader-fleet-readiness` writes the
current reader heartbeat as a Parquet object under the tenant control prefix,
then lists all reader heartbeats to answer whether at least `min_ready` readers
can serve `min_version` within `max_staleness_ms`. `reader-traffic-gate` is the
deployment-facing local gate: it returns HTTP 200 with `serve_traffic=true`
only when this reader can serve the tenant target version. Lagging, cold, or
loading readers return HTTP 503 with `serve_traffic=false` and a reason such as
`version_lag` or `not_loaded`, so load balancers or sidecars can drain the
instance and automatically add it back when the same endpoint returns 200. By
default the gate is strict against the current writer manifest and attempts
`LoadAtLeast(target_version)` for a lagging cached reader before deciding; use
`refresh=false` for a pure status probe, and use
`allow_stale=true&max_staleness_ms=...` only for explicitly bounded stale-read
deployments.

Recovery scans unreferenced commit objects and publishes only the next
contiguous version after the current manifest. Cleanup deletes unreferenced
commit objects whose version is not newer than the manifest, and keeps future
orphans for recovery review.

Object records carry a top-level `layout_version`. Missing layout versions are
treated as legacy layout v1, current writes use layout v2, and unknown future
versions are rejected before loading. `repair` defaults to dry-run and reports
legacy layout objects, alias conflicts, duplicate CI identities, edge endpoint
issues, stale source ownership, and index/entity-page inconsistencies. With
`--apply` or `{"apply":true}`, it runs only deterministic repairs: rebuild a
missing/corrupt manifest from valid snapshots and contiguous commits, recreate
tenant metadata, rebuild the tenant registry, compact the normalized current
snapshot, rebuild index catalog/entity pages/by-id records/edge shards, and
clean obsolete index objects. Repair dry-runs include an action plan, repair
tasks checkpoint each running/applied action, and applied repairs include a
post-repair integrity audit report. `GET /v1/control/integrity-audit` performs
the same manifest -> snapshot catalog -> snapshot schema/pages/shards -> index
catalog -> index objects -> entity by-id record audit as a read-only operation.

## Tenant Lifecycle

Tenants are still isolated by `X-Tenant-ID` and object prefix, but product
operators can manage explicit lifecycle metadata:

- `GET /v1/tenants` lists managed tenants from the lifecycle registry. Use
  `include_legacy=true` only for one-off migration discovery; it scans tenant
  prefixes and can be slow on large buckets.
- `POST /v1/tenants` creates tenant metadata and initializes the manifest.
  The create body can also include default `config` and `source_policy`
  templates; they are written atomically with tenant metadata.
- `GET /v1/tenants/{tenant-id}` returns status, metadata, manifest version,
  snapshot version, and commit tail length.
- `PUT /v1/tenants/{tenant-id}` replaces display metadata (`name`,
  `description`, `labels`, `metadata`).
- `POST /v1/tenants/{tenant-id}/disable` blocks ordinary writes while keeping
  reads and control operations available.
- `POST /v1/tenants/{tenant-id}/enable` re-enables a disabled or soft-deleted
  tenant.
- `DELETE /v1/tenants/{tenant-id}` soft-deletes a tenant. Ordinary reads and
  writes return `tenant_deleted`, but data remains under the prefix.
- `POST /v1/tenants/{tenant-id}/purge` permanently deletes all tenant objects;
  without `force=true`, the tenant must be soft-deleted first.
- `POST /v1/tenants/{tenant-id}/clone` clones the current graph snapshot into a
  new tenant and copies source policy / tenant config. It does not byte-copy
  old commit history.
- `POST /v1/tenants/{tenant-id}/backup` starts a `tenant_backup` task. The
  task result includes `backup_key` and `backup_manifest_key`. The manifest is
  an independent Parquet object that lists snapshot/index/entity page/edge shard
  objects with bytes, SHA256, row counts, content hashes, and schema hashes.
- `POST /v1/tenants/{tenant-id}/restore` starts a `tenant_restore` task from
  `{"backup_key":"..."}`. Prefer the `backup_manifest_key` from backup results;
  legacy backup result keys remain readable. Restore targets must be empty
  unless `{"overwrite":true}` is set. `{"dry_run":true}` validates backup
  objects and reports whether the target exists without writing.
- `POST /v1/tenants/{tenant-id}/restore-drill` starts a
  `tenant_restore_drill` task. If `backup_key` is omitted it first creates a
  fresh backup, restores it into an isolated drill tenant under
  `GRAPHDB_PREFIX/restore-drills/<task-id>` by default, runs integrity audit,
  saved query templates, and usage comparison, then writes a recoverability
  proof in the task result. Set `cleanup=false` to keep the restored drill
  tenant for inspection, or pass `target_prefix` / `target_tenant_id` to use a
  fixed drill destination.

For same-tenant cross bucket/prefix migration, use:

```bash
go run ./tools/tenantmigrate -tenant tenant-a \
  -source-storage s3 -source-prefix graphdb \
  -source-s3-endpoint http://127.0.0.1:9000 -source-s3-bucket source \
  -source-s3-access-key-id minio -source-s3-secret-access-key minio123 \
  -target-storage s3 -target-prefix graphdb-dr \
  -target-s3-endpoint http://127.0.0.1:9000 -target-s3-bucket target \
  -target-s3-access-key-id minio -target-s3-secret-access-key minio123 \
  -dry-run
```

Tenant renames must use backup/restore because tenant IDs are embedded inside
objects.

CLI equivalents include `list-tenants [--include-legacy]`, `create-tenant`, `tenant`,
`set-tenant-metadata`, `disable-tenant`, `enable-tenant`, `delete-tenant`,
`purge-tenant`, `clone-tenant`, `backup-tenant`, and `restore-tenant`.

## Indexes

The reader keeps in-memory indexes rebuilt from commits or snapshots. For
larger tenants, `POST /v1/indexes/rebuild` materializes a persisted index
catalog, field secondary index objects for indexed/unique CI fields, relation
edge shard objects, and entity page/by-id objects. The catalog includes entry
count, distinct value count, top-value statistics for indexed fields, edge shard
counts, entity page counts, object keys, `format`, `codec`, `row_count`,
`content_hash`, and `schema_hash`. `GET /v1/indexes` returns the latest
catalog.

Entity pages and edge shards use 64 hash buckets by default to reduce Parquet
small-object pressure on S3/RustFS. Readers can still consume older 256-bucket
catalogs until the next rebuild publishes the compacted shard layout. Small
entity pages and edge shards may also share one physical Parquet pack object
while the catalog keeps their logical page/shard entries separate.
Non-empty field secondary indexes are published as shard-only Parquet objects:
the legacy full `postings` object is only used for empty indexes or older
catalogs. Small logical field shards are packed into shared physical objects,
and hot string shards split to longer prefixes once they exceed the target row
count. A catalog may therefore contain multiple logical shard entries pointing
at the same Parquet object; readers de-duplicate physical object reads and use
the shard roles to route equality, range, and prefix scans.

Persisted index definitions, catalog, and data are written as Apache
Arrow-backed Parquet objects for definitions, the catalog, secondary indexes,
edge shards, entity pages, and entity by-id records. Sharded snapshot
catalog/schema/data also use Parquet. Non-Parquet persisted-index objects are
not interpreted as data plane objects; rebuild, health, GC, and cleanup manage
Parquet index objects only.
Readers fall back to the existing graph read path when an index object is stale,
missing, corrupt, or hash-mismatched. The
`index-inspect` CLI command prints object-level summaries for debugging Parquet
objects and commit segment hashes.

When the persisted catalog matches the tenant manifest, query execution pushes
eligible `match` equality filters into persisted field indexes and reads
explicit outbound `neighbors` expansion from relation edge shards. Matching
entities are materialized lazily from by-id objects; projected `match` and
neighbor queries request only the fields required for filtering and output.
Range/fuzzy match queries with a `kind` can lazy-scan entity pages instead of
loading the full graph, and first-page sorted/aggregate match queries keep only
the requested top-N result buffer while accumulating aggregates over the full
matched set.
Simple entity/edge upsert and delete commits incrementally update affected
secondary indexes, edge shards, entity pages, and by-id objects; schema,
relation, merge, or split commits mark the catalog stale until rebuild.

Index operations:

- `GET /v1/indexes/health`: bounded manifest/catalog freshness check; reports `ready`, `missing`, `stale`, or `error`.
- `GET /v1/indexes/health?deep=true`: full index object and content validation for repair/debug workflows.
- `GET /v1/control/integrity-audit`: full current-state object-chain audit; CLI equivalent is `graphdb integrity-audit <tenant-id> [--shallow]`.
- `POST /v1/indexes/rebuild`: runs a synchronous Parquet rebuild.
- `POST /v1/indexes/rebuild?async=true`: starts an online background rebuild.
- `GET /v1/indexes/tasks/{task-id}`: returns background rebuild status.

Unified operational tasks:

- `POST /v1/tasks` starts an async task. Supported `type` values are
  `compact`, `gc`, `repair`, `export_snapshot`, `replay_deadletters`, and
  `index_rebuild`, `tenant_backup`, `tenant_restore`, and
  `tenant_restore_drill`.
- `GET /v1/tasks?type=...&status=...` lists current and historical tasks.
  Legacy index rebuild tasks from `/v1/indexes/tasks/{task-id}` are visible in
  this unified list as `type=index_rebuild`.
- `GET /v1/tasks/{task-id}` returns task status, progress, params, result
  metadata, checkpoint, and any failure message. Running tasks update phase and
  checkpoint in the task object itself.
- Long task checkpoints include an `actions` array. Each action records a
  stable action id, status, updated time, selected input/output object keys, and
  verification details. `compact`, `export_snapshot`, `tenant_backup`, and
  `tenant_restore` use these action checkpoints so retries can skip completed
  sub-steps when their output objects still validate.
- `POST /v1/tasks/{task-id}/cancel` marks a queued/running task as canceled and
  cancels the in-process execution when the active writer owns it. The persisted
  status is retained so a restarted or replacement single writer can observe the
  cancel before resuming work at context-aware operation boundaries.
- `POST /v1/tasks/{task-id}/retry` starts a new task from a failed/canceled task.
  For paused GC and deadletter replay tasks, retry carries checkpoint
  `next_cursor` into the new task params as `cursor`. Retry also carries the
  prior task checkpoint so compact/export/backup/restore/repair can skip
  completed sub-steps when their checkpointed objects still validate.
- CLI equivalents: `graphdb start-task <tenant-id> <type> [params.json]`,
  `graphdb list-tasks <tenant-id> [type] [status]`, and
  `graphdb task <tenant-id> <task-id>`, `graphdb cancel-task <tenant-id>
  <task-id>`, `graphdb retry-task <tenant-id> <task-id>`. Tenant backup and
  restore helpers include `graphdb backup-tenant <tenant-id>`,
  `graphdb restore-tenant <tenant-id> <backup-key>`, and
  `graphdb restore-drill-tenant <tenant-id> [params.json]`.

Task records, index rebuild task records, task result objects, and reader
heartbeats are stored as Parquet control objects.

Error contract:

- Error responses use a stable envelope:
  `{"error":"...","code":"...","message":"...","retryable":false}`.
- Product-level codes are frozen in `docs/error_codes.md` and mirrored in
  `docs/openapi.yaml`. They include
  `tenant_required`, `invalid_tenant`, `operation_disabled`, `tenant_disabled`,
  `tenant_deleted`, `quota_exceeded`, `lease_held`, `manifest_cas_conflict`,
  `object_write_conflict`, `object_store_unavailable`, `index_stale`,
  `reader_not_fresh`, `task_conflict`, `repair_required`, `version_conflict`,
  `idempotency_conflict`, `commit_tail_too_long`, `index_rebuild_running`,
  `maintenance_task_running`, and `write_admission_queue_timeout`.
- Write backpressure keeps detailed `reasons[]` for precise automation while
  the top-level `code` is normalized to the product category.

## Observability

- `GET /metrics` exposes Prometheus text metrics for HTTP requests, query
  latency, slow query counts, reader visible versions, request-time catch-up,
  reader cache hit/miss events, read admission, write backpressure, object
  store latency, manifest CAS conflicts, commit tail length, suppressed
  conflict counts, ingest suppression, and the latest known index health
  status.
- HTTP access logs, write/control audit events, ingest results, index rebuild
  events, and slow query logs are emitted as JSON lines to stdout.
- `GRAPHDB_OTLP_ENDPOINT` enables OTLP/HTTP tracing. Leave it empty to keep
  tracing no-op. Use `GRAPHDB_OTLP_INSECURE=true` for plain HTTP collectors.
  `POST /v1/commits` emits child spans for write admission, request decoding,
  commit execution, graph load, commit-tail replay, manifest CAS write, index
  update, and object-store operations so slow writes can be broken down by
  phase.
- `DD_PROFILING_ENABLED=true` starts the Datadog continuous profiler for the
  `serve` command. It initializes only `dd-trace-go/v2/profiler`; it does not
  import or start the Datadog tracer. `DD_SERVICE`, `DD_ENV`, and `DD_VERSION`
  set the profile's service identity. Datadog Agent or agentless upload settings
  continue to use the standard `DD_*` variables.
- `POST /v1/control/profiling` accepts `{"enabled":true|false}` to control the
  profiler for the current process. Enabling sets that process's
  `DD_PROFILING_ENABLED=true` before starting the profiler; disabling stops it
  and sets the value to `false`. This does not modify the deployment environment
  after a restart. Restrict this process-level control endpoint to trusted
  operators at the network boundary.
- Go runtime profiles are available through the standard `net/http/pprof`
  endpoints: `GET /debug/pprof/`, `GET /debug/pprof/heap?gc=1`,
  `GET /debug/pprof/profile?seconds=30`, and
  `GET /debug/pprof/trace?seconds=1`. These process-wide endpoints can expose
  stack traces, command-line arguments, and memory details, so expose them only
  on a trusted operator network.
- `GRAPHDB_SLOW_QUERY_THRESHOLD` controls slow query logging. Set it to `0` to
  disable slow query classification.
- `GRAPHDB_INDEX_HEALTH_INTERVAL` controls background health sampling for
  tenants observed by the process. Set it to `0` to disable background sampling.
- `GRAPHDB_MAINTENANCE_INTERVAL` controls writer-side automatic maintenance.
  Set it to `0` to disable the loop. Tenants without explicit config use
  conservative defaults: auto compact at a 240-commit tail, index rebuild when
  missing/stale, GC every 30 minutes, keep two snapshots, and cleanup index
  orphans. Per-tenant `maintenance.auto_compact`,
  `maintenance.gc_interval_seconds`, retention fields such as
  `maintenance.deadletter_max_age_seconds` and
  `maintenance.task_max_age_seconds`, `indexes.auto_rebuild`, and
  `indexes.rebuild_on_stale` decide which actions run. Maintenance can also
  compact small-file-heavy tenants via `maintenance.small_file_object_threshold`
  plus `maintenance.small_file_bytes_threshold`, and report page/shard layout
  pressure with `maintenance.entity_page_*_threshold` and
  `maintenance.edge_shard_*_threshold`.
- `GRAPHDB_READER_INDEX_CACHE_ENTRIES` controls the in-process LRU cache for
  Parquet entity pages, edge shards, and secondary index shards. Cache keys
  include tenant, catalog version, object key, and catalog content hash.
- `GRAPHDB_READER_CATCHUP_TIMEOUT` controls the per-request reader catch-up
  window for `min_version` reads. The default is `2s`.
- `GRAPHDB_READ_MAX_CONCURRENT`, `GRAPHDB_READ_MAX_PER_TENANT`, and
  `GRAPHDB_READ_QUEUE_TIMEOUT` protect non-query read APIs such as scan, entity
  lookup, and snapshot export. Query APIs keep using the query admission
  controls.
- `GRAPHDB_READ_OBJECT_MAX_CONCURRENT` limits concurrent object-store reads per
  process, and `GRAPHDB_READ_OBJECT_SINGLEFLIGHT=true` coalesces concurrent
  reads for the same object key during cold cache misses.
- `GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT` bounds process-wide Arrow/Parquet
  materialization. The default is `4`; lower it when large entity pages cause
  heap pressure, at the cost of queued reads.
- `GRAPHDB_READER_INDEX_CACHE_DIR` enables a disk-backed cache for the same
  Parquet read objects. By default it uses
  `GRAPHDB_DATA_DIR/cache/index-objects`.
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES` limits the estimated payload of a
  merged entity-page object. The default is `32MiB`; it prevents large fields
  from turning row-count-based packs into oversized Parquet decode units.
- `GRAPHDB_INDEX_ENTITY_RECORDS=false` skips optional per-entity by-id record
  objects on the service write path. Set it to `true` on both writer and reader
  processes to return validated by-id records before loading an entity page.
  In that mode logical entity pages stay un-packed so one page update does not
  invalidate records belonging to unrelated sibling pages.
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true` incrementally maintains
  the collector checkpoint and is the default. Setting it to `false` makes a
  cold status read derive state from all persisted ingest batch records and is
  intended only for compatibility or controlled migrations.
- `GRAPHDB_FAULT_OBJECT_READ_DELAY` is a fault-injection knob for load, soak,
  and release-gate drills. Leave it unset in normal deployments; setting it
  adds delay to object `Get`, `Head`, and `List` reads before read protection
  and metrics wrappers run.

## Release Gate

`scripts/release_gate.sh` is the fixed production acceptance entrypoint. It
runs unit tests, race tests, RustFS compose, S3 compatibility against RustFS,
writer/reader e2e, load test, reader freshness smoke, repair/recover smoke,
multi-reader cold-start with slow object reads, reader restart, writer restart,
and object-store outage rejection. Set
`RUN_EXTERNAL_S3=1` with `S3_ENDPOINT`, `S3_BUCKET`, and credentials to add an
external S3-compatible endpoint to the same matrix. Set `S3_PATH_STYLE=true`
when that endpoint requires path-style access. Set
`RUN_OBJECT_STORE_OUTAGE=0` to skip the disruptive outage drill in shared local
environments. Set `RUN_COLD_READER_SCALE=0` to skip the isolated multi-reader
slow-read drill.

The fixed storage fault-injection matrix is
`go test ./internal/storage -run TestFaultInjectionRegressionMatrix`. It covers
object-store timeouts, partial 5xx write failures, ETag/CAS conflicts,
inconsistent list results, stale reader catalogs, task interruption retry, and
post-crash recovery from orphan commit objects.

## Configuration

Environment variables define service defaults. Per-tenant overrides live at
`tenants/<tenant>/config/tenant-config.parquet` and can override write
backpressure thresholds, quota limits, and simple maintenance/index policy
flags without changing process-wide defaults.

- `GRAPHDB_MODE=all|writer|reader`
- `GRAPHDB_STORAGE=local|s3`
- `GRAPHDB_ADDR=:8080`
- `GRAPHDB_PREFIX=graphdb`
- `GRAPHDB_DATA_DIR=.graphdb`
- `GRAPHDB_POLL_INTERVAL=2s`
- `GRAPHDB_QUERY_MAX_CONCURRENT=64`
- `GRAPHDB_QUERY_MAX_PER_TENANT=32`
- `GRAPHDB_QUERY_QUEUE_TIMEOUT=5s`
- `GRAPHDB_WRITE_MAX_CONCURRENT=32`
- `GRAPHDB_WRITE_MAX_PER_TENANT=1` (single-writer mode; `0` disables this
  admission dimension, values greater than `1` are rejected)
- `GRAPHDB_WRITE_QUEUE_TIMEOUT=2s`
- `GRAPHDB_WRITE_OBJECT_LATENCY_THRESHOLD=2s`
- `GRAPHDB_WRITE_OBJECT_ERROR_WINDOW=30s`
- `GRAPHDB_WRITE_OBJECT_ERROR_THRESHOLD=1`
- `GRAPHDB_WRITE_CAS_CONFLICT_WINDOW=30s`
- `GRAPHDB_WRITE_CAS_CONFLICT_THRESHOLD=5`
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=300`
- `GRAPHDB_WRITE_MAX_OBJECTS_PER_TENANT=0`
- `GRAPHDB_WRITE_MAX_BYTES_PER_TENANT=0`
- `GRAPHDB_WRITE_MAX_ENTITIES_PER_TENANT=0`
- `GRAPHDB_WRITE_MAX_EDGES_PER_TENANT=0`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=512MiB` bounds retained writer graphs using
  a conservative logical-size memory weight; `0` disables the graph cache.
- `GRAPHDB_SLOW_QUERY_THRESHOLD=500ms`
- `GRAPHDB_INDEX_HEALTH_INTERVAL=30s`
- `GRAPHDB_MAINTENANCE_INTERVAL=30s`
- `GRAPHDB_TENANT_USAGE_CACHE_TTL=60s`
- `GRAPHDB_READER_INDEX_CACHE_ENTRIES=4096`
- `GRAPHDB_READER_INDEX_CACHE_DIR=.graphdb/cache/index-objects`
- `GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT=4`
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB`
- `GRAPHDB_INDEX_ENTITY_RECORDS=false`
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true`
- `GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces`
- `GRAPHDB_OTLP_INSECURE=true`
- `GRAPHDB_SERVICE_NAME=graphdb`
- `S3_ENDPOINT=http://localhost:9000`
- `S3_BUCKET=graphdb`
- `S3_PATH_STYLE=false` (default virtual-host access; set `true` for local
  MinIO/RustFS-style endpoints)
- `S3_REGION=us-east-1`
- `S3_ACCESS_KEY_ID=minioadmin`
- `S3_SECRET_ACCESS_KEY=minioadmin`
