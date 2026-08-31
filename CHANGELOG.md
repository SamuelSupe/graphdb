# Changelog

All notable GGraphDB changes are recorded here. Versions follow semantic
versioning; release tags and binaries expose the exact build commit and date.

## [Unreleased] - 1.3 workstream

### Contract

- Define an opt-in PostgreSQL-CAS multi-writer WAL profile for
  `POST /v1/ingest/batches`, with an independent persistent WAL per writer,
  bounded same-tenant batch rebase/shrink, stable owner-routed status, and
  `202` durable-takeover semantics.
- Keep PostgreSQL as coordination metadata/head CAS only; object storage
  remains the graph-data authority and stores immutable graph objects.
- Define rolling compatibility between 1.2 direct writers and 1.3 WAL writers,
  with drain-before-downgrade and a process-crash/original-volume durability
  boundary.

### Release status

- The 1.3 contract is release-gated. No 1.3 performance, crash-recovery, or
  multi-writer acceptance result is claimed until commit-bound evidence is
  produced.

## [1.2.5] - 2026-08-31

### Improved

- When `GRAPHDB_MODE=all` has a warm `ReaderCache` whose cached version
  satisfies the requested freshness target, regular and stream queries use the
  materialized graph. Reader mode and cold-cache requests retain the lazy
  persisted-index path.
- Bounded single-node mixed read/write evidence on OrbStack Linux/arm64 (8 CPUs,
  8 GiB; 4 writers, 16 readers, 200 items/request, three 45-second
  duration-bound closed-loop rounds per comparison cohort) measured QPS
  `62.586→106.278` (`+69.81%`),
  QPS/core `+62.72%`, and mean operation-level p95
  `1308.0→386.3 ms` (`-70.46%`).

### Evidence boundaries

- The operation-level p95 comparison has sample variability, and some hot
  saved-query and scan paths have p50 regressions. Ingest p50 is effectively
  flat (`14097→13988 ms`, `-0.77%`), while ingest p95 worsened from
  `22291.7` to `23554.3 ms` (`+5.66%`, candidate CV `8.48%`); write-tail
  improvement is `UNKNOWN`. RSS improvement is also `UNKNOWN`: the mean fell
  `9.76%`, but candidate CV was `6.56%`.
- Index health was transiently stale in some end samples, and integrity
  snapshots reported `snapshot_catalog_missing` with maintenance disabled, so
  no full integrity `PASS` is claimed. Snapshot export regressed from mean p95
  `3744.3` to `5943.0 ms` and completed count `46.3` to `26.3`. Production
  capacity and full matrix coverage remain `UNKNOWN`.

### Compatibility

- API and storage layout remain compatible with 1.2.4; existing deployment
  modes and clients remain supported, while Go and Python SDK user-agent
  versions advance to 1.2.5. The performance figures are bounded
  fixed-environment evidence, not a production SLO or capacity guarantee.

## [1.2.4] - 2026-08-31

### Improved

- Large-bucket field-index lookups use a snapshot-level ordered cache and a
  stable streaming merge; aggregate and Top-K paths no longer allocate a full
  candidate-ID list before selecting and merging results.
- OrbStack Go 1.25.14 linux/arm64 process-internal relative evidence improves
  the original benchmark median from `7.133` to `6.058 ms/op` and allocation
  from `304,849` to `35,800 B/op`. On a 50,000-entity range aggregate c64 wave,
  latency is `43.765→31.192 ms`, throughput `1,462→2,052 queries/s`, p95
  `35.25→14.79 ms`, p99 `53.89→32.18 ms`, and allocation
  `34,614,535→13,642,518 B/wave`.

### Compatibility

- API and storage layout remain compatible with 1.2.3; existing deployment
  modes and clients remain supported, while Go and Python SDK user-agent
  versions advance to 1.2.4. The performance figures are process-internal
  relative measurements, not HTTP, object-storage, or mixed read/write
  production SLOs.

## [1.2.3] - 2026-08-30

### Improved

- Commit-tail replay is concurrent and bounded while preserving version-ordered
  application. Entity-page decode releases Arrow payloads promptly, and heavy
  graph load/compact work is bounded by backpressure and timeout controls.
- Materialized range/aggregate paths copy only final results, support value
  top-K, and deduplicate multi-value index keys. Fuzzy matching avoids
  per-entity filters and string allocations.
- Fixed-environment relative evidence, not a production SLO: tail-31
  `157.146→96.849 ms`, compact `149.525→112.156 ms`, and in-use heap
  `2218.06→1247.61 MB`; native in-process c64 range QPS
  `70.97→777.09`, p95 `1028.15→49.28 ms`, and
  `49.763→0.890 MB/query`; fuzzy QPS `1251.31→2568.26`, p95
  `48.955→12.305 ms`, and `1.235 MB→35.187 KB/query`.

### Fixed

- Compact keeps a newly advanced commit tail when the head moves, avoiding a
  maintenance conflict while retaining the newer writes.

### Compatibility

- API and storage layout remain compatible with 1.2.2; deployment modes and
  existing clients remain supported, while Go and Python SDK user-agent versions
  advance to 1.2.3.
- No HTTP, stream, saved-query, freshness, or mixed service-level performance
  pass is claimed; that matrix remains `UNKNOWN`.

## [1.2.2] - 2026-08-29

### Fixed

- Query validation now rejects oversized nested filters, projections, sorts,
  aggregates, traversal patterns, and cost budgets before storage work begins;
  GraphQL selection and variable handling follow the same bounded contract.
- Streaming and materialized query paths preserve cancellation and timeout
  semantics while avoiding unnecessary graph materialization and repeated
  index/object scans.
- Server shutdown stops task admission, cancels active maintenance and index
  workers, and waits for their terminal state; synchronous CLI operations now
  wait for the task result instead of returning after queue admission.
- Index rebuild admission and definition updates roll back cleanly when a task
  cannot start, and terminal state is published only after capacity and leases
  are released.
- Restore-drill cleanup failures now fail the task, retry partial cleanup under
  the original writer fence, and never report a failed required cleanup as a
  successful drill.
- PostgreSQL coordinator rollback reports mode-restoration failures and restores
  PostgreSQL mode when marker removal fails, avoiding a hidden write outage.
- The ingest WAL reports background writer startup and final sync/close errors;
  concurrent close calls are idempotent and return the same result.

### Compatibility

- Storage layout, endpoint names, and deployment modes remain compatible with
  1.2.1. Requests above the new documented query-shape limits are rejected;
  Go and Python SDK user-agent versions advance to 1.2.2.

## [1.2.1] - 2026-08-28

### Improved

- Commit-tail compaction and graph loading reuse decoded state, load persisted
  commit segments concurrently, and preserve version-ordered application.
- Reader graph caches retain active tenant graphs independently of manifest
  polling and bound cold-load concurrency, queue wait, and background load time.
- Query validation runs before storage I/O, while `timeout_ms` now covers
  admission, index access, cold graph loading, and execution end to end.
- Materialized kind pagination follows a cached stable ID order and stops at the
  requested window; mutation batches invalidate that order once.
- Materialized queries skip redundant persistent-index catalog reads, and lazy
  index failures use a short bounded retry backoff.

### Operations

- Added `GRAPHDB_READER_CACHE_IDLE_TTL`,
  `GRAPHDB_READER_CACHE_LOAD_TIMEOUT`,
  `GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT`, and
  `GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT`.
- Added benchmarks covering materialized kind/index pagination plus match,
  neighbors, pattern, traverse, impact, and shortest-path operations.

### Compatibility

- Storage formats, query request/response contracts, and existing deployment
  modes are unchanged from 1.2.0. New reader load controls have bounded defaults.

## [1.2.0] - 2026-08-28

### Added

- Optional local durable-WAL ingest with acknowledged admission, tenant FIFO
  batching, restart recovery, status/readiness reporting, and bounded queue and
  WAL backpressure.
- Persisted retrieval snapshots and a retrieval service boundary for lexical,
  vector, and fused evidence queries.
- GraphQL evidence responses with explicit freshness and retrieval metadata.

### Improved

- Entity upserts avoid publishing graph versions for semantic no-op writes and
  reduce copy-on-write work on the mutation path.
- HTTP routes and CLI commands declare mutation semantics next to their
  handlers, keeping tenant lifecycle enforcement and local-writer fencing in
  sync with registration.
- Object-store, coordinator, cache, and tenant-store construction now lives in
  a dedicated bootstrap layer; coordinator and ingest dependencies use smaller
  capability interfaces.
- Release hygiene rejects Finder-style duplicate files and incomplete vendor
  trees before the release gate starts.

### Compatibility

- Local WAL ingest is opt-in, defaults to direct ingest, requires local
  coordination, and is unavailable in reader mode.
- Existing 1.0/1.1 core graph layout and retained technical identifiers remain
  compatible; the release does not add RDF/OWL storage, SPARQL, or inference.

## [1.1.0] - 2026-07-27

### Added

- Domain-neutral entity type aliases, entity labels, relation property schemas,
  JSONL/CSV bulk import, and bounded graph pattern queries.
- GraphQL query transport with documents, operation names, variables, aliases,
  fragments, directives, and the standard `data`/`errors` envelope.
- Optional PostgreSQL tenant-head CAS for 2–8 concurrent writers, immutable
  manifests/write contexts, idempotency ownership, task fencing, legacy
  manifest outbox, and asynchronous derived-index catch-up.
- Coordinator migration/bootstrap/status/sync/rollback commands and coordinator
  availability, head revision, conflict, mirror lag, and backlog metrics.
- Separate data/admin listeners with pprof disabled by default.
- Complete OpenAPI coverage, versioned Go/Python SDK user agents, binary build
  metadata, reproducible capacity reports, and strict release gates.

### Compatibility

- Core layout version 2 entity, relation, commit, snapshot, and manifest formats
  remain readable by GGraphDB 1.0.
- `GGraphDB` is the public product name. The `graphdb` binary, module paths,
  `GRAPHDB_*` configuration, `X-GraphDB-*` response headers, and object keys
  remain unchanged for 1.0 compatibility.
- The former `GQL` name is retired. `/v1/query/gql`, `graphdb gql`, and SDK
  `GQL` methods remain deprecated aliases for the 1.0 `FIND`/`MATCH` text DSL;
  they are not GraphQL.
- Labels are persisted in `fields.__graphdb_labels`; relation schemas are an
  optional sidecar.
- The release gate builds `release_20260722_01` and validates both 1.0
  writer/1.1 reader and 1.1 writer/1.0 reader directions.
- PostgreSQL mode permits 1.0 readers through the legacy manifest mirror but
  rejects every 1.0/local writer.

### Security

- `/debug/pprof/*` is no longer exposed on the compatibility/data listener.
- Enabling pprof requires a distinct management listener.
- Production gateway guidance defines TLS termination, authenticated tenant
  header replacement, admin RBAC, and private upstream networking.

## [1.0.0] - 2026-07-22

- Initial current-state property graph release with Parquet object storage,
  tenant manifests, single-writer coordination, reader fleets, JSON query DSL,
  `FIND`/`MATCH` text DSL, ingestion governance, indexes, maintenance, and
  operations APIs.
