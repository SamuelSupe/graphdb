# Changelog

All notable GGraphDB changes are recorded here. Versions follow semantic
versioning; release tags and binaries expose the exact build commit and date.

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
