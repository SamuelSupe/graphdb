# Changelog

All notable GGraphDB changes are recorded here. Versions follow semantic
versioning; release tags and binaries expose the exact build commit and date.

## [1.1.3] - 2026-07-30

### Added

- Add the opt-in `GRAPHDB_INGEST_METADATA_MODE=segment` format for local WAL
  writers. Requests published across multiple graph flushes share one
  content-addressed Parquet metadata segment and one ingest-manifest CAS.
- Add batch, idempotency, and collector Bloom indexes, 32 recent segment
  references, and tiered reference-only catalogs with at most eight catalogs
  per level. Historical payload segments are never rewritten.
- Export fixed-cardinality metadata queue, segment/object-write, manifest CAS,
  lookup, and replay metrics together with structured lifecycle logs and OTel
  spans linked to the accepted requests.

### Changed

- Keep `PUBLISHED` WAL records until their metadata manifest is durable, then
  append batched `FINALIZED` state and reclaim the WAL.
- Make `Prefer: wait=committed` force the tenant's current metadata window while
  normal `202` requests may remain `published` until the metadata threshold or
  interval is reached.
- Read ingest identities in active-WAL, segment/index, then legacy-object order.
  Collector totals are seeded from legacy state when a tenant first enables
  segment metadata.

### Compatibility

- `legacy` remains the default. Segment metadata is limited to
  `GRAPHDB_COORDINATION=local` plus `GRAPHDB_INGEST_MODE=wal`.
- After a tenant has a segment manifest, a 1.1.3 legacy writer refuses further
  ingest for that tenant. All readers and writers must be upgraded before
  enabling the new mode; a 1.1.2 writer must not be used after activation.
- Graph commits, direct ingest, dead-letter objects, logical versions, FIFO
  ordering, and tenant isolation are unchanged. Legacy metadata is retained and
  remains readable without migration.

## [1.1.2] - 2026-07-30

### Added

- Add an opt-in process-local segmented WAL for
  `GRAPHDB_COORDINATION=local` single-writer deployments, with durable
  `202 Accepted` responses, batch status lookup, `Prefer: wait=committed`,
  crash recovery, bounded memory/disk queues, and graceful drain on shutdown.
- Batch per-tenant FIFO requests through one graph apply, one Parquet commit
  segment, and one manifest publication while sharing group fsync across
  tenants.
- Export low-cardinality WAL, queue, flush, deduplication, and recovery
  metrics; structured lifecycle logs; and OpenTelemetry spans and links that
  retain accepted-request context across WAL recovery.

### Changed

- Detect exact entity upserts before storage copy-on-write and reuse the same
  entity preparation path during normal and batched apply.
- Keep direct ingest as the default while allowing the WAL mode to coordinate
  direct commits, compaction, clone, disable, delete, and purge through a
  per-tenant flush barrier.

### Compatibility

- Object layout version 2, existing commit/manifest readers, and the default
  synchronous direct-ingest API remain unchanged. WAL mode is explicitly
  enabled and is limited to local single-writer coordination.

## [1.1.1] - 2026-07-30

### Fixed

- Treat ingest batches whose resulting logical graph is unchanged as no-op
  writes, while distinguishing `logical_noop` from `idempotent_replay` in the
  API, Go SDK, metrics, and audit output.
- Copy only graph maps affected by a storage mutation instead of cloning
  unrelated entity, edge, schema, and index maps before every commit.
- Build conflict-to-item associations with a batch index instead of repeatedly
  scanning large ingest requests.
- Encode dual-key ingest metadata once, publish the idempotency and batch
  records concurrently, and probe legacy compatibility keys with bounded
  concurrency while retaining current-key precedence.

### Compatibility

- The object layout and stored ingest record contract remain compatible with
  GGraphDB 1.1.0. `skip_reason` is an additive response field and is not
  persisted as idempotency state.

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
