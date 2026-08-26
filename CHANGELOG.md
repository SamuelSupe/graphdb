# Changelog

All notable GGraphDB changes are recorded here. Versions follow semantic
versioning; release tags and binaries expose the exact build commit and date.

## [1.2.0] - 2026-08-25

### Changed

- Make `wal + segment + sync` the default ingest path for local writers. The
  default `POST /v1/ingest/batches` response is now durable `202 Accepted`;
  callers that require immediate visibility can send `Prefer: wait=committed`.
- Use a 250 ms graph flush interval with a trigger at 8 requests / 2 MiB and
  two graph flush workers; metadata flush defaults to 500 ms with a trigger at
  256 requests / 8 MiB and two metadata workers. Busy tenants may merge the
  same-round queue.
- Reuse the request normalized at WAL acceptance during graph flush and reuse
  the prepared commit-segment content identity during physical publication,
  removing duplicate normalization, commit-tail loading, and logical JSON
  encoding from the hot path without merging logical commits or versions.
- Reject new ingest with `429` and `Retry-After` at bounded queue, WAL, or
  pending-age admission watermarks. Readiness becomes non-writable at the WAL
  drain-only watermark. The fixed-host performance client retries the same
  batch after that delay, so saturation sheds load without dropping scheduled
  work or creating an immediate 429 retry loop. The fixed-host matrix gives all
  tenants one synchronized measured-workload start after seed and index setup.
  Performance defaults use a 5 ms group-fsync window, two graph and two
  metadata flush workers, the flush triggers above, a 4 GiB write cache, a
  20,000 commit-tail limit, and a two-minute pending-age guard. The admission
  path prepares durable envelopes outside the service lock, transfers their
  payload directly to the synchronous WAL append, and avoids rescanning the
  unchanged metadata queue. Successful durable-acceptance logs retain the first
  event and every 1,024th event while metrics, traces, failures, and WAL records
  remain complete. Heavy background task execution is single-concurrency by
  default.
- Store the complete request only in its accepted WAL record. Prepared,
  published, and finalized records carry compact state deltas, eliminating
  repeated graph-item serialization during state changes. Prepared records
  retain only the publish identity and result: recovery replays the accepted
  request when the manifest is still at the base version, and recognizes the
  final manifest without persisting a second copy of commit mutations.
- Bound decoded ingest-metadata cache residency by the retained graph-item
  footprint rather than compressed Parquet bytes, and serve immediate
  post-commit status polling from a bounded recent-result ring.
- Keep synchronous batch index updates correct when one flush advances multiple
  logical graph versions. The default WAL path defers derived index refresh,
  and per-tenant automatic maintenance requires a one-minute ingest-idle window
  before compaction, GC, or index catch-up. The fixed-host gate initializes
  indexes before timing.
- Require sync WAL durability for the public WAL mode so every `202` response
  is fsynced and recoverable; the former OS-buffered acceptance mode is not a
  v1.2.0 server option.
- Mount the writer data directory on a named volume in the supplied Compose
  profiles so durable accepted WAL records survive container replacement.

### Release evidence

- Add a fixed-host OrbStack gate with eight tenants, 16 collectors, sync WAL,
  segment metadata, five 30-minute runs for v1.1.5 and v1.2.0, and commit-bound
  accepted/committed latency, throughput, RSS, CPU, direct-write, and query
  regression evidence. Any failed threshold blocks the release workflow.

### Compatibility

- This release intentionally changes the default ingest protocol and does not
  provide a reverse-compatible writer rollback. Existing local data can be
  opened by v1.2.0, but once segment metadata is active the supported path is
  forward-only. PostgreSQL coordination must explicitly set
  `GRAPHDB_INGEST_MODE=direct`; distributed WAL is not implemented.

## [1.1.5] - 2026-08-01

### Fixed

- Recover readiness after a transient object-store or metadata flush error once
  the retry succeeds, instead of retaining a stale `last_error` and rejecting
  traffic after the dependency is healthy again.
- Fence a WAL writer after a fatal append, short-write, rotate, or fsync error.
  Subsequent writes are rejected without advancing the LSN, preserving a
  deterministic recovery boundary for the damaged WAL tail.

### Release evidence

- Add real process-level WAL recovery evidence for durable `202 Accepted`
  batches across restart and object-store interruption with explicit local WAL
  mode and segment metadata enabled. The evidence records the tested commit and
  verifies recovery before release.

### Compatibility

- Request/response shapes, WAL and metadata formats, object layout, and default
  modes are unchanged. The additive `ingest_wal_unavailable` error code is
  backward-compatible. `direct` ingest, `legacy` metadata, and local
  single-writer coordination remain the defaults; v1.1.4 WAL/checkpoint data is
  readable by this release.

## [1.1.4] - 2026-07-31

### Added

- Add a bounded process-local metadata manifest/index/segment cache with
  singleflight cold loads and short negative caching for local WAL writers.
- Add atomic local-WAL `checkpoint.json` recovery hints. A valid checkpoint
  scans only the active tail; invalid or missing hints safely fall back to a
  full WAL scan.
- Export metadata dispatch overshoot, cache, and WAL checkpoint metrics with
  corresponding structured logs and OpenTelemetry spans.

### Changed

- Raise the default `GRAPHDB_INGEST_METADATA_FLUSH_WORKERS` from 1 to 4 and
  avoid scheduler blocking when the metadata worker queue is saturated.
- Preserve one in-flight metadata flush per tenant while allowing independent
  tenants to make progress concurrently.

### Compatibility

- No graph commit, ingest metadata segment, dead-letter, logical-version, or
  tenant-isolation format changes. `legacy` remains the default metadata mode.
- Checkpoints and caches are disposable local accelerators; they neither
  replace the WAL nor change the existing segment activation/rollback fence.

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
