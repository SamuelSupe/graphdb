# Changelog

All notable GGraphDB changes are recorded here. Versions follow semantic
versioning; release tags and binaries expose the exact build commit and date.

## Unreleased

## [2.2.4] - 2026-10-04

- Retain immutable reader graphs across local writer-fence updates and prepared
  maintenance publication. Expire and revalidate the manifest identity instead
  of replaying the entire graph; writer/control/index caches and full tenant
  invalidation boundaries remain intact. Preserve query deadlines and durability.
- Preserve the failed v2.2.3 tag and its Raft saved-query 504 evidence. It was not
  released; v2.2.4 independently repeats all official release gates.

- Reuse directory checks during snapshot and maintenance capture, allow readers
  through snapshot capture, and encode legacy snapshots outside the application
  lock. Retain immutable-file pinning, full graph validation and snapshot budgets.
- Pipeline up to four prepared-maintenance chunks after confirming the first
  manifest, then publish only after all chunks are acknowledged. Preserve full
  transfer digests, generation checks, rollback durability and resumable input.
- Seed every environment traversal root before the soak readers start, avoiding
  a startup race against the first writer; preserve failures rather than retrying
  an invalid query into success.

- Isolate backup worker failures by tenant and give each tenant its own polling
  budget. Reject inconsistent persisted cycle/task state before admission, keep
  corrupt state untouched, and let durable backups continue when only textfile
  metrics publication fails.

- Add a durable external backup worker for standalone and Raft entry points,
  with interval scheduling, checkpoint-preserving retries, uncertain-admission
  reconciliation, mandatory S3 readback, isolated restore drills, Prometheus
  textfile diagnostics and Compose/systemd deployment examples. Retain the
  standalone built-in policy; external remote retention uses explicit S3 lifecycle.

- Pin the leader's committed position before sampling followers during rolling
  upgrade preflight. Continuing writes no longer make healthy replicas appear
  behind a later sample; lagging replicas and fresh-quorum drain checks still
  block an unsafe restart. Apply the same sampling order to the rolling gate.
- Settle historical direct-commit idempotency records before acquiring the
  compaction publication lock, in both synchronous and background compaction.
  Slow record scans no longer hold up foreground commits. Keep conditional
  settlement, history validation and the concurrent commit tail at publication.

- Preserve published tenant objects when post-write fence validation encounters
  an I/O error. Return the original error instead of a retryable CAS conflict;
  a Raft application records the failure and rolls back without advancing its
  checkpoint. Conclusively stale leases and deleted tenant generations still
  remove the stale publication.

- Canonicalize typed timestamps to UTC before hashing Parquet commits, snapshots,
  indexes, tasks, saved queries, ingest/dead-letter and idempotency records. This
  prevents valid non-UTC timestamps from producing unreadable persisted objects.
  Retain opaque field values and existing UTC encodings; idempotent retries compare
  equivalent typed timestamps consistently. Existing inconsistent objects still
  fail integrity checks and require a trusted restore or replay.

- Fix encoded write-route authorization bypasses in the production NGINX template.
  Capture the normalized public request path before the auth subrequest selects
  required roles. Existing gateways must reload the updated configuration.
  Preserve the original HTTP method for identity checks and replace forged client
  method/URI headers. CI and release gates exercise real TLS, role rejection and tenant-header replacement;
  archives retain the matching gateway configuration and validation evidence.
  Include the diagnostic helper required by the packaged Raft gates.
- Mark unavailable historical capacity-run artifacts as local paths instead of
  broken repository links; retain the original historical result boundaries.

- Reject runtime archives whose data/WAL/Raft objects would share a destination
  subtree after relocating roots. Validate this before creating restore targets;
  publish staged files without replacing unexpected existing files.
- Hold data, WAL and Raft ownership locks throughout offline runtime backup and
  restore, including interrupted restore cleanup. Raft retains a separate directory
  lock and recovery also locks raft.db for older binaries. Process lock files are
  not archived or replaced; older archives and journals that contain WAL .lock
  remain readable without replacing the live lock inode.

- Bind Raft data directories and new replicated commands to `GRAPHDB_PREFIX`.
  Reject prefix changes on restart and foreign objects in incoming snapshots
  before replacing healthy state. Existing directories without a prefix marker
  are checked before the marker is first written; legacy commands remain readable.
- Bind acknowledged migration chunks to their byte count and per-chunk SHA256
  in replicated ownership. Missing or changed persisted chunks stop only the
  faulty replica without advancing its checkpoint; invalid transfer requests
  remain business conflicts. Legacy unfinished staging requires cancellation
  and a fresh transfer before cutover.
- Cold-validate tenant graphs and relation schemas in captured Raft snapshot
  views and decoded incoming snapshots before replacement. Legacy and streaming
  formats cannot export or install detected graph corruption while relying only
  on the archive checksum. Streaming validation runs in the snapshot builder.

- Validate tenant migration graphs and relation schemas from actual copied files,
  including required commits/snapshots, the current logical digest, tenant controls
  and objects that copy rewrites. Both export
  formats and the Raft install path reject invalid graphs before cutover or source
  cleanup; rejected installs preserve incarnation controls and keep replicas running.
  Lost files already declared by the transfer remain replica faults; malformed
  optional index catalogs cannot stop the entire destination group.
- Stage tenant copies before replacement. Exclusively opened local targets publish
  with the existing recoverable directory journal, preserving the old graph when
  copying or validation fails. The offline tenantmigrate tool uses this path;
  dry-run remains an inventory, and arbitrary object-store replacement is not atomic.

- Reject missing tenant heads when graph objects remain, instead of exposing an
  empty graph and resetting its version on the next write. Explicit standalone
  repair and initial orphan recovery remain available; Raft faults stop replay.
  Implicit first writes publish an empty head before staging a commit so failed
  publication and idempotent retry do not look like a lost published head.
- Verify the current logical graph digest on cold full-graph loads. Recovery and
  manifest reconstruction publish a matching digest; legacy digests stay readable.
- Preserve explicit entity and edge source identities when loading snapshots,
  preventing provenance metadata from inventing aliases and merging independent
  entities. Affected datasets require a separate upgrade qualification; mixing
  the old and corrected interpretation is outside the rolling-upgrade window.

- Preserve the S3 restore input digest across Raft leader changes by using the
  admitted task time for the integrity report. Standalone checks retain real time.
- Roll back and stop a replica when published graph dependencies are missing or
  malformed, instead of consuming the Raft command as an ordinary failed task.
  Healthy replicas continue; repaired replicas can replay the command.
- Add fault/replay and S3 transfer failover regressions, including the existing
  real S3 gate, and document recovery for partial transfers from older binaries.

The performance candidate's 30-minute maintenance soak recorded four HTTP 503
query failures; that failed result remains in the qualification report. Capture
microbenchmarks show lower allocation costs, but overall throughput, cross-host
availability and production capacity remain unqualified. The exact release
binary is independently checked by the complete tag workflow.

## [2.2.3] - Unpublished, 2026-10-04

The exact-tag release workflow failed with one HTTP 504 in its 30-minute Raft
maintenance workload. No Release or assets were created. Keep the original tag
and [failure record](docs/validation-v2.2.3.md); the changes above are carried
forward into the next candidate without treating this failure as a pass.

## [2.2.2] - 2026-10-03

- Publish the standalone, Raft, tenant sharding, rolling-upgrade and diagnostic
  features with aligned SDK/OpenAPI versions and release evidence.
- Correct bilingual product, deployment, architecture and documentation indexes
  to describe tenant sharding and default standalone together. Scope internal S3
  backup automation to standalone, and keep historical 2.1.2 qualification separate.
- Retain the publication-handler and GC test-fixture corrections from the 2.2.1
  candidate. The immutable 2.2.0 tag failed verification; the 2.2.1 distribution
  candidate was cancelled for documentation corrections. Neither published assets.

## [2.2.1] - 2026-10-03 (unreleased distribution candidate)

- Prepare the 2.2 standalone, Raft, tenant sharding, rolling-upgrade and
  diagnostic features through the qualified release pipeline.
- Install the publication-batch test hook before starting Raft, matching the
  real server and eliminating a test-only handler replacement race.
- Keep the GC execution-handoff deadline separate from completion of all
  filesystem deletions, preserving bounded batches and capacity-release checks.
- Align SDK/OpenAPI, deployment examples and qualification documentation with
  2.2.1. The immutable 2.2.0 tag failed release verification and did not publish
  a formal distribution; its failure evidence remains available.

## [2.2.0] - 2026-10-02 (unreleased distribution candidate)

- Add optional Share-Nothing Raft with majority durability and strong reads,
  alongside the default standalone direct/WAL deployment.
- Add a replicated tenant catalog, independent data groups, stable placement,
  shard expansion, explicit migration/cancellation, and disk-backed resumable
  transfer. One tenant remains entirely within one data group.
- Support qualified protocol-1 rolling upgrades with safe draining, leadership
  transfer, follower forwarding, redundant routers and a serial coordinator.
  Protocol 2/3 features require separate activation after all voters support them.
- Fix replica admission divergence, accepted WAL application stalls, membership
  races, stale tasks and migration flushes, and corrupted import publication.
- Stream optional snapshots and large recovery input, preserve resumable transfer
  identity, and protect full-runtime cold restore and replica directory roles.
- Make maintenance fair across tenants, pause only conflicting tenant writes,
  prepare protocol-3 GC outside the application barrier, and batch durable
  before-images before final publication.
- Separate Raft control transport, configure election timing independently,
  isolate disk probes, and align gateway withdrawal with upgrade drain waits.
- Retain verified active routes for a bounded catalog outage; unknown or expired
  routes fail closed and data groups continue enforcing epoch and majority rules.
- Expose local diagnostics, timing, queue, drain, maintenance and router metrics,
  with operational alert examples and deployable failure/soak gates.
- Align SDK/OpenAPI versions, build identity, deployment documentation and release
  packages; verify protocol 1/2/3 evidence against the packaged Linux binary.

Standalone 2.0/2.1 directories remain compatible after shutdown. Raft compatibility
is limited to the tested source/target/protocol window; protocol 3 persists a
minimum supported version and prevents reopening with a protocol-2-only binary.
Cross-host, production capacity and day-scale stability remain pending. Tenant
maintenance can return 429 and exhibit long waits; no general throughput or
latency guarantee is claimed. See [2.2.0 qualification](docs/validation-v2.2.0.md).

## [2.1.2] - 2026-09-29

Compatible with existing 2.0/2.1 local data directories and HTTP contracts.

- Check read-view protection before decoding orphan index files during GC,
  avoiding repeated Parquet validation under the tenant lock for deferred files.
- Preserve the retirement epoch through preflight and deletion checks so newly
  admitted readers cannot repeatedly postpone eligible orphan collection.
- Keep content/tenant validation before reclamation, same-version catalog
  protection, checkpoint accounting and deletion-time view checks.
- Update bilingual READMEs, deployment instructions, SDK versions and release
  documentation. No data-format or durability-default changes.

The focused 4-writer/16-reader workload observed write P95 fall from
11.77–12.34 s to 8.08 s. Compaction showed an unresolved regression signal;
this does not establish an overall performance-qualified capacity envelope.
See [measurements and limits](docs/performance-write-tail.md) and
[release validation](docs/validation-v2.1.2.md).

## [2.1.1] - 2026-09-28

Compatible with existing 2.0 local data directories and HTTP contracts.

- Add opt-in tenant S3 backup schedules, durable retries, checksum verification,
  optional restore drills, bounded retention, and status/reset APIs in both SDKs.
- Finish observing and reclaiming successful captures even when scheduling is
  disabled during a run; protect schedule writes with tenant-generation fencing.
- Unify ordinary and index tasks, shutdown ownership, query view selection, and
  direct/WAL graph publication. Remove unused distributed heartbeat/TTL paths.
- Let GC defer files held by older read views without excluding new readers;
  preserve same-version catalog and restore/delete lifecycle protections.
- Avoid scanning unrelated tenant read views on every file publication and stop
  S3 retention pagination at its deletion budget. Remove duplicate initialization.
- Fix task terminal-state admission races and audit logging with empty fields.
- Preserve a forced WAL flush across an in-flight publication failure, while
  keeping backoff for subsequent failures. The unpublished 2.1.0 candidate was
  cancelled after CI exposed this recovery timeout; 2.1.1 supersedes it.
- Update SDKs, OpenAPI, deployment and bilingual operating instructions.

See [2.1 validation and limitations](docs/validation-v2.1.1.md).
No overall throughput or tail-latency improvement is claimed by this release.

## [2.0.0] - 2026-09-28

Local disk is now the main edition and stable release. One process owns a data
directory and serves concurrent tenants; S3-compatible storage is reserved for
snapshot backups and on-demand restore. This is a breaking release: use a fresh
2.0 directory. No 1.x migration or cross-major rollback is provided.

- Replace `data_md5` with `data_hash` (`sha256-shards-v2:<64 hex digits>`).
  Update only changed logical hash buckets; hash a fixed-size root per version.
- Copy only changed entity, edge, adjacency and field-index buckets across versions;
  keep small field-index ID sets compact.
- Reuse entity partitions for incremental indexes; bound queued delta batches
  instead of rebuilding merely because a backlog threshold was crossed.
- Avoid redundant automatic rebuilds while ordered incremental indexes are catching up.
- Bound GC scan work and reopen read admission during long view waits, including
  overlapping maintenance waiters. Share
  memory admission across queued indexes, active builds and restore drills.
- Bound file publication groups by count, bytes and elapsed time, retaining
  data-before-manifest durability and directory barriers.
- Reduce restore/backup lock scope; preserve tenant generations, recovery
  journals, accepted WAL semantics and in-flight read protection.
- Publish the Go module as `github.com/SamuelSupe/graphdb/v2`; Go and Python SDKs
  are version 2.0.0. HTTP `/v1` routes remain the current transport API.
- Align deployment, documentation, website and stable release gates with 2.0.
  Build containers, CI and release binaries with Go 1.26.7, matching local validation.

See [2.0 performance and validation](docs/performance-v2.0.md) for measurements
and limits. Historical 1.x performance results do not qualify this release.

## [1.3.4-local.9] - 2026-09-20

Independent local disk prerelease on `codex/local-disk-v2`; `main` and the stable
Latest release remain unchanged.

- Enumerate dead-letter directories once per internal scan while preserving
  cursors, early termination, and fresh metadata reads.
- Partition the graph once during deep index validation and share the hash
  calculation for current and legacy shard identifiers.
- Remove unreachable PostgreSQL coordination code and its dedicated tests;
  retain local locking, fencing, durable recovery, data formats, and explicit
  rejection of unsupported coordination settings and markers.
- Run local GC in batches of up to 4,096 deletions, releasing locks and rechecking
  current references between batches. Index orphan cleanup now honors cursors,
  deletion budgets, and dry runs.
- Apply the 4,096-file batch bound even when an overall deletion budget is set,
  admit queued readers between exclusive maintenance batches, and share
  directory durability barriers within each deletion batch.
- Release task admission across local GC batches so compaction can relieve WAL
  backpressure, while reacquiring the global execution limit for each batch.
- Deduplicate repeated WAL entity changes before updating index postings, and
  create new secondary shards without attempting to read an empty object key.
  Both cases previously triggered unnecessary full index rebuilds.
- Keep a fixed GC candidate listing across the last-page deletion checkpoint,
  preventing continuous writes from extending a cleanup run indefinitely.
- Release the tenant maintenance slot during post-rebuild cleanup while retaining
  the global worker limit; live local GC workers no longer expire solely because
  their persisted heartbeat is delayed.
- Classify tenant-usage sampling deadlines during soak shutdown consistently
  with other sampling operations; retain failures outside shutdown grace.
- Stop maintenance loops cleanly when the soak run ends; request deadlines and
  actual maintenance failures remain errors.
- Include the source required by the packaged Dockerfile, and verify a container
  built from the extracted release archive before publication.
- Flush streaming query/scan output in 32-row batches after immediate metadata,
  fixing a counter reset that flushed every row; report materialized stream
  encoding failures in query telemetry.
- Update bundled Go/Python SDK versions to `1.3.4+local.9`.

A focused 10K-entity deep-index benchmark measured 1.65 s/op before and 1.49 s/op
after, with 5.08% fewer allocations. This small warm sample is not an overall
throughput or cold-read guarantee. See
`docs/performance-local-disk-code-simplification.md` for evidence and limits.

## [1.3.4-local.1] - 2026-09-18

Independent local disk prerelease from `codex/local-disk-v2`; the default `main`
branch and stable Latest release are unchanged.

- Run one process with concurrent tenant workloads and an exclusive data-directory
  lock shared by the service and offline tools. Remote online-storage backends,
  PostgreSQL coordination, separate reader/writer modes and owner routing are
  retired; incompatible settings and coordinated data fail explicitly.
- Preserve local Parquet/WAL formats and API contracts. File publication batches
  directory syncs after durable file writes, with publication barriers before
  catalog replacement. Bounded metadata caches track process-local generations.
- Read Parquet directly through closable random-access files. Publication updates
  reader caches; active read views protect files during GC, purge and restore.
- Optionally back up committed tenant snapshots to S3-compatible object storage.
  Discover published backups after local data loss, verify SHA-256 before restore,
  and retry interrupted tasks using their original captured versions.
- Deploy with GraphDB and one persistent local directory. Local release gates
  cover compatibility, SDKs, HTTP recovery, restart and background maintenance;
  focused before/after reports record measured gains and unresolved regressions
  separately, without a general performance guarantee.
- Optimize bounded Parquet decoding, projected/paginated reads, incremental
  indexes, no-op compaction and deep index validation. Preserve recovery,
  cancellation, read-view isolation and nested result ownership invariants.
- Avoid repeated decoding and self-comparison hashes when GC validates listed
  index orphans. Retain catalog hash checks, current references and tenant/version
  protection; this fixes a maintenance bottleneck found by the release soak.
- Publish Linux amd64/arm64 and macOS arm64 binaries with checksums, build
  metadata, validation evidence and SDKs (`1.3.4+local.1`).

## [1.3.4] - 2026-09-15

### Performance

- Local incremental index refresh no longer holds the tenant foreground lock;
  same-tenant refreshes remain ordered by commit version.
- Immutable Parquet index objects and sharded snapshot parts use bounded
  four-worker I/O, while preserving catalog ordering and validation semantics.
- Graph query streams flush the first item immediately and batch later flushes;
  logical MD5 encoding reuses its buffered writer.

### Correctness

- Version gaps caused by queued index refreshes rebuild the current catalog;
  independently stale catalogs still return the existing warning contract.
- Panics from bounded index or snapshot workers are converted to task errors
  instead of escaping from worker goroutines.

### Verification boundary

- OrbStack Linux/arm64 core tests, focused storage tests, targeted race tests,
  and full-repository Go compilation passed on the release candidate.
- The release still requires the repository's static, compatibility,
  RustFS/PostgreSQL integration, soak, and rollback gates.

## [1.3.3] - 2026-09-07

### Improved

- COW graph versions inherit sorted entity ID order and selectively invalidate
  changed kind/field/value-key caches; scalar range scans reuse ordered keys.
- Changed entity and edge shards build from the authoritative post-commit graph;
  Parquet entity reads prune irrelevant row groups.
- Warm `all`-mode scans and snapshot streams reuse fresh version-matched graphs
  with catalog-pinned cursors; logical MD5 state shares immutable COW data.
- Entity streams expose `JSONValue` for one outer encoding; reverse `To` data
  uses 128-edge physical packs, and edge reads use streaming row-group pruning.
- Local maintenance preserves an active same-instance runtime after lease expiry;
  strict manifest/ETag matching protects write-cache reuse and public clone isolation.

### Fixed

- Active local maintenance is no longer misclassified as stopped solely because
  its writer lease expired; orphan/coordinated ownership semantics remain unchanged.

### Performance evidence

- The self-contained [1.3.3 performance report](docs/performance-v1.3.3.md)
  separates round1 history from the final comparison. In the same bounded
  OrbStack Go 1.25.14 linux/arm64 envelope, writes were `21→28`, ingest p95
  `16.8→13.0 s`, export p95 `621→340 ms`, and stream median `13.136→6.480 µs`.
- Large indexed stream p99 regressed `167→221 ms` (min-version `131→224 ms`);
  16-target cold reverse lookup rose about 15%, and RSS was higher on a larger graph.
- Maintenance reached `succeeded`; after restart integrity was `status=ok` with
  1,747 checks and 0 issues, freshness was `30/30` with 0 ms lag, and index health
  was `ready` with 17 orphan warnings from skipped cleanup.

### Compatibility

- No object-storage layout or WAL record format migration is introduced; JSON
  response shapes and cursor semantics remain unchanged.
- Focused and multi-pack checks plus the source-final-v3 local static gate exited
  0. Release assets require the documented unit/race/compatibility, integration,
  30-minute soak, and rollback workflow gates.

## [1.3.2] - 2026-09-05

### Fixed

- Concurrent WAL admissions are enqueued in completed durable WAL append order,
  preserving per-writer FIFO behavior under concurrent `Accept` calls.
- WAL pruning retains an accepted record until its active admission state is
  registered, so a record cannot be removed in the registration gap.
- Terminal preparation failures retry the entire completion batch. A per-record
  terminal WAL append failure still preserves the successful-prefix retry
  boundary.
- After direct ingest has published data, a wrapped `context.DeadlineExceeded`
  from metadata/object storage while the overall `writeCtx` remains valid now
  follows the timeout branch: it returns `504 request_timeout` with
  `retryable: true`, invalidates the cache, and keeps the published data visible.
- Same-tenant shutdown no longer busy-loops, and ready/complete queues remain
  live when multiple tenants are active instead of deadlocking.

### Performance evidence

- In an OrbStack Linux/arm64 benchmark using Go 1.25.14 in the
  `golang:1.25-bookworm` image, 8 CPUs, 8 GiB, real `appendTerminalBatch` plus
  WAL writes, and setup/teardown outside the timer, three fixed `10x` rounds
  measured the terminal bookkeeping hotspot. With `active=8192, complete=256`,
  the median moved from
  `20.466951` to `3.409939 ms/op` (`-83.3%`); with
  `active=4096, complete=256`, it moved from `12.507517` to
  `2.894312 ms/op` (`-76.9%`). This is a bounded hotspot measurement, not an
  end-to-end QPS or capacity claim.

### Verification boundary

- Local Go tests, `go vet`, focused race checks, and isolated PostgreSQL checks
  passed for the source tree. Release publication has separate static,
  RustFS/PostgreSQL integration, 30-minute CAS soak, and rollback gates; this
  local evidence does not substitute for those workflow results.
- The benchmark used `go test -mod=readonly -run '^$' -bench
  '^BenchmarkIngestTerminalCompletion$' -benchmem -benchtime=10x -count=3
  ./internal/storage`, comparing the baseline at `26175c37` with the candidate.

### Compatibility

- No ingest WAL record format change or migration is introduced. The existing
  1.3 graph/object layout and HTTP contract remain compatible.

## [1.3.1] - 2026-09-02

### Added

- Added commit-equivalent ingest controls across direct, local-WAL, and
  PostgreSQL-coordinated writers: `expected_version`, bounded entity/edge
  preconditions, and `best_effort` or `atomic` failure modes now retain stable
  terminal error codes through durable admission, retry, recovery, and status
  polling.
- Added blocking and non-blocking ingest support to the Go and Python SDKs,
  including WAL `202` acceptance, owner-routed status polling, and explicit
  terminal waiting. Both SDK package versions advance to 1.3.1.

### Improved

- Compatible requests in one writer WAL flush can share validation and one
  copy-on-write candidate publication while retaining independent results,
  consecutive logical versions, and FIFO order. PostgreSQL writers still
  compete through tenant-head CAS; payloads are never merged across writers.
- Conflict handling reuses prepared work and bounded CAS cohorts to reduce
  repeated graph loads, commit objects, and manifest writes without weakening
  lifecycle fencing or idempotency.
- Snapshot creation no longer materializes an unused embedded index. Legacy
  snapshots carrying that field remain readable and indexes are rebuilt from
  authoritative graph data.
- Derived-index rebuilds now use one Parquet implementation instead of carrying
  inactive format-selection and incremental-capture paths.

### Fixed

- Recovery compaction no longer remains blocked by stale ingest-active state,
  and shutdown/recovery paths preserve terminal WAL ownership and failure
  state.
- Direct PostgreSQL ingest now reserves both the primary idempotency key and
  the `batch_id` alias, preventing two writers from committing the same batch
  identity with different keys.
- Ingest path identifiers reject dot path segments consistently with the Go
  and Python SDKs, so an accepted durable request always has a pollable owner
  status URL.
- Lifecycle fencing, partial-failure metadata, publisher leases, and batch CAS
  completion remain atomic across retries and writer takeover.

### Contract changes

- Removed the unsupported Evidence Search GraphQL surface, its retrieval error
  codes, and the dormant retrieval service/storage implementation. The
  supported public GraphQL contract has the `graph` root only.
- Expanded OpenAPI mutation and ingest schemas to match the implemented HTTP
  contract, including conditional failures and canonicalization results.

## [1.3.0] - 2026-09-01

### Added

- Added an opt-in PostgreSQL-CAS multi-writer WAL profile for
  `POST /v1/ingest/batches`. Each writer has an independent persistent WAL;
  PostgreSQL stores tenant-head CAS and coordination metadata only, while
  immutable graph objects in object storage remain authoritative.
- Added durable owner takeover semantics: `202` is returned only after the
  writer's local WAL is synced and means that writer durably accepted
  responsibility for the batch. It does not mean that a graph version is
  committed. The response includes the stable `writer_id` and owner-routed
  status URL, including during startup recovery.
- Added bounded batch CAS/publish slots, per-writer WAL FIFO, successful
  PostgreSQL CAS ordering across writers, rebase and repeated-conflict batch
  shrinking, and cross-writer idempotency coordination.
- Added lifecycle generation fencing so freeze, delete, and recreate take
  precedence over unpublished WAL work; fenced work becomes a visible final
  failure rather than publishing against a new tenant generation.

### Supported topology and operations

- Supports 2–8 concurrent writers for one tenant, with horizontal scale across
  tenants. Eight same-tenant writers are a correctness and availability
  boundary, not a linear hot-tenant throughput claim.
- Supports controlled coexistence of 1.2 direct writers and 1.3 WAL writers
  through PostgreSQL coordination schema v5 and the existing graph/object
  layout. Each writer must use a unique stable `GRAPHDB_INSTANCE_ID` and its
  own persistent WAL volume.
- Rolling downgrade requires stopping new WAL admission and draining every
  writer WAL until no durable record remains pending. An unconditional
  in-place downgrade, WAL-volume reassignment with pending records, or
  recovery after permanent volume loss is outside the contract.

### Verification and evidence

- Full Go tests, focused race checks, `go vet`, OpenAPI/static release checks,
  and isolated PostgreSQL checks passed for atomic publish/rollback,
  cross-writer idempotency, same-tenant 2-, 4-, and 8-writer concurrency,
  four independent tenants, recovery, and owner-routed status.
- Reader-cache behavior remains bounded: a warm `ReaderCache` in `all` mode
  can serve a fresh-enough materialized graph, while reader mode and cold-cache
  requests retain the lazy persisted-index path.
- Fixed-environment single-node read evidence (OrbStack Linux/arm64, 8 CPUs,
  8 GiB; three 45-second rounds per cohort) measured QPS
  `62.586→106.278` (`+69.81%`) and mean operation-level p95
  `1308.0→386.3 ms` (`-70.46%`). These measurements are historical relative
  evidence, not a production SLO or a 1.3 WAL capacity certification.

### Compatibility and evidence boundaries

- Direct commits and other mutations retain their existing synchronous paths;
  the WAL profile covers ingest batches only. PostgreSQL or object-store
  outages keep accepted batches retryable; a simultaneous outage does not
  provide an immediate graph commit or exactly-once semantics.
- The durability guarantee covers process failure when the original writer WAL
  volume can be recovered. It does not cover permanent loss of that volume.
- The release does not claim unbounded graph size, linear throughput from eight
  writers on one hot tenant, or a production capacity guarantee. Re-run the
  capacity envelope in the target deployment before setting limits.

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
