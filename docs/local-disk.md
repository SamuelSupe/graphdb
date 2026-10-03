# GGraphDB 2.x local disk operation

The default standalone deployment runs one process with concurrent tenant workloads. The process owns
`GRAPHDB_DATA_DIR`; graph data, control metadata, background tasks and backups are
local files. PostgreSQL and remote online-storage backends are unavailable.
Optional [S3-compatible snapshot backups](object-backup.md) support recovery after local disk loss.

This guide describes the local storage and standalone path. The same release also supports independent [Raft replicas](raft-ha.zh-CN.md) and [tenant sharding](sharding.zh-CN.md); replicated durability, administration, recovery and rolling upgrades have their own contracts.

## Configuration

| Setting | Default / contract |
| --- | --- |
| `GRAPHDB_DATA_DIR` | `.graphdb`; persistent local directory |
| `GRAPHDB_PREFIX` | `graphdb`; existing tenant layout retained. Raft voters must share the original prefix; a bound directory rejects changes on restart. See [prefix protection](raft-operations.zh-CN.md). |
| `GRAPHDB_MODE` | `all`; other modes fail startup |
| `GRAPHDB_STORAGE` | `local`; other values fail startup |
| `GRAPHDB_COORDINATION` | `local`; PostgreSQL settings fail startup |
| `GRAPHDB_INGEST_MODE` | `direct` or `wal` |
| `GRAPHDB_INGEST_WAL_DURABILITY` | `sync`; fsync before durable acknowledgement |
| `GRAPHDB_INGEST_WAL_DIR` | `<data-dir>/wal/ingest` |
| `GRAPHDB_READER_CACHE_MAX_BYTES` | 512 MiB |
| `GRAPHDB_READER_CACHE_MAX_TENANTS` | 64 |

Use a local filesystem supporting file locks, atomic rename, and file/directory
fsync. Network shares and multiple processes sharing a directory are unsupported.
Do not change files underneath a running service. The directory lock also covers
offline CLI tools, including local-to-local tenant copy. A second opener fails
immediately; the operating system releases the lock after process death.

## Publication and reads

Immutable data files are individually synced and renamed. Groups of at most 64
write jobs, using four workers, check 16 MiB / 50 ms budgets between complete files and sync each affected directory before
publishing a referencing catalog. Newly created directories sync their parents.
Manifest/catalog publication remains an atomic file replacement. A failed group
can leave unreferenced files; it does not create a multi-file transaction.

Parquet entity pages, edge shards, secondary indexes and snapshots use random file reads and retain
column/row-group selection and content/schema checks. File descriptors and decode
admission slots are released when the operation finishes. Graph caches are
invalidated by local publication, including restore to an older version.
Decoded manifests have an 8 MiB / 4096-entry bound and are invalidated by file
generation, so a restored graph version cannot reuse an earlier cached head.
On a cache miss, the manifest is read from the current disk file, bypassing byte
caches and shared in-flight reads that may have started before publication.
Legacy monolithic snapshots and backup/task-result records also decode from a
file handle, avoiding a second complete copy of the compressed file in memory.

HTTP read views prevent GC, commit cleanup, purge and restore from deleting files
that a request has not opened yet. Ordinary commits publish new immutable files
while existing reads finish. Background task reference markers remain durable.
Shared cache loads retain their read view until loading finishes, even if the
initiating HTTP request is canceled.
Reader freshness is reported for the local instance; no remote reader fleet is
inferred from old heartbeat files.

Pagination cursors bind the durable tenant generation, including index and graph
fallback paths. Restore and purge invalidate older cursors even when the graph
version is reused. Legacy cursors remain valid in the initial generation; normal
commits and index updates do not change it. A missing pinned catalog or data file
causes an error instead of continuing against another graph. Running-query list
and cancellation use the process registry with tenant validation, without waiting
for graph read views or maintenance.

GC releases its tenant and read-view locks between batches and reloads the current
manifest and catalogs. Local candidate pages contain at most 512 files. Between
objects, GC yields after 50 ms of work or 16 MiB of candidate bytes; a single
object always makes progress, so this is a cooperative budget, not a hard latency
limit. The explicit total deletion budget and dry-run cursor remain supported.
Files modified after the scan starts are left for a later run. Only the current
candidate pages and directory entries on the listing path are retained, rather
than a complete prefix listing. Very large individual directories still require
sorting their entries; a single large file can exceed a batch's time budget.

Deletions share directory durability barriers, flushed before releasing locks,
including on failure or cancellation. Index orphan validation reads directly
from file handles while retaining its content and tenant checks. Local GC yields
task execution capacity between batches so compaction can relieve WAL backpressure.
Index rebuild still blocks write admission during backfill; its cleanup uses the
same batch locks. Live workers are tracked in process; task liveness does not depend on heartbeats.

## Readiness and memory budgets

`/v1/readiness` checks that the data directory exists and can create, write, sync
and remove a small temporary file. Concurrent dependency probes share work. A
readable but unwritable directory is not ready for the local write service;
`/v1/health` remains a liveness check.

Compose accepts `GRAPHDB_MEMORY_LIMIT`, `GOMEMLIMIT`, and reader/writer cache byte
limits. Defaults keep the container/runtime memory limits unset and both graph
caches at 512 MiB. Set these together for a finite-memory deployment, for example:

```sh
GRAPHDB_MEMORY_LIMIT=7g GOMEMLIMIT=5GiB docker compose up -d
```

This is a starting configuration, not a capacity guarantee. `GOMEMLIMIT` is a Go
soft heap budget, not an RSS limit; leave room for decoding, snapshots, stacks,
file access and other caches. Validate the actual dataset with maintenance enabled.

## Ingestion

Entity page packing defaults to a 32 MiB estimated-memory budget
(`GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES`). One logical shard can exceed this budget.
The decoded entity-page cache is capped at half of the configured
`GRAPHDB_READER_INDEX_CACHE_MAX_BYTES`, matching the decoded edge-cache policy.

Direct writes return their published result. WAL mode returns 202 after durable
acceptance; that is not yet a committed graph version. Poll the response's
`status_url` or use `Prefer: wait=committed`. Status URLs use
`/v1/ingest/batches/{source}/{collector_id}/{batch_id}`. Legacy writer-routed URLs
return 501. A restart recovers accepted requests from the existing WAL format.

After direct/WAL ingest manifest publication, an in-process worker advances indexes in version
order. At most eight background updates are retained across all tenants; when
full, ingest finishes its index work synchronously after releasing the tenant lock.
Adjacent pending batches share a bounded delta; more than 8192 change
references start another bounded incremental batch. `committed` still means durable, readable graph
data. Queries use a graph view meeting `min_version` when indexes lag. The
`/v1/commits` API still waits for its index update and shares that ordering chain. GC, restore, and directory close
wait for those file references. A crash can leave indexes behind the durable
graph head; it does not discard published graph commits.

## Upgrade and recovery

2.1 retains the 2.0 data format. Stop the old process before reusing a 2.0/2.1
directory. Replacing 1.x requires a fresh directory; no 1.x migration or cross-major
rollback is provided. Manifests use `data_hash`; pre-2.0 Parquet manifests are
rejected. Keep 1.x installations and their backups separate.
PostgreSQL coordination markers are also rejected; do not remove them to force entry.

Use the backup/restore and integrity-audit HTTP APIs. To recover after disk loss,
configure [object-storage snapshots](object-backup.md) and create a backup with
`destination: object`. The existing same-directory backup remains available.
Use a backup produced by the same supported major version for recovery.

Restore results and their tenant generation are published with the replacement
directory. If terminal task persistence fails, retry only finalizes the task,
preserving later acknowledged writes even if the source backup is unavailable.
A replaced tenant or a legacy task without a verifiable publication checkpoint
causes retry to fail instead of overwriting again. Remote snapshot download and
decoding do not hold the directory switch lock; queued file access and restore
publication honor cancellation.

Local backup manifests validate their immutable backup record, including older
manifests that also listed live heads or indexes. Subsequent commits and index GC
do not invalidate that recovery record.

Restore builds and verifies a complete tenant in a staging directory on the same
filesystem. Existing tasks and local backups are preserved under a barrier for
that tenant directory; other tenants can continue file access during preparation.
Each retained directory is synced once. The global IO gate covers the directory
switch and generation update; old-directory deletion runs after releasing it. A durable switch
journal retains the old directory until the new one is committed; startup rolls
back incomplete switches before accepting requests. These directory renames are
not an atomic multi-file transaction. Reserve space for both old and new data.
If a disk error prevents rollback, the store rejects further operations until it
is reopened and recovery succeeds.

Purge and restore pause WAL admission, drain accepted requests, and then acquire
the exclusive tenant view. WAL records carry a tenant generation stored outside
the tenant directory; purge and restore advance it so offline recreation cannot
replay an old tenant's requests. Legacy unbound WAL is recoverable only in the
initial generation. Concurrent tasks reuse an active task only with identical
parameters; conflicting parameters return a conflict instead of silently using
a different restore or backup.

## Validation and performance

Current release scope is in [2.2 validation](validation-v2.2.2.md); actual release workflow conclusions and packaged evidence qualify its binary. Historical [2.1.2 validation](validation-v2.1.2.md) and [write-tail measurements](performance-write-tail.md) describe that older build. Its 30-minute run was stopped by explicit release decision and is not a pass; that historical waiver does not apply to the current release gates.
The [2.0 report](performance-v2.0.md) and earlier reports below are historical.

Start performance work with one representative comparison and repeat only to
investigate a measured regression. `scripts/local_disk_optimization.py` compares
before/after local binaries using the same deterministic seed in fresh per-version directories, covering
direct/WAL mixed load, export, compaction, backup/restore, and process-cold reads.
See the [optimization measurements](performance-local-disk-optimization.md).
The [second optimization round](performance-local-disk-optimization-2.md) covers
catalog hashes, entity pagination, HTTP encoding, Parquet row locality and incremental indexes.

`scripts/release_gate.sh` runs unit/vet/race/2.x-contract/SDK checks, then direct
and WAL HTTP scenarios, load, and restart equality. `GRAPHDB_GATE_SOAK=1` adds a
30-minute mixed workload with compaction, GC and index rebuild. The HTTP gate uses
a tiny graph cache to exercise persisted index reads; it is not a capacity benchmark.

The optional full acceptance matrix compares baseline `ffa85414` with S3, the same baseline
with local files, and this edition on identical Linux volumes and resource limits.
Use approximately 10K/100K entities, direct/WAL, four writer clients and sixteen
reader clients, 60 seconds warmup and five minutes measurement, in rotating order
for three repetitions. Report published throughput, p50/p95/p99, acceptance-to-
visibility delay, CPU/RSS, filesystem I/O, fsyncs, and open file descriptors.
Distinguish a cold process cache from an OS page-cache reset.

Targets relative to the existing local baseline: at least 10% mixed throughput
and cold-read p95 improvement; no more than 5% other tail/export regression or 10%
peak RSS increase. Measurement noise, failed correctness checks, or missing cases
must remain visible in the report. Historical performance reports are not evidence
for this build.

### Optional full comparison

```sh
scripts/local_disk_compare.sh
```

Run from a full Git checkout with Docker/OrbStack. The script builds the exact
baseline with the same observational fsync counters, builds this worktree, then
uses isolated containers and Linux volumes. Defaults run both graph sizes and
both ingestion modes, 60s/300s warmup/measurement for mixed load and the maintenance
sequence, thirty process-cold reads, and three rotating repetitions. This takes
several hours. A shorter harness check can use `--entities 10002 --warmup 2
--measure 10 --maintenance-seconds 0 --cold-samples 2 --repeats 1`; its results
cannot establish acceptance. Evidence and source hashes are retained under
`capacity-runs/`, and the named Linux volume remains available for inspection.
GraphDB fsync counters do not include MinIO's internal storage calls.
All comparison groups use a 60-second write-admission queue timeout: the default
two seconds can reject queued writers in the 100K direct workload. Queue waiting
remains included in reported write latency; this does not change service defaults.
Comparison groups also allow 120 seconds for reader catch-up, so a cold 100K
snapshot export records its full load time instead of hitting the default
two-second freshness deadline. The correctness soak uses a 120-second HTTP client
timeout to include WAL waits during maintenance; measured delays remain visible.
Timed clients retry retryable 429 responses using `Retry-After`, retaining the
same idempotency key. Retry waiting is included in end-to-end ingest latency;
backpressure responses are reported separately and never count as published work.
The fixed host dataset includes an unindexed `loadtest_revision` field whose
deterministic update changes every batch, including the transition from warmup
to measurement. A run fails if the graph-version delta differs from the successful
batch count. This prevents no-op responses from inflating published throughput.
WAL readability timing includes terminal-status confirmation, 10 ms polling and
a successful `min_version` entity read; it is an upper bound on first visibility.

## Maintenance latency and memory admission

GC visits at most 512 candidates per batch and checks 50 ms / 16 MiB budgets between objects. Files still needed by older read views are deferred; new readers remain admitted. Later batches reconsider deferred files after those views finish. A single large object can exceed the budget; sustained long reads may delay reclamation.

GC checks read-view protection before decoding orphan index files, avoiding repeated validation under the tenant lock when a file cannot yet be reclaimed. Eligible files still undergo content and tenant validation; deletion rechecks read-view protection.

Incremental entity, forward-edge and reverse-edge pages use a reusable 64-partition entity directory and adjacency maps. Membership changes copy only affected directory partitions. Persisted layouts remain unchanged; each affected page is still rewritten in full. Pending deltas stop coalescing at 8192 changed IDs and continue in bounded publication order instead of forcing a full rebuild. Schema changes, catalog gaps and corrupt inputs can still require rebuilding.

Index health reports `updating: true` while a published graph is ahead of its queued or running index update. Automatic maintenance waits for that update instead of starting a redundant full rebuild and throttling writes. Failed updates remain eligible for repair on the next maintenance cycle. Explicit rebuild requests still run.

`GRAPHDB_MAINTENANCE_MAX_BYTES` defaults to `512MiB`. It is one shared estimate budget for active index/snapshot builds and queued asynchronous index graphs. Queue reservations transfer into execution without releasing and reacquiring memory. At most two builds and four partition encoding/write jobs run concurrently. An oversized build runs alone to preserve progress. On the retained-byte or eight-work-item limit, callers catch up synchronously after releasing the tenant lock.

These are admission estimates, not RSS limits. Caches, waiting requests and all encoding temporaries are not covered. Size the shared maintenance pool, caches and runtime overhead together with `GOMEMLIMIT` and container limits. Queueing can increase response latency; already-published data remains readable through version-checked graph fallback.

`graphdb_maintenance_phase_seconds` exposes `gc_wait_views`, `gc_wait_tenant`, `gc_hold`, `gc_sync`, `index_wait_previous`, `index_work`, `memory_wait`, `tenant_wait`, `tenant_hold`, and `file_batch_sync`. Phases can nest and must not be added as independent durations. `graphdb_maintenance_estimated_bytes{pool="active"}` and `{pool="pending_indexes"}` report charged estimates, not measured RSS.

Entity, edge, top-level adjacency and field-index maps use 256 copy-on-write buckets;
maps with at most 32 entries retain a compact representation. Published versions
share untouched buckets. Logical hashing stores SHA-256 leaf digests in
256 buckets per category, replaces only touched buckets and hashes a fixed 32 KiB
root. It no longer retains full per-entity JSON encodings or hashes the full graph
on each update. Cold graph/hash construction still scans the graph. The new API
field is `data_hash`; see [the exact contract](content-hash-v2.md).
