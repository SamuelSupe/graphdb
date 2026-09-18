# Local disk operation

This edition runs one process with concurrent tenant workloads. The process owns
`GRAPHDB_DATA_DIR`; graph data, control metadata, background tasks and backups are
local files. PostgreSQL and remote online-storage backends are unavailable.
Optional [S3-compatible snapshot backups](object-backup.md) support recovery after local disk loss.

## Configuration

| Setting | Default / contract |
| --- | --- |
| `GRAPHDB_DATA_DIR` | `.graphdb`; persistent local directory |
| `GRAPHDB_PREFIX` | `graphdb`; existing tenant layout retained |
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
write jobs, using four workers, then sync each affected directory once before
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
references trigger a rebuild. `committed` still means durable, readable graph
data. Queries use a graph view meeting `min_version` when indexes lag. The
`/v1/commits` API still waits for its index update and shares that ordering chain. GC, restore, and directory close
wait for those file references. A crash can leave indexes behind the durable
graph head; it does not discard published graph commits.

## Upgrade and recovery

Stop the previous service and back up its data directory before opening it with
this edition. Existing local Parquet and WAL formats are retained. A PostgreSQL
coordination marker causes startup to fail: remote data migration and coordinated
head conversion are outside this change. Do not delete that marker to force entry.

Use the backup/restore and integrity-audit HTTP APIs. To recover after disk loss,
configure [object-storage snapshots](object-backup.md) and create a backup with
`destination: object`. The existing same-directory backup remains available.
Roll back binaries only while stopped, using the retained directory backup.

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
filesystem. Existing tasks and local backups are preserved. A durable switch
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

Start performance work with one representative comparison and repeat only to
investigate a measured regression. `scripts/local_disk_optimization.py` compares
before/after local binaries using copies of the same stopped database, covering
direct/WAL mixed load, export, compaction, backup/restore, and process-cold reads.
See the [optimization measurements](performance-local-disk-optimization.md).
The [second optimization round](performance-local-disk-optimization-2.md) covers
catalog hashes, entity pagination, HTTP encoding, Parquet row locality and incremental indexes.

`scripts/release_gate.sh` runs unit/vet/race/compatibility/SDK checks, then direct
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
