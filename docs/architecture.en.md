# GGraphDB 2.x Local Disk Architecture

[中文](architecture.md)

GGraphDB runs as one process on one machine, with concurrent reads and writes across tenants.
Parquet commits, snapshots and indexes remain the durable representation. An optional WAL
provides durable ingestion acceptance and replay. Object storage is an optional backup destination.

```mermaid
flowchart LR
  Client[HTTP / GraphQL / SDK] --> API[Protocol and admission]
  API --> Read[Storage query view selection]
  Read --> Query[Graph query execution]
  Query --> Cache[Bounded caches]
  Cache --> Files[Random file reads]
  API --> Write[Direct / WAL]
  Write --> Publish[Sync data then publish manifest]
  Publish --> Disk[Exclusive local directory]
  Files --> Disk
  API --> Tasks[Shared task executor]
  Maintenance[Maintenance scheduling] --> Tasks
  Tasks --> Disk
  Views[Active read views] --> GC[GC / restore / purge protection]
  GC --> Disk
```

## Writes and publication

Foreground publication is serialized per tenant; different tenants can proceed concurrently.
Referenced files and their directories are synchronized before manifest/catalog publication.
The pointer replacement is atomic; the preceding multi-file writes are not a transaction.
Partition encoding shares four slots and cooperative file-count, 16 MiB and 50 ms budgets.

Entities, edges, adjacency and field indexes use 256 copy-on-write partitions; small collections
of up to 32 entries use compact maps. Incremental partitioned SHA-256 produces `data_hash`.
WAL acceptance, grouped publication, idempotency and crash replay retain their existing protocol.
An accepted request is not yet a published graph version.

`TenantStore` publishes durable immutable graphs to attached read caches. Direct commits, WAL,
recovery and compaction use this shared path. HTTP handlers and WAL bootstrap no longer update
read caches separately. Publication does not depend on whether the writer cache retains the graph.
File replacement invalidates views, including restore to a lower or equal version.

## Reads and reclamation

Parquet reads use closable random-access files with column and row-group selection. Decode
admission and cache limits remain bounded. Buffered and streaming queries share storage-level
index selection, freshness constraints and failure backoff. Request metadata reuse is scoped to
the store and tenant; it does not replace active read-view protection.

GC retires unreferenced files against active read epochs and defers only reclamation needed by
older views. New queries can enter during GC. A changed manifest/catalog invalidates the batch
proof, including same-version index publication. Retirement bookkeeping is bounded; overflow
conservatively defers deletion. Restore, purge and explicit recovery/commit cleanup remain
exclusive lifecycle operations. `checkpoint.deferred_files` can be nonzero even when a scan
completed; maintenance retries reclamation. Caches share publication generations and immutable
graphs, while their retention budgets remain separate. Idle eviction replaces freshness polling.

## Tasks and directory ownership

New index rebuilds are ordinary `index_rebuild` tasks, sharing queue limits, cancellation, retry,
progress and terminal-state persistence with other operations. Index endpoints retain their
response shape. Existing separately stored index task records remain readable. Definition
changes can queue a replacement; progress from its predecessor cannot replace the active task.
Index cleanup yields execution capacity between batches.

The runtime registry identifies active local tasks. Cancellation signals the execution context
directly; local cancellation polling and GC heartbeats are unnecessary. After restart, orphaned
active records are marked failed when inspected/reconciled and can be retried using supported
checkpoints. This is not automatic replay of every task. WAL replay remains a separate protocol.

The directory lock excludes other processes. The persisted writer token/epoch fences previous
tenant generations; local operation does not depend on lease expiration or renewal. Conditional
writes, content checks and generation checks remain in place. Cached generation records are
validated using file metadata to avoid repeated Parquet decoding.

The runtime cancels and joins task, index and maintenance workers before releasing the data
directory. WAL shutdown is included and errors propagate to service/CLI callers. A worker timeout
retains directory ownership until a successful shutdown retry.

## Backup automation

The maintenance runner schedules durable ordinary backup tasks, with restart recovery, retry
backoff, full download verification, optional isolated restore drills and bounded S3 retention.
No separate scheduler or task execution engine is introduced. See [object backups](object-backup.md).

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `internal/httpapi` | HTTP/GraphQL protocol, tenant routing, admission, responses and tracing; external gateways handle authentication/authorization |
| `internal/storage` | Persistence, tenant generations, read views, publication, query index selection and task lifecycle |
| `internal/maintenance` | Scheduling and decisions using storage operations/budgets, with usage and audit callbacks |
| `internal/graph` / `internal/query` | Graph structures, planning and execution |
| `internal/backupstore` | Optional object-storage backup transport, outside online reads/writes |

Local file access retains narrow interfaces for testing and observation, random reads and bounded
directory scans. The unused single-writer wrapper is removed. Maintenance scheduling can run
without an HTTP server.

## Deployment and compatibility

The service and offline CLI exclusively own `GRAPHDB_DATA_DIR`. Use HTTP for maintenance while
the service is running. The layout remains `<prefix>/tenants/<tenant>/`, with manifest, commits,
snapshots, indexes, config and tasks. This simplification preserves released 2.0 Parquet/WAL data
and does not introduce another metadata encoding.

Remote primary storage, PostgreSQL coordination and split reader/writer modes are rejected.
There is no replication or failover protocol. See the [local disk guide](local-disk.md) for
configuration and validation, and [version boundaries](naming-and-compatibility.md) for the
2.0 break from 1.x data and the old MD5 contract.
