# Storage modules

`storage` coordinates tenant state and persistence. Its child packages own logic
that can run without a tenant store, filesystem runtime or publication lock.
Existing callers continue to use `storage` public types and constructors; the
`*_api.go` files retain these entry points through aliases and thin wrappers.

| Package | Owns | Dependencies within storage |
| --- | --- | --- |
| `identity` | Tenant validation, escaped path segments, commit IDs | None |
| `ingest` | Request/result models, transactional options, preconditions, graph mutation conversion, conflict attribution | `identity` |
| `parquetio` | Decode admission, Arrow column validation, value conversion, timestamp formatting, writer settings | None |
| `codec` | Commit tables, graph component rows, UTC normalization, layout validation, legacy wire hashes | `ingest`, `parquetio` |
| `admission` | Pressure sampling, threshold decisions, retryable pressure errors | None |

These packages must not import the parent `storage` package. They do not acquire
tenant locks or publish persistent state. `codec` uses `ingest` models when
normalizing timestamps for stored requests; it does not depend on ingestion
scheduling. `parquetio` owns one process-wide decode limit, including the
reserved control-metadata slot, across all codecs and storage callers.

## Coordination in the parent package

The parent retains operations whose correctness depends on shared tenant state:

| Files | Responsibility |
| --- | --- |
| `ingest_service.go`, `ingest_accept.go`, `ingest_service_status.go` | Service lifecycle, durable acceptance and client-visible status |
| `ingest_scheduler.go`, `ingest_flush*.go`, `ingest_pending.go` | Per-tenant dispatch, flush publication, WAL transitions and completion |
| `ingest_wal.go`, `ingest_wal_writer.go`, `ingest_wal_replay.go`, `ingest_wal_checkpoint.go`, `ingest_wal_durability.go` | WAL ownership, appends, replay, reclamation and durable boundaries |
| `ingest_batch*.go` | Locked batch orchestration, apply, metadata and manifest publication |
| `ingest_store.go`, `ingest_attempt.go`, `collector_status_store.go` | Batch identity records, failed attempts and collector progress |
| `cache.go`, `cache_load.go`, `cache_revalidate.go` | Reader-cache state, shared loads and generation revalidation |
| `parquet_entity.go`, `parquet_entity_columns.go`, `parquet_entity_lookup.go` | Entity-page persistence, physical schema and version-aware lookup |

`codec.CommitBuilder` exposes decoded upserts in their original ordinal order.
Sparse ordinals remain sparse, which is required to reconstruct mixed ingest
items. Entity and edge visitors return copies so a consumer cannot mutate the
builder's retained graph values. Column accessors expose metadata without
exposing the builder's maps or Arrow column fields.

This organization does not change HTTP/SDK contracts, object paths, layout
versions, JSON/Parquet/WAL encodings, error identities, locking or publication
order. Update the existing orchestration tests when changing those contracts.

## Validation

Run the child packages as well as the parent; `./internal/storage` alone excludes
the child-package tests:

```sh
go test -mod=readonly ./internal/storage/...
go test -mod=readonly -race ./internal/storage/...
go vet -mod=readonly ./...
```

Use OrbStack/Linux for backend execution. The full repository checks in
`CONTRIBUTING.md` also cover storage callers and compatibility boundaries.
