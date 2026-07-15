# Final Report: GraphDB correctness and performance hardening

## Outcome

All correctness and performance findings from the renewed review are fixed on `codex/fix-correctness-performance`. Purge/recreate is a hard generation boundary, public content identity remains compatible, indexed commits do mutation-sized work, and cache/admission/cursor hot paths avoid the identified excess work.

## Correctness Results

- Lease and manifest Parquet records carry a backward-compatible monotonic fence epoch. Purge state persists outside the tenant prefix and preserves the last epoch across recreate.
- Operation-scoped fences are propagated into ordinary and index task goroutines. Old tasks, heartbeats, metadata writes, commits, and maintenance operations cannot adopt a recreated tenant; overwrite restore is the one explicit, tested generation switch.
- Mutable tenant objects use CAS plus pre/post fence validation. Native S3 `DeleteObject If-Match` is implemented, with conflict mapping and safe fallback behavior for stores without conditional delete.
- GC/task state is CAS-safe and recoverable. A heartbeat-backed active marker replaces history scans in write admission, and concurrent cancellation cannot be overwritten by progress.
- Cross-prefix migration acquires the target writer fence, preserves that lease during overwrite, and rebinds the copied manifest to the target generation.
- `data_md5` remains the exact 32-hex MD5 of canonical logical graph content. Legacy manifests decode without the optional persisted value, while the internal incremental fingerprint is mutex-safe.

## Performance Results

- Single-entity commits update only affected secondary groups, edge shards, entity pages, and records; unchanged physical objects are reused.
- Write-cache accounting includes variable-length logical content and remains overflow-safe.
- Write admission performs one authoritative read per active marker after lease acquisition and no historical task scan.
- Cursor pages reuse a bounded compiled catalog containing sorted starts, maps, targets, and content hash.

## Verification Evidence

- `go test -mod=readonly ./... -count=1` and `go vet -mod=readonly ./...` passed.
- Full storage race passed in 87.136 seconds; graph, HTTP API, query, and migration race coverage also passed.
- The 10k-entity indexed single-entity benchmark ran at 25.2–26.1 ms/op, 38.11–38.17 MB/op, and about 337k allocs/op. The recorded same-machine `main` baseline was 36.6–38.3 ms/op, 56.27–56.34 MB/op, and about 506k allocs/op.
- OrbStack RustFS `TestS3StoreIntegration` passed, including conditional writes/deletes, purge, and recreate.
- Diff hygiene and dynamic-workflow validation passed.

## Intentional Boundaries

- Status-only readers may observe lifecycle changes for up to `LifecycleCacheTTL` (one second by default); writer acquisition is strongly checked.
- A graph first loaded from a legacy snapshot may perform one full fingerprint walk; later commits maintain the value incrementally.
- Purge and validated immutable-index cleanup retain specialized direct-delete loops because their owning fence/content checks are stronger than a generic mutation wrapper.
