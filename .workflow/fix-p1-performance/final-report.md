# Final Report: Fix all P1 performance issues

## Outcome

All nine P1 findings from the 2026-07-10 performance review are implemented and verified. Final integration review found no remaining P1 blocker.

## Accepted Results

- P1-A: ordered affected-ID sets, storage-only graph copy-on-write, in-place replay, compatible direct logical hashing, early ExpectedVersion rejection, and bounded writer graph caching.
- P1-B: lazy visitor scans with cursor seek, cross-version shard ordering, predecessor-based shortest path, request-local adjacency indexes, callback-scoped zero-copy reads, and version-gated catalog reuse.
- P1-C: shallow background health, no-op/low-progress compaction guards, paged resumable GC, bounded task admission/execution, stable bounded heartbeats, and incremental materialized collector status with one-time upgrade migration.
- P1-D: single-pass index artifacts, bounded O(1) memory caches, bounded disk/list/key/metadata caches, safe verification fast paths, and purge invalidation.

## Rejected Results

None. Packet changes were accepted only after focused regressions and integration review.

## Conflicts Resolved

- Cache verification state remains memory-only; disk hits must re-establish content integrity.
- Legacy 256-shard entity pages are merged into current 64-bucket scan order so old cursors cannot skip results during rolling upgrades.
- Missing collector checkpoints derive legacy history once under CAS, then all later restarts use the persisted incremental state.
- Heartbeat cleanup separates active-result capacity from total scan/delete work and fails closed while the bounded inventory is incomplete.

## Verification Evidence

- `go test ./... -count=1` passed.
- Full race runs passed for `internal/graph`, `internal/query`, and `internal/storage`; storage took about 396 seconds under race instrumentation.
- `go vet ./...` and `git diff --check` passed.
- `BenchmarkApplyManyToManyEdges`: 5.85-6.18 ms/op and about 10.55 MB/op, versus the review baseline of 622-671 ms/op and about 1.03 GB/op.
- `BenchmarkReaderCacheHotRead`: isolated copy 1.11 ms/op, 3.31 MB/op, 14,060 allocs; callback read view 155 ns/op, 0 B/op, 0 allocs.
- Production-default `BenchmarkSingleEntityIndexedCommit` at 10,000 entities: 114-118 ms/op and about 203 MB/op.

## Remaining Risks

These are P2 follow-ups, not unresolved original P1s:

- A storage mutation still copies top-level maps, computes one full next-content hash, and constructs full logical index artifacts once; persistent maps, incremental hashes, and affected-shard construction need format/architecture work.
- `GRAPHDB_INDEX_ENTITY_RECORDS=true` (disabled by default) rewrites per-entity Parquet records when a physical page pack ETag changes; the profiled 10,000-entity fixture reached about 2.2 seconds and 18.7 GB allocated per update.
- Strict fresh reads intentionally perform one manifest probe per request; first Parquet object decode remains proportional to the physical page/shard, and constrained stepped shortest paths can retain an antichain of incomparable histories.
- Writer graph caching is tenant-count bounded rather than byte weighted. Task-list/history scans and explicitly unbounded manual GC/index-orphan cleanup remain operator/control-plane scale debt.

## Reusable Follow-up

Keep the three focused benchmarks as regression guards. The next highest-value optimization is affected-shard index construction plus a lighter optional entity-record representation, followed by byte-weighted writer graph eviction and bounded task-history listing.
