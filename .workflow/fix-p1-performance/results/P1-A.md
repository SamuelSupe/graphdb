# P1-A: graph apply and write-path performance

## Result

- Replaced repeated `appendUnique` rebuilding for `ApplyReport.AffectedEntityIDs` with one insertion-ordered set per apply. The existing 4,000-entity/2,000-edge benchmark improved from the review baseline of 622-671 ms and about 1.03 GB allocated per operation to 6.1-6.9 ms and about 10.0 MB allocated per operation.
- Kept the public `Graph.ApplyCommit*` atomic/deep-copy contract unchanged. The storage commit path now uses a private mutation copy with shallow graph maps and copy-on-write index buckets; unchanged entity/edge payloads are shared only between immutable storage snapshots.
- Manifest replay now applies commits in place to the private graph under construction, eliminating one full graph clone per replayed commit.
- `ContentMD5` now builds its logical snapshot directly instead of first cloning a full snapshot and all indexes. Its old snapshot-based encoding is covered by a compatibility test.
- The writer cache carries the last computed logical MD5, so hot commits hash the previous graph once and reuse that value on later commits; the next graph is still fully hashed to preserve no-op/content semantics.
- `ExpectedVersion` now reads and checks the current manifest before loading snapshot/commit history. A matching cached graph is reused only when the manifest ETag and read set match; manifest CAS remains the final cross-instance guard.
- Bounded the full-Graph writer cache to 64 tenants by default with tenant LRU eviction. Delete/purge invalidation also removes LRU state.

## Files changed

- `internal/graph/apply.go`
- `internal/graph/cardinality_benchmark_test.go`
- `internal/graph/content_hash.go`
- `internal/graph/content_hash_test.go`
- `internal/graph/copy_on_write.go`
- `internal/graph/edge_merge.go`
- `internal/graph/entity_stale.go`
- `internal/graph/graph.go`
- `internal/graph/index.go`
- `internal/graph/storage_apply_test.go`
- `internal/graph/unique_strings.go`
- `internal/storage/tenant.go`
- `internal/storage/tenant_commit.go`
- `internal/storage/tenant_load.go`
- `internal/storage/write_cache.go`
- `internal/storage/write_cache_test.go`

## Verification

- `go test ./internal/graph -count=1`
- `go test ./internal/storage -run 'Test(Commit|Expected|WriteCache|DeleteWriteCache|TenantStoreSegmentsCommitTailAndLoadsAfterLooseCleanup|TenantStoreLoad)' -count=1`
- `go test -race ./internal/graph -count=1`
- Focused storage race tests for commit replay, ExpectedVersion, and write-cache LRU.
- `go vet ./internal/graph ./internal/storage`
- `git diff --check`
- `go test ./internal/graph -run '^$' -bench 'BenchmarkApply(ManyToManyEdges|SingleEntityStorageCopy)$' -benchmem -count=3`

## Residual risks

- A storage mutation copy still copies top-level graph maps, so a one-entity update remains O(entity/edge map size), but it no longer deep-copies every payload or every nested index bucket. Removing that final O(N) map copy would require a persistent/overlay graph representation and is intentionally outside this focused change.
- The next logical graph still requires a full content hash for exact no-op detection. Only the unchanged previous hash and the unnecessary index/snapshot cloning were removed; an incremental hash would require a versioned hash scheme and migration plan.
- Full `internal/storage` currently has unrelated failures in index-tamper/repair tests while the concurrent index-cache packet is changing hit validation. P1-A's focused suites pass; integration must reconcile those cache tests before completion.
