# P1-C Result: Maintenance and Control-Plane Bounds

## Outcome

Implemented the maintenance, GC, task, heartbeat, and collector-status P1 fixes without changing the public HTTP request shapes.

## Changes

- Background index monitoring and auto-rebuild decisions now use shallow manifest/catalog health checks. Explicit index-health requests retain deep validation.
- Auto-compaction skips tenant-usage listing when the current version is already fully snapshotted. After a snapshot, historical object/byte heuristics require at least 16 new tail commits before they can retrigger; the explicit tail threshold still takes precedence. `Compact` returns the existing manifest for an already-compacted graph.
- Maintenance GC uses a 256-delete budget, persists its cursor between loop iterations, and immediately resumes paused work instead of waiting for the normal GC interval.
- GC scans loose commits, commit segments, index tasks, ingest/deadletters, snapshots, and unified tasks in raw-key lexical phase order. S3 uses native `start-after`/`max-keys` pages; no-delete pages also pause and advance the cursor. Loose commit paging stops before nested segment keys so the segment phase cannot skip them.
- Snapshot retention uses bounded lookahead for the newest retained snapshots and paged deletion of sharded snapshot objects. Active readers conservatively defer snapshot retention.
- Unified and legacy index tasks share a bounded queue (128), global execution limit (4), and per-tenant striped execution slots. Same-type local tasks are deduplicated, and cancellation while queued is persisted.
- Reader heartbeat IDs are stable across service restarts when no explicit instance ID is configured, heartbeat writes are rate-limited for unchanged state, and expired reader objects are deleted. Active-result capacity and total scan/delete work have separate bounds; GC fails closed when a bounded page cannot establish the complete active-reader set.
- Collector status is materialized by default, avoiding cold full-history derivation in normal operation. A missing checkpoint derives old disabled-mode history once and persists it with CAS; later restarts remain incremental. The compatibility derivation path streams records with constant aggregation memory instead of sorting all records. Collector-status and heartbeat process caches have explicit capacity/TTL bounds.

## Focused Regression Coverage

- `TestGCPausesAfterBoundedNoDeletePage`
- `TestGCResumesAllNestedCommitSegmentPages`
- `TestGCBoundsCommitDecodeWorkWhenDeleteBudgetSet`
- `TestQueuedTaskCancellationPersistsBeforeExecutionSlot`
- `TestTaskAdmissionDeduplicatesAndBoundsQueue`
- `TestReaderHeartbeatWritesAreRateLimitedAndStateSensitive`
- `TestListReaderHeartbeatsDeletesExpiredRecords`
- `TestReaderHeartbeatLegacyCleanupHasTotalWorkBudget`
- `TestGCFailsClosedWhenHeartbeatInventoryExceedsScanBudget`
- `TestCollectorStatusCacheIsBoundedAndExpires`
- `TestCollectorStatusMaterializationMigratesDisabledHistoryOnce`
- `TestAutoCompactSkipsUsageForAlreadyCompactedManifest`
- `TestMaintenanceHistoricalObjectsDoNotRetriggerCompactForOneNewCommit`
- `TestS3ListPageUsesStartAfterAndLimit`

## Verification

- `go test ./internal/storage ./internal/httpapi ./internal/config ./cmd/graphdb -count=1` — passed.
- Focused GC, task, heartbeat, cache, maintenance, and S3 paging tests — passed.
- Focused `go test -race` for task/cache/heartbeat/GC state — passed.
- `go vet ./internal/storage ./internal/httpapi ./internal/config ./cmd/graphdb` — passed.
- `git diff --check` — passed.

## Residual Risks

- Task admission is process-local; cross-instance mutation exclusion continues to rely on the existing writer lease and object-store CAS rules.
- Active readers intentionally defer snapshot deletion, favoring safe over-retention over an expensive full snapshot scan.
- Native metadata paging is implemented for S3. Object-store implementations without optional paging use the compatibility `List` fallback, while decode/delete work remains bounded.
- Checkpoint/dry-run GC continues to skip the existing unbounded index-orphan cleanup path; an explicit non-checkpoint GC is still required for that cleanup.
