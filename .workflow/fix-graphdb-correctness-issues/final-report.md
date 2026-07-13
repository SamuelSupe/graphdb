# Final Report: Fix GraphDB correctness issues

## Outcome

Completed. All six confirmed database correctness areas are fixed.

## Accepted Results

- GC rejects a checkpoint cursor inside the current sharded snapshot before deletion starts.
- Lifecycle metadata uses tenant locking, writer leases and CAS; purge is serialized and leaves a durable external tombstone.
- Disabled/deleted tenants are rejected by storage and HTTP maintenance mutations.
- Direct commits use pending/prepared/committed idempotency records and recover ambiguous manifest publication without a duplicate commit.
- Restore drills require an empty isolated target and verify exact object ownership before cleanup.
- Index rebuild completion uses a terminal marker barrier so a visible terminal task cannot be reused.

## Rejected Results

None.

## Conflicts Resolved

- The reader-heartbeat scan budget test now counts only heartbeat-path reads, preserving the 64-object limit while allowing lifecycle guard reads.
- RustFS does not support conditional delete; purge tombstone cleanup safely falls back to unconditional delete while holding the writer lease.

## Verification Evidence

- `go test -mod=readonly ./...`
- `go vet -mod=readonly ./...`
- `go test -race -mod=readonly ./internal/storage ./internal/httpapi ./internal/query`
- Index rebuild race regression with `-count=100`
- Targeted correctness race suite with `-count=20`
- OrbStack RustFS S3 integration with idempotency replay and purge/recreate
- `git diff --check`

## Remaining Risks

No known correctness blocker remains in the reviewed scope. The S3 integration uses a disposable test prefix and unique tenant identifiers.

## Reusable Follow-up

Packet definitions and accepted results remain under this workflow directory for future correctness audits.
