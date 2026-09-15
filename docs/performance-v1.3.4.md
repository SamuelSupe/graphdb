# GGraphDB 1.3.4 performance report

This report records the bounded source-level optimizations included in the
v1.3.4 release candidate. It is not an end-to-end capacity or production SLO
claim.

## Changes

- Local commit and ingest paths publish the graph first, release the tenant
  foreground lock, and perform incremental index refresh outside that lock.
  A per-tenant ordered refresh chain preserves catalog version order.
- Secondary and edge Parquet immutable objects use a bounded four-worker pool.
  Sharded snapshot entity pages and edge shards use the same bounded shape for
  writes and cold loads, with catalog entries and final graph slices retaining
  deterministic order.
- Query streaming keeps the first item visible immediately and batches later
  response flushes. The logical MD5 path reuses a 64 KiB buffered writer.
- Worker panics are returned as ordinary errors so task recovery can persist a
  failed terminal state.

## Verification

The candidate was copied over a clean `git archive HEAD` source tree and tested
inside the OrbStack `golang:1.25-bookworm` Linux/arm64 environment with 8 CPUs,
8 GiB memory, read-only module cache, and `GOTOOLCHAIN=local`.

- `go test -mod=readonly ./internal/graph ./internal/storage ./internal/httpapi ./internal/query`
- Focused storage tests covering concurrent index refresh, snapshot load/compact,
  index rebuild concurrency, and panic recovery
- Targeted `go test -race` for the same storage concurrency boundaries
- `go test -mod=readonly -run=^$ ./...`
- `git diff --check`

A three-iteration process-local `BenchmarkIncrementalIndexedEntityCommit10K`
sample measured about 24.1 ms/op in this environment. Earlier samples varied
substantially, so this report does not claim a stable single-commit throughput
improvement from that benchmark alone.

## Boundaries

The release-candidate checks above do not replace the repository's static
release gate, Python SDK checks, RustFS/PostgreSQL integration, 30-minute CAS
soak, rollback rehearsal, or production object-store measurements. Existing
untracked duplicate files in the developer worktree were preserved and were
not included in the clean validation tree or release commit.
