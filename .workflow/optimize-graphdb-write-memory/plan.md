# Optimize GraphDB write memory

## Goal

Identify write paths that create excessive live or cumulative memory pressure and reduce them without weakening graph isolation, manifest CAS, index integrity, or reader compatibility.

## Success criteria

- Measure production-default and `GRAPHDB_INDEX_ENTITY_RECORDS=true` writes separately.
- Attribute cumulative allocations and peak RSS/live heap to concrete write phases.
- Remove avoidable whole-graph, whole-index, packed-page, or per-record materialization where the storage contract permits it.
- Add regression benchmarks/tests that pin the optimized behavior.
- Pass focused tests, full `go test ./...`, race checks for changed concurrent code, `go vet`, and `git diff --check`.

## Current context

- Work continues on top of the uncommitted P1 performance fixes in the shared worktree.
- The production default disables entity-record indexes, while `NewTenantStore` enables them for broad storage test coverage.
- Existing untracked `internal/.DS_Store` is user-owned and must remain untouched.

## Constraints

- Keep modules focused and implementations simple.
- Preserve logical content hashes, immutable object/version semantics, manifest publication, PageETag tamper protection, and public API behavior.
- Do not hide an expensive optional mode by benchmarking only the default.

## Risks

- Skipping entity-record siblings after a packed-page ETag change can weaken integrity or make point lookups stale.
- Streaming index construction may change deterministic ordering or hashes.
- Allocation totals are not the same as simultaneous RSS; both must be checked.

## Approval required

No destructive, external, deployment, credential, or Git-history action is planned. Local source/test/benchmark edits are within the requested optimization.

## Work packets

- WM-A: read-only phase/RSS profiling and correctness guardrails.
- WM-B: optional entity-record serialization/fanout optimization.
- WM-C: production-default index artifact/build memory optimization.
- Root: graph/hash/cache audit, integration, benchmarks, and full verification.

## Integration policy

Packet file ownership is disjoint. Root accepts only measured improvements with unchanged compatibility/integrity tests.

## Verification

Focused benchmarks with `benchmem`, allocation profiles, peak-RSS/live-heap sampling, package/full tests, race checks, vet, and diff checks.

## Reusable artifacts

Packet findings, before/after measurements, and the final report in this workflow directory.

## Goal

## Success Criteria

## Current Context

## Constraints

## Risks

## Approval Required

## Work Packets

## Integration Policy

## Verification

## Reusable Artifacts
