# Orchestration: GraphDB correctness and performance hardening

## Execution Rules

- Keep the original objective intact.
- Ask for approval before risky, expensive, external, or destructive actions.
- Keep immediate blocking work local.
- Delegate only bounded, disjoint, materially useful packets.
- Integrate packet results before final verification.
- Preserve `internal/.DS_Store` and unrelated user changes.
- Add a failing regression test before or together with each correctness fix.
- Prefer a small persistent marker or epoch over repeated prefix scans.

## Branching Rules

- A packet advances only after its focused tests pass.
- If a design requires an incompatible object-format migration, stop and replace it with a backward-compatible defaulted field/key.
- If a benchmark regresses unrelated hot paths, revert only that packet and document the rejected approach.

## Packet Prompts

- Packet 01 owns lease, tenant lifecycle, tombstone and lifecycle-cache files.
- Packet 02 owns unified task state, GC active marker and backpressure task lookup.
- Packet 03 owns graph mutation reports/content identity and commit no-op detection.
- Packet 04 owns entity/edge scan cursor start and catalog lookup helpers.
- Packet 05 owns integration verification and workflow reports; it does not change product behavior.
- Packet 06 owns tenant generation propagation, non-manifest mutation fencing, task/heartbeat fencing, and purge drain semantics.
- Packet 07 owns `data_md5` compatibility and fingerprint publication/concurrency safety.
- Packet 08 owns mutation-scoped index artifact updates and their benchmarks.
- Packet 09 owns cache byte accounting, write-admission marker reads, and cursor catalog compilation/hash reuse.
- Packet 10 owns renewed full verification and replaces the stale final report.

## Completion Audit

- Every original finding maps to a code change and regression test.
- No running marker can remain authoritative without a live lease/heartbeat.
- No destructive loop removes its own synchronization primitive before completion.
- No request/commit hot path performs an unbounded historical prefix scan.
- Repository diff contains no unrelated files.
- A completed purge remains complete even when an old request is released after purge; no tenant-prefix object is recreated.
- Same logical content produces the same public `data_md5` across legacy and new commit paths.
- The single-entity indexed benchmark has mutation-sized work rather than full-graph allocation growth.
