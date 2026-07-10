# Orchestration: Optimize GraphDB write memory

## Execution rules

- Measure before changing code and distinguish default deployment behavior from optional modes.
- Treat PageETag, catalog content hashes, and manifest CAS as correctness boundaries.
- Prefer eliminating repeated materialization over pooling large buffers indefinitely.
- Reject optimizations that merely shift unbounded memory into a cache.

## Packet ownership

- WM-A is read-only and may inspect all write paths/profiles.
- WM-B owns entity-record serialization/write fanout and its focused tests/benchmarks.
- WM-C owns index artifact construction/builders and its focused tests/benchmarks.
- Root owns shared integration, workflow artifacts, graph/hash/cache files, and final verification.

## Branching rules

- If cumulative allocations are high but peak live memory is bounded and unavoidable, document rather than weaken integrity.
- If an optional-mode optimization needs a format migration, implement a backward-compatible encoding only when existing decoders can remain safe.
- If two packets touch the same file, stop one edit path and let root integrate the authoritative change.

## Completion audit

- Compare default and optional modes before/after.
- Verify no stale/tampered entity record can bypass page validation.
- Map each accepted change to a test and measured allocation/RSS effect.
- Confirm no user-owned files or unrelated worktree changes were removed.

## Execution Rules

- Keep the original objective intact.
- Ask for approval before risky, expensive, external, or destructive actions.
- Keep immediate blocking work local.
- Delegate only bounded, disjoint, materially useful packets.
- Integrate packet results before final verification.

## Branching Rules

## Packet Prompts

## Completion Audit
