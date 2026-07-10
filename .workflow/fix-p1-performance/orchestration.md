# Orchestration: Fix all P1 performance issues

## Execution Rules

- Keep the original objective intact.
- Ask for approval before risky, expensive, external, or destructive actions.
- Keep immediate blocking work local.
- Delegate only bounded, disjoint, materially useful packets.
- Integrate packet results before final verification.

## Branching Rules

- If a proposed optimization changes externally visible ordering or hash semantics, preserve semantics first and record the remaining optimization.
- If a cache cannot safely trust local state across writers, validate the immutable versioned key rather than disabling freshness checks globally.
- If a P1 needs a broad format migration, implement the safest bounded/incremental improvement now and document the residual migration risk.
- Run focused tests after each packet, then integrate and run full verification once.

## Packet Prompts

- P1-A owns `internal/graph` and commit/load files explicitly assigned in its prompt.
- P1-B owns `internal/query`, query HTTP orchestration, and reader cache files explicitly assigned in its prompt.
- P1-C owns maintenance, GC/task scheduling, heartbeat, and collector status files explicitly assigned in its prompt.
- P1-D owns index refresh/build and object/list/disk cache files.

## Completion Audit

- Map every original P1 to accepted code and tests.
- Confirm no user-owned files were removed.
- Record benchmark deltas and any remaining non-P1 follow-ups.
