# Fix all P1 performance issues

## Goal

Remove every P1 performance issue identified in the 2026-07-10 review across graph apply/commit, indexes, query execution, read caches, maintenance, bounded caches, and collector status.

## Success Criteria

- Each reviewed P1 has a concrete code fix and regression coverage.
- Small mutations no longer perform the known quadratic affected-ID aggregation or duplicate index construction.
- Expected-version conflicts avoid loading the full graph.
- Hot reads and lazy queries avoid unnecessary full-graph/full-result materialization.
- Background maintenance defaults avoid repeated deep scans and no-op compaction.
- Long-lived caches, task workers, heartbeats, and collector status processing have explicit bounds or incremental behavior.
- Targeted benchmarks improve and `go test ./...`, `go vet ./...`, and race-sensitive focused tests pass.

## Current Context

- Branch `main`, baseline `1a84e2bd53c2f2f1dd50f211c8ecb3adf3b35c09`.
- Object-store-backed GraphDB with shared writer/reader binary modes.
- Existing untracked `internal/.DS_Store` is user-owned and must remain untouched.

## Constraints

- Keep files and implementations small; avoid unnecessary abstraction.
- Preserve public API, manifest visibility, cross-instance OCC, and content-hash semantics.
- Do not introduce a second browser/test harness; no frontend work is involved.
- Agents share the worktree and must not revert concurrent changes.

## Risks

- Graph immutability/caching changes can introduce aliasing or stale-read bugs.
- Maintenance default changes can reduce automatic detection if shallow/deep semantics are conflated.
- Cache eviction must not break object-store correctness or cross-instance visibility.
- Query early-stop changes must preserve ordering, cursors, and budget enforcement.

## Approval Required

No external, destructive, production, credential, deployment, or Git history action is planned. Local source and test edits are authorized by the implementation request.

## Work Packets

- P1-A: graph apply, commit hashing/cloning, and expected-version fast rejection.
- P1-B: query iteration, shortest-path traversal, adjacency reuse, and hot read/catalog reuse.
- P1-C: maintenance scheduling, compaction/GC/task bounds, and collector status incrementality.
- P1-D: duplicate index build removal and bounded memory/disk/list caches; root-owned integration.

## Integration Policy

- File ownership is disjoint where possible; shared config changes are coordinated through root.
- Accept fixes only with focused tests and clear correctness invariants.
- Root resolves overlaps and runs formatters after all packets land.

## Verification

- Focused unit tests and benchmarks per packet.
- `go test ./... -count=1`.
- `go vet ./...` and `git diff --check`.
- Repeat the affected-ID allocation benchmark and compare against baseline (~1.03 GB/op).
- Run race tests for changed cache/task components where practical.

## Reusable Artifacts

The workflow plan, packet results, and final report under this directory.
