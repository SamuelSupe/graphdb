# Packet 06: tenant generation fencing

## Objective

Make purge/recreate a hard generation boundary for every tenant-scoped mutation, including task progress, GC markers, config/query/index metadata, and destructive maintenance.

## Do

- Add backward-compatible persistent generation identity.
- Validate generation immediately before tenant-scoped Put/Delete operations.
- Stop old task heartbeats/progress from recreating objects after purge.
- Make purge drain or detect late writes before declaring completion.
- Add deterministic multi-store race tests spanning lease expiry, purge, late release, and recreate.

## Do not

- Depend on process-local locks for cross-instance correctness.
- Add a remote tombstone GET to every hot request when a validated generation can be propagated.

## Verification

Focused lifecycle/task/fencing tests, race tests, and S3 conditional-write integration.
