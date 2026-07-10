# Packet WM-A: write-memory profile

## Objective

Read-only attribution of allocation volume, retained heap, and peak RSS for default and entity-record-enabled writes.

## Files / sources

Write benchmarks and graph/storage commit, index-build, Parquet, object-store, and cache paths.

## Ownership

No workspace edits.

## Do

- Run representative 10k-entity seed plus single-update profiles.
- Separate setup from measured commit where possible.
- Report allocation-space, in-use/live heap, object counts, CPU, and peak RSS.
- Identify correctness boundaries and rank safe optimization candidates.

## Do not

- Modify files or classify cumulative allocations as simultaneous memory without evidence.

## Expected output / verification

Concrete measurements, top call paths, and a P0/P1/P2 candidate ranking in `results/WM-A.md` or final message.
