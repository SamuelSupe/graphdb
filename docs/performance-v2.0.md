# GGraphDB 2.0 performance and validation

This report is being completed for the 2.0 release candidate. It is not yet a
performance acceptance statement. The main release remains gated on correctness,
backup/restore, mixed-load calibration and the 30-minute maintenance soak.

## Changes under validation

- 256-bucket copy-on-write entity/edge/adjacency and field-index tables for graph versions; small maps stay compact until they grow beyond 32 entries.
- Incremental `sha256-shards-v2` logical digest; no complete-graph MD5 per update.
- Reused entity partitions for incremental page/index updates.
- Shared queued/active maintenance memory admission, including protection
  against reserving memory behind an unreserved synchronous predecessor.
- Cooperative GC and file publication budgets with unchanged durability ordering.
- Ordinary maps for request-local lookup results; lazy bucket allocation for empty graphs.

## Current evidence

OrbStack Linux arm64, Go 1.26.7: full unit/vet/race, Python SDK, direct/WAL HTTP,
backup/restore and restart consistency passed for the first candidate. Focused
checks and CI are validating the final request-allocation adjustment.

The first 10K maintenance comparison had lower query throughput in the candidate
while host load increased. This is an unresolved result, not an improvement claim.
The final report will retain the measurements and calibration limits.

Use a new 2.0 directory. 1.x compatibility and migration are not release gates.
