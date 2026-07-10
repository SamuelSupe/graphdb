# Packet P1-C: maintenance and control-plane bounds

## Objective

Bound recurring maintenance work, prevent no-progress compaction loops, control task/heartbeat growth, and make collector status incremental by default.

## Context and ownership

- Own maintenance HTTP/command/config, GC/checkpoints, S3 paging, task admission, reader heartbeat, collector status, and related docs/tests.
- Preserve explicit deep health and operator request shapes.

## Do

- Use shallow background health, skip already-compacted/no-meaningful-tail work, and page/resume GC with bounded deletes.
- Bound task queue/execution and persist queued cancellation.
- Stabilize and rate-limit heartbeat IDs/writes; page, expire, and fail closed on incomplete active-reader scans.
- Default collector status to materialized, migrate missing legacy checkpoints once with CAS, then update incrementally.

## Do not

- Make GC unsafe around active readers, silently accept incomplete inventories, or lose legacy collector totals during upgrade.

## Expected output and verification

- Focused GC/task/heartbeat/migration/CAS tests, package tests, race checks, vet, diff check, and updated operator documentation.
