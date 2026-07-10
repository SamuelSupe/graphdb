# Packet WM-B: optional entity-record memory

## Objective

Reduce the extreme allocation fanout of `WriteEntityRecords=true` while keeping record/page/content/ETag integrity and old-record compatibility.

## Files / sources

`internal/storage/entity_record_store.go`, `parquet_entity_record.go`, narrowly related helper/tests/benchmarks. Coordinate before touching `index_delta_write.go`.

## Ownership

May edit only the files above and newly added focused test/helper files. Do not revert concurrent work.

## Do

- Evaluate batching records into fewer Parquet encoders/objects, lazy serialization after equality checks, or another backward-compatible low-allocation path.
- Preserve existing record keys and decoder compatibility unless a dual-format migration is fully covered.
- Measure enabled-mode before/after at a survivable fixture and 10k when practical.

## Do not

- Skip sibling updates solely because their entity payload is unchanged if PageETag validation would become stale.
- Disable content/tamper validation.

## Expected output / verification

Focused code/tests, before/after allocations, race safety where concurrent workers remain, and `results/WM-B.md`.
