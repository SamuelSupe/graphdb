# Packet WM-C: default index-build memory

## Objective

Reduce production-default single-update allocation/live memory from full logical index artifact construction and changed object serialization.

## Files / sources

`internal/storage/index_build_artifacts.go`, index builders/hash helpers, and newly added focused tests/benchmarks. Avoid entity-record writer files.

## Ownership

May edit the files above and new narrowly scoped helpers/tests. Do not revert concurrent work.

## Do

- Profile which artifact copies/sorts/hashes coexist.
- Reuse immutable graph indexes or release/stream artifacts phase-by-phase where deterministic catalog hashes permit it.
- Keep catalog/object ordering and content hashes byte/logically compatible.

## Do not

- Implement a broad new cache framework or weaken rebuild/refresh correctness.

## Expected output / verification

Measured default-mode improvement, focused compatibility tests, and `results/WM-C.md`.
