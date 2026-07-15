# Packet 09: cache, admission, and scan hot paths

## Objective

Restore conservative cache-byte accounting, coalesce authoritative task-marker reads per commit, and reuse compiled catalog ordering/maps/hash across cursor pages.

## Verification

Large-field cache eviction tests, object-store operation-count tests, cursor snapshot tests, and allocation benchmarks.
