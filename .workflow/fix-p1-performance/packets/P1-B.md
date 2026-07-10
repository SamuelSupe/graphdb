# Packet P1-B: query and read path

## Objective

Remove full-result/full-graph materialization and repeated traversal/cache work from hot reads while preserving query ordering, cursor compatibility, budgets, and isolation.

## Context and ownership

- Own query execution, query/read HTTP orchestration, ReaderCache callbacks, entity-page cache, and persisted lookup iteration.
- Coordinate shared catalog/cache semantics with P1-D; do not weaken integrity or freshness checks.

## Do

- Add early-stopping entity visitors with cursor seek and rolling-upgrade shard ordering.
- Replace copied shortest-path histories with predecessor state and correctness-preserving deduplication.
- Decode/index edge shards once per request.
- Add callback-scoped immutable graph/page reads, bounded O(1) LRU caches, and request/cross-request catalog reuse gated by version.

## Do not

- Expose mutable cached maps, change public copy-returning APIs, skip cursor validation, or trust unvalidated persisted cache state.

## Expected output and verification

- Query/http/storage regressions for early stop, page 2, old 256-shard cursors, shortest-path histories, cache isolation/tamper handling, focused race tests, and ReaderCache benchmarks.
