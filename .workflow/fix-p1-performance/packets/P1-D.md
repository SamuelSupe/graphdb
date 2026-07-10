# Packet P1-D: index construction and bounded caches

## Objective

Eliminate duplicate full index construction and place explicit entry/byte/age bounds on long-lived object, metadata, list, and disk caches.

## Context and ownership

- Own index artifact build/refresh, raw index-object memory/disk caches, writer list/object/key/metadata caches, and purge invalidation.
- Coordinate decoded page/catalog behavior with P1-B and preserve cross-writer integrity.

## Do

- Build catalog, secondary indexes, edge shards, and entity pages once per refresh/rebuild.
- Use O(1) LRU bookkeeping, memory/disk byte and entry ceilings, TTL pruning, and atomic disk writes.
- Charge list metadata against cache capacity; avoid retaining partial/oversized lists.
- Keep successful hash-verification state process-local, revalidate remote metadata on the configured interval, and clear all tenant cache state on purge.

## Do not

- Persist trusted verification bits, reuse a catalog across a mismatched strict version, or turn cache misses into correctness failures.

## Expected output and verification

- Cache-bound, disk-pruning, tamper, catalog, rebuild, purge, and writer-list regressions plus production-default indexed-commit benchmark coverage.
