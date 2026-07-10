# WM-A result: write-memory profile

Status: complete

## Method

Measured a 10,000-entity seed/rebuild followed by one entity update with the setup excluded from write deltas. A 1 ms sampler recorded `runtime.MemStats`, live heap, heap-in-use, process RSS, GC count, and post-GC retention. Both production-default `WriteEntityRecords=false` and optional `true` modes used FileStore; `benchmem` cross-checks used MemoryStore.

The profiles distinguish cumulative allocations from simultaneous memory. The original 18.8 GB number was allocation churn across hundreds of GCs, not an 18.8 GB live heap.

## Baseline findings

- Default: 210.0 MB cumulative allocation, 1.457M mallocs, 42.2 MB peak live-heap delta, 38.7 MB heap-in-use delta, and 9 GCs.
- Entity records enabled: 18.80 GB cumulative allocation, 107.14M mallocs, 79.3 MB peak live-heap delta, 75.9 MB heap-in-use delta, 526 GCs, and 16.51 seconds on FileStore.
- The optional mode updated 2,022 per-entity Parquet files because one logical page shared a physical pack with sibling pages and every record bound the pack ETag.
- Default allocation hotspots were index refresh/catalog/page hashing and Parquet generation. Optional-mode hotspots were one-record Parquet encode/decode and page-pack fanout.

## Final integrated measurements

| Metric | Default baseline | Default final | Entity-record baseline | Entity-record final |
| --- | ---: | ---: | ---: | ---: |
| cumulative allocation | 210.0 MB | 162.7 MB | 18.80 GB | 227.3 MB |
| malloc count | 1.457M | 1.205M | 107.14M | 1.462M |
| GC cycles | 9 | 8 | 526 | 11 |
| peak live-heap delta | 42.2 MB | 36.5 MB | 79.3 MB | 32.6 MB |
| heap-in-use delta | 38.7 MB | 33.2 MB | 75.9 MB | 29.4 MB |
| RSS delta | 0.49 MB | 0.46 MB | 9.16 MB | 0.20 MB |
| post-GC retained write delta | 11.9 KB | 11.2 KB | 260 KB | 14.1 KB |
| FileStore wall time | 213 ms | 155 ms | 16.51 s | 0.98 s |

The optional mode therefore reduced cumulative bytes by 98.8%, allocation count by 98.6%, peak live heap by 58.9%, heap-in-use by 61.2%, RSS growth by 97.9%, and wall time by 94.1%.

The final writer no longer populates the decoded page read cache while publishing immutable pages; the post-seed page-cache count is zero in both modes. This removed a measured 16 MB true-mode retained-heap regression from un-packed pages. Readers populate the bounded cache on demand.

## Accepted safety decisions

- Kept all record siblings that share a physical ETag synchronized; directly writing only the mutated entity was rejected as unsafe under the current PageETag contract.
- Instead, entity-record mode uses one physical object per logical entity page, limiting invalidation to that page.
- Kept four-worker record-write concurrency and optimized encoder cost rather than moving unbounded buffers into a pool/cache.
- Added conservative byte weights to retained writer graphs and decoded entity pages.

## Residual

First-time materialization of 10,000 optional by-ID Parquet objects still has fixed per-object schema/column cost. Eliminating that remaining cold-build churn requires a versioned compact record format or packed-record lookup migration; it is not needed for the now-bounded hot single-update path.
