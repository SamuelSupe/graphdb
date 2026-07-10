# P1-D: index construction and bounded caches

## Result

- Added one `indexBuildArtifacts` result that carries the catalog, secondary indexes, edge shards, and entity pages. Commit refresh and explicit rebuild now write those same artifacts instead of rebuilding every full-graph index twice.
- Replaced the raw index-object cache's linear slice LRU with O(1) list bookkeeping and added a 512 MiB default memory ceiling.
- Added a bounded disk cache with entry, byte, and age limits (default 4 GiB and seven days), startup pruning, and atomic temporary-file writes.
- Configured hot index/entity caches use a 30 second metadata revalidation window, while unconfigured stores retain strict per-hit validation for integrity-sensitive callers and tests. Successfully hash-validated memory entries carry a process-local verification bit so hot reads avoid repeating a full content hash; that bit is deliberately never persisted to disk.
- Writer object-list metadata is charged against `MaxBytes`; oversized lists are not retained, partial prefixes are not cached, object copies/sorts moved out of the global lock where safe, and writes update only cached directory ancestors.
- Bounded object-key/prefix and writer metadata caches. Tenant purge now clears related local metadata, lease, object-key, and writer object-cache entries.
- Added version-gated cross-request catalog reuse. Strict reads only accept the manifest-established version; explicitly unconstrained stale reads may reuse any cached catalog.

## Verification

- Focused writer-object, index-object, disk-cache, object-key, index-catalog, rebuild, and tamper tests, including proof that disk entries cannot persist trusted verification state.
- Added regression tests for list byte accounting, oversized list rejection, raw/disk index cache bounds, and object-key reset at capacity.
- Full repository verification is recorded in the workflow final report after packet integration.

## Residual risks

- Index building now avoids duplicate work but still computes full-graph artifacts; affected-shard incremental construction is a later format/layout optimization.
- The production-default 10,000-entity single-update benchmark is about 114-118 ms and 203 MB allocated. Optional per-entity Parquet records are disabled by default; enabling them still causes all records in a changed physical pack to follow its new ETag and remains a separate P2 optimization.
- Metadata cache eviction is deliberately simple and bounded rather than a generalized cache framework.
