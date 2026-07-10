# P1-B: query and read-path performance

## Result

- Replaced lazy kind scans that accumulated and globally sorted every entity page with an ordered visitor. Unsorted match and stream queries stop after the requested page plus one lookahead result; page 2+ pushes the cursor entity down to the visitor so earlier shards/pages are skipped. Old 256-shard catalogs are grouped into the current 64 canonical scan buckets and merge at most four physical pages, preserving cursor order across rolling upgrades while current catalogs retain the direct one-page fast path. Sorted/aggregate scans charge budget while visiting instead of materializing the full candidate set before budget enforcement.
- Changed `shortest_path` from copied paths/visited sets to BFS state plus predecessor reconstruction. Unconstrained searches expand each entity once. Positional path-step searches retain multiple histories only when their compact predecessor-chain visited sets are incomparable; subset-dominance removes redundant histories while preserving simple-path cycle eligibility.
- Added a request-local decoded edge-shard index (`from -> edges`) to `PersistedIndexLookup`, avoiding repeated Parquet decode, full content hashing, and shard scans during traversal.
- Added read-only callback paths for ReaderCache graphs and decoded entity pages. Public copy-returning APIs remain isolated, while query, entity, schema, export, and snapshot-stream fallbacks reuse immutable cache snapshots without full Graph/page clones.
- Entity page cache LRU operations are O(1), memory is estimated and bounded to at most 512 MiB by default, and configured caches use a 30 second revalidation window. Unconfigured stores retain strict per-hit validation for integrity and tenant-isolation checks.
- Validated decoded entity-page hits trust the immutable `(tenant, version, object, content hash, schema hash)` cache key plus ETag policy, avoiding a second full logical content hash on every visitor/GetEntity hit. Raw health inspection still receives invalid decoded pages so it can report tenant/hash diagnostics precisely.
- Added request-scoped manifest/catalog memoization so lazy attempts and graph fallbacks do not fetch the same control objects twice. Catalog decoding is also reused across requests when the strict manifest/graph version matches; unconstrained allow-stale reads reuse any cached catalog. Explicit `allow_stale=true` with no minimum version skips the eager manifest probe; default fresh reads keep their existing manifest visibility check.
- ReaderCache benchmark with a 2,000-entity graph improved the hot query-facing path from about 1.44 ms, 3.31 MiB, and 14,061 allocations per operation to about 5.4 microseconds with zero allocations in the measured callback.

## Files changed

- `internal/query/entity_pages.go`
- `internal/query/index_lookup_test.go`
- `internal/query/lazy_match_scan.go`
- `internal/query/match.go`
- `internal/query/match_page.go`
- `internal/query/shortest.go`
- `internal/query/stream.go`
- `internal/query/types.go`
- `internal/httpapi/query.go`
- `internal/httpapi/query_read_memo.go`
- `internal/httpapi/query_read_memo_test.go`
- `internal/httpapi/read_freshness.go`
- `internal/httpapi/read_graph_view.go`
- `internal/httpapi/scan.go`
- `internal/httpapi/server.go`
- `internal/storage/cache.go`
- `internal/storage/cache_read_view_test.go`
- `internal/storage/entity_page_cache.go`
- `internal/storage/entity_page_cache_perf_test.go`
- `internal/storage/entity_page_copy.go`
- `internal/storage/entity_page_size.go`
- `internal/storage/index_lookup.go`
- `internal/storage/index_lookup_edge_cache_test.go`
- `internal/storage/index_lookup_edges.go`
- `internal/storage/index_lookup_scan.go`
- `internal/storage/index_lookup_scan_perf_test.go`
- `internal/storage/parquet_edge.go`
- `internal/storage/parquet_entity.go`

## Verification

- `go test ./internal/query -count=1`
- `go test ./internal/storage -count=1`
- `go test ./internal/httpapi -count=1`
- Focused query and storage race tests for cursor resume, stepped shortest-path histories, entity-page visitation, and legacy shard pagination.
- Focused tests cover lazy scan early stop and cursor resume, old/new shard-order compatibility, shortest-path diamond deduplication and cycle-history correctness, decoded edge-shard reuse, read-only Graph reuse, entity-page early stop/isolation/cursor resume, byte bounds, revalidation policy, and request-scoped manifest/catalog reuse.
- `go test ./internal/storage -run '^$' -bench '^BenchmarkReaderCacheHotRead$' -benchmem -benchtime=5x -count=1`
- `git diff --check`

## Residual risks

- A first cache miss still decodes the complete physical Parquet object containing a logical entity page or edge shard. The visitor stops before later logical pages and avoids repeat decodes, but row-group predicate pushdown would require a storage-layout change.
- Stepped shortest-path constraints can require an antichain of incomparable visited histories for correctness. States share predecessor chains and depth is capped at 16, but adversarial constrained graphs can still create more states than unconstrained global BFS.
- The zero-copy ReaderCache path is intentionally callback-scoped because `graph.Graph` still exposes mutable maps. Public `Load` methods continue to clone; new callers must use the callback only for synchronous reads.
- Default fresh HTTP reads still probe the current manifest once per request to preserve the existing visibility contract. Cross-request manifest/catalog TTL caching was not introduced.
- Decoded edge-shard indexing is request-local, which bounds lifetime and preserves version isolation but does not share decoded graph data across independent requests.
