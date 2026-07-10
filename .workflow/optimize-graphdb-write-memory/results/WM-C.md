# WM-C result: default index-build memory

Status: complete

## Finding

The production-default 10k-entity single-update path did allocate heavily in index artifact construction. Before this packet, `buildIndexArtifactsWithDefinitions` allocated about 48.11 MB and 372k objects per build. The dominant source was entity-page content hashing: every page was JSON marshaled, unmarshaled into a second full object graph, and marshaled again. Secondary index object grouping was also rebuilt and copied for catalog construction and again for object writing.

The full `BenchmarkSingleEntityIndexedCommit` baseline was about 203.16 MB/op and 1.371M allocs/op, so this was material rather than benchmark noise.

## Changes

- Mark graph-derived index values, entity pages, and edge shards as JSON-normalized only after an allocation-free recursive type check.
- Preserve the old normalize-and-hash path for non-canonical/manual objects.
- Compute and retain logical content hashes once on immutable build artifacts; these fields are unexported and excluded from serialized objects.
- Prepare secondary-index object groups once and reuse them for catalog generation and the subsequent write phase.
- Reuse already sorted posting slices in canonical build artifacts and avoid a redundant merge for one-shard groups.
- Pre-count entity pages and edge shards, allocate their final capacities, and build directly from graph maps without an intermediate full slice.
- Added compatibility tests that compare the optimized hashes against the legacy normalization path for nested entity/edge values, plus a regression for non-canonical integer input.

Catalog ordering, serialized object fields, and logical content hashes remain unchanged.

## Measurements

All measurements used the same Apple M5 Max worktree and production default `WriteEntityRecords=false`.

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `BenchmarkBuildIndexArtifacts10K` time | 50.83 ms/op | 21.19 ms/op | -58.3% |
| artifact allocations | 48.11 MB/op | 11.16 MB/op | -76.8% |
| artifact allocation count | 372,321/op | 130,606/op | -64.9% |
| `BenchmarkSingleEntityIndexedCommit` time | 111.73 ms/op | 92.85 ms/op | -16.9% |
| full commit allocations | 203.16 MB/op | 155.07 MB/op | -23.7% |
| full commit allocation count | 1,370,784/op | 1,103,606/op | -19.5% |

The 10-iteration allocation profile attributed 531.96 MB cumulatively to index artifact construction before the change and 110.24 MB after it. The old 322.62 MB `normalizeGraphEntities` allocation site disappeared from the optimized build. Because hashes are generated per bounded page and no full normalized entity clone coexists with the artifacts, transient live memory during this phase is now bounded by one encoded page plus the final artifact slices rather than several full logical copies.

## Verification

- `go test ./internal/storage -run 'Test(PreparedIndexArtifactsPreserveLogicalHashes|NonCanonicalEntityPageKeepsNormalizedHash)$' -count=1`
- `go test ./internal/storage -count=1`
- `go test -race ./internal/storage -run 'Test(PreparedIndexArtifactsPreserveLogicalHashes|NonCanonicalEntityPageKeepsNormalizedHash|PersistedLookup|Index)' -count=1`
- `go test ./internal/storage -run '^$' -bench '^(BenchmarkBuildIndexArtifacts10K|BenchmarkSingleEntityIndexedCommit)$' -benchmem -benchtime=5x -count=3`

All passed.

## Residual

The complete commit still allocates roughly 155 MB/op at this checkpoint. The remaining large sites are primarily Arrow/Parquet serialization, graph mutation/copy state, and the graph logical hash; those are outside WM-C's index-builder ownership and are being addressed by the other workflow packets. Within index artifact construction, the remaining roughly 11 MB/op is mostly the final 10k entity-page slices and bounded per-page JSON bytes, not simultaneous full-graph normalization copies.
