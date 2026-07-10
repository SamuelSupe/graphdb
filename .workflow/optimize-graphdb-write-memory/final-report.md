# Final Report: Optimize GraphDB write memory

## Outcome

The write path had two material memory cases and both are optimized:

- Production-default writes repeatedly normalized and copied full index artifacts and logical hash state.
- Optional entity-record writes created extreme Parquet allocation churn and invalidated thousands of sibling records through physical page packing.

Final profiling shows lower cumulative allocation, lower peak live heap, and negligible post-write retained growth in both modes.

## Accepted Results

- Stream the exact legacy-compatible graph MD5 encoding one item at a time and reuse the measured logical byte count for writer-cache weighting.
- Build graph-derived index pages/shards with final capacities, avoid intermediate full slices/JSON normalize copies, and cache immutable logical hashes/object groups only within the build artifact.
- Encode each tiny entity record as one row group with shared immutable schema/properties, low-level column writers, no ineffective dictionary/statistics metadata, lazy serialization, and projected binding/version reads.
- Keep entity pages un-packed when entity records are enabled so one update invalidates only one logical page's records.
- Do not populate decoded entity-page read cache from writer publication; readers fill it on demand.
- Bound retained writer graphs by both 64 tenants and a configurable 512 MiB conservative memory weight (`GRAPHDB_WRITE_CACHE_MAX_BYTES`). Charge decoded entity pages at a heap-calibrated 5x structural estimate.

## Rejected Results

- Rejected updating only the directly mutated entity record while keeping packed pages: sibling records embed the changed physical PageETag, so skipping them would weaken integrity/correctness.
- Rejected a large persistent buffer pool: it would turn allocation churn into retained memory and complicate bounds.
- Rejected changing content hashes or the entity-record codec/schema; rolling compatibility is preserved.

## Conflicts Resolved

- Un-packed optional pages initially populated the read cache and retained about 16 MiB of duplicate decoded graph state. Writer-side page-cache population was removed; on-demand reads retain existing behavior.
- Canonical hash fast paths apply only after an allocation-free recursive JSON-normalization check. Manual/non-canonical values retain the legacy normalize-and-hash path.
- Old multi-row-group entity-record Parquet files remain readable, while newly written files use the lower-memory single-row-group layout.

## Verification Evidence

- `go test ./... -count=1` passed. Full storage race passed in 301 seconds; graph, config, and command race tests also passed.
- `go vet ./...` and `git diff --check` passed.
- Default 10k single-update cumulative allocation: about 203-210 MB to 155-163 MB; peak live heap 42.2 MB to 36.5 MB.
- Optional entity-record 10k single-update cumulative allocation: 18.80 GB to 217-227 MB; peak live heap 79.3 MB to 32.6 MB; FileStore time 16.51 s to 0.98 s. The final MemoryStore benchmark is about 102-114 ms/op and 217.4 MB/op.
- Index artifact build: 48.11 MB/op to 11.16 MB/op and 372k to 131k allocations.
- Graph logical hash: 18.56 MB/op snapshot reference to 10.41 MB/op streaming; peak process footprint about 45.9 MB to 39.5 MB in the isolated benchmark.
- Optional entity-record compatibility covers legacy files, tampered same-binding content, changed bindings, and newer-version conflicts.

## Remaining Risks

- Production-default commits still allocate about 155 MB cumulatively in the 10k indexed fixture, primarily Arrow/Parquet serialization and final immutable artifacts. Simultaneous live heap is much smaller (about 36.5 MB delta).
- First-time creation of 10,000 optional by-ID Parquet objects still has about 4.9 GB cumulative allocation because the compatibility layout requires one 39-column file per key. Hot single updates no longer fan out across packs.
- The writer graph cache byte value is a conservative logical-size weight rather than an exact Go heap measurement. Large values are rejected and LRU eviction enforces the configured total.
- Entity-record mode trades several packed page objects for up to 64 logical page objects to keep update fanout bounded.

## Reusable Follow-up

The next format-level improvement would replace per-entity 39-column Parquet objects with a versioned compact/packed point-lookup format and an explicit rolling migration. That is no longer required to keep normal or hot optional writes within bounded peak memory.
