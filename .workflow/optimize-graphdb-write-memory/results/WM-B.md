# WM-B result: optional entity-record memory

Status: complete

## Finding

`WriteEntityRecords=true` was a major cumulative-allocation and GC-churn source. Each tiny by-ID record used a fresh Arrow schema/table writer, and `WriteTable(..., 1, ...)` emitted one Parquet row group per logical entity row. On an existing object, the write path also encoded the replacement and fully decoded every field/source row before deciding whether to replace it.

For 100 representative records, the original path allocated 256.84 MB for new objects and 922.08 MB for an unchanged rewrite. A single marshal allocated 2.56 MB and about 14k objects. The worker pool bounded simultaneous encoders, so this was primarily allocation churn rather than an equally large retained heap, but it materially increased GC and CPU on large writes.

## Changes

- Write each entity record as one Parquet record batch and one row group instead of one row group per logical row.
- Use the low-level Parquet column writer with one immutable cached file schema, Arrow schema, and writer-property set. This avoids rebuilding pqarrow schema manifests and temporary chunk/table wrappers for every keyed object.
- Disable per-column dictionaries and statistics for these tiny point-read objects; Snappy compression and the existing 39-column logical schema remain unchanged.
- Omit redundant stored Arrow schema metadata. Plain Parquet logical types reconstruct the same Arrow schema, while the decoder continues to read old files that contain stored schema metadata.
- Delay serialization until the object is known to need a create/compare, and use an exact-byte match to avoid a second full Parquet decode for current-format unchanged objects.
- Project only `page_hash`, `page_etag`, and `version` from the first row when the encoded bytes differ. If the page binding changed and the stored version is not newer, replace the invalid old binding directly instead of decoding every entity row. Same-binding mismatches still take the full decode/content-hash path, and a projected newer version still takes the full decode before returning `ErrConflict`.

Record keys, the entity-record codec/schema, `PageHash`, `PageETag`, `ContentHash`, conditional object ETags, and old-record decoding remain compatible.

## Measurements

All isolated measurements used 100 representative records on Apple M5 Max with `-benchtime=1x`.

| Benchmark | Before | After | Allocation change |
| --- | ---: | ---: | ---: |
| single `marshalParquetEntityRecord` | 2.56 MB, 14,126 allocs | 0.53 MB, 2,632 allocs | -79.1% bytes, -81.4% allocs |
| 100 new records | 256.84 MB, 1,411,967 allocs | 51.88 MB, 263,416 allocs | -79.8% bytes, -81.3% allocs |
| 100 unchanged records | 922.08 MB, 5,238,630 allocs | 51.86 MB, 262,852 allocs | -94.4% bytes, -95.0% allocs |
| 100 changed page bindings, encoder-only checkpoint to final | 218.60 MB, 1,363,291 allocs | 66.80 MB, 368,419 allocs | -69.4% bytes, -73.0% allocs |

The final isolated 10k-new-record stress benchmark completed in 0.82 s with 4.94 GB cumulative allocations and 26.37M allocations. This is still an intentionally severe full-materialization case; only four encoders are live concurrently, and production object stores do not retain every object byte like the benchmark `MemoryStore`.

As an external integration result, the root packet also disables entity-page packing when entity records are enabled and avoids writer-side decoded-page caching. That keeps a one-entity update from invalidating records across a 13-shard physical pack or retaining a duplicate decoded graph. The final 10k single-update benchmark reports about 102-114 ms/op, 217.4 MB/op, and 1.348M allocs/op, versus the workflow's original optional-mode benchmark of roughly 2.2 s/op and 18.7 GB/op.

## Integrity and compatibility coverage

- New files contain exactly one row group and decode through the production reader.
- A synthesized legacy file with one row group per logical row plus stored Arrow schema metadata still decodes to the same logical content hash.
- Tampered entity content with the same page binding is fully decoded, detected, and repaired.
- Changed page bindings are replaced and reloaded with matching `PageHash`, `PageETag`, and `ContentHash`.
- A valid newer record with a different page binding still returns `ErrConflict` rather than being overwritten.

## Verification

- `go test ./internal/storage -run 'Test(ParquetEntityRecord|DecodeParquetEntityRecord|PutEntityRecordIfChanged|EntityRecordsUseParquet|IndexHealthReportsEntityRecordContentMismatch)' -count=1`
- `go test -race ./internal/storage -run 'Test(ParquetEntityRecord|DecodeParquetEntityRecord|PutEntityRecordIfChanged)' -count=1`
- `go test ./internal/storage -run '^$' -bench '^BenchmarkPutEntityRecordBatch$' -benchmem -benchtime=1x -count=1`
- `go test ./internal/storage -run '^$' -bench '^BenchmarkPutEntityRecordBatch10KNew$' -benchmem -benchtime=1x -count=1`

All packet-local commands passed.

## Residual

A first-time 10k entity-record materialization still has high cumulative allocation because compatibility requires one 39-column Parquet object per entity key. Removing that fixed per-object column-writer overhead would require a versioned compact schema or packed-record lookup format plus rolling-upgrade migration. That is a separate format change, not a safe in-place optimization for this packet. Incremental writes are now protected by the un-packed entity-page layout, projected binding check, bounded four-worker concurrency, and substantially cheaper encoder.
