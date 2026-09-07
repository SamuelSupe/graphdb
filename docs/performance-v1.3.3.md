# GGraphDB 1.3.3 performance report

This report keeps two comparisons separate. **Round 1** is the historical
v1.3.2-to-round1 service and micro comparison. **Round 2** starts from that
round1 state and measures the candidate after three additional storage and
streaming optimizations. Round2 micro and isolated-service results are included
below. The source-final-v3 local static
gate exited 0, but that does not assert that GitHub release gates passed. This
is an evidence summary, not a production SLO or capacity certification.

## Scope and comparison

- **Round 1 baseline:** v1.3.2 / commit `247a432394ab363c28af5017f1dd1cfd9ec22cd9`.
- **Round 1 candidate:** `codex/product-performance`, candidate2 retry3 service run.
- **Round 2 baseline:** the validated round1 candidate state, with a fresh
  round2 micro envelope and corrected reverse batch/lookup workloads.
- **Round 2 candidate:** production candidate commit `9cf079ee`, after Entity
  JSONValue encoding, reverse physical-pack packing, and edge streaming decode.
- **Service shape:** `GRAPHDB_MODE=all`, `GRAPHDB_COORDINATION=local`, direct
  writes, four writers, sixteen readers, batch size 200, 100 ms read/write
  interval, 60 s nominal duration and roughly 58 s active soak.
- **Raw artifact names:** Round 1 uses `service-comparison-2.json`,
  `micro-comparison.json`, `paired-summary.txt`, and
  `cold-baseline-query-probe-comparison.txt`. Round 2 uses the valid near-time
  baseline `baseline-micro-recheck-v2/micro.log`,
  `baseline-micro/entity-stream-path/micro.log`,
  `candidate2/micro-reverse/micro.log`, `candidate/micro/micro.log`, and the
  isolated service `operation-metrics.txt`, `ingest-success-summary.json`,
  `service-comparison-round1.tsv`, `resources.post-soak.txt`, `cpu-top-cum.txt`,
  `alloc-space-top.txt`, `heap-top.txt`, `pprof-summary.txt`, and
  `pprof-comparison.txt`. `baseline-micro/baseline-summary.txt`
  is retained only as the earlier reference. Values below are transcribed from
  review artifacts; the artifacts are not repository paths.

## Environment

The first-round and isolated service runs used the same controlled envelope:
OrbStack, Go 1.25.14,
linux/arm64, GOMAXPROCS=8, an 8-CPU and 8-GiB limit, and the same tmpfs setup.
The service snapshot ran without code changes during the run. CPU profiles cover
about 45.2 seconds after setup; their cumulative stacks overlap.

The principal implementation points are reviewable in
[graph/read_ids.go](../internal/graph/read_ids.go#L84),
[graph/read_range.go](../internal/graph/read_range.go#L23),
[storage/index_incremental_pages.go](../internal/storage/index_incremental_pages.go#L12),
[storage/index_incremental_edges.go](../internal/storage/index_incremental_edges.go#L32),
[httpapi/scan_graph.go](../internal/httpapi/scan_graph.go#L15), and
[storage/parquet_entity_scan.go](../internal/storage/parquet_entity_scan.go#L193).

## Round 1 historical service results (v1.3.2 → round1)

These numbers describe the first optimization round and are retained as the
baseline for round2. They are not the final 1.3.3 service result.

All latency values are milliseconds. Write percentiles use successful HTTP 200
requests only; cancellations are reported separately.

| Operation | v1.3.2 baseline | Round1 candidate | Reading |
|---|---:|---:|---|
| Successful ingest | 12 HTTP 200 + 4 HTTP 499; p50/p95/p99 17197.828/21930.528/21930.528 | 21 HTTP 200 + 4 HTTP 499; p50/p95/p99 8719.891/16805.706/18430.341 | More successful writes; not a fixed-data A/B. |
| Snapshot export | 46 HTTP 200; p50/p95/p99 104/5302/6045 | 133 HTTP 200; 291/621/679 | Tail improved; p50 increased. |
| Strict indexed match | p95/p99 70/287 | 33/123 | Improved tail. |
| Range aggregate match | p95/p99 67/255 | 33/105 | Improved tail. |
| Scan entities | p95/p99 377/1227 | 34/56 | Improved strict path. |
| Scan edges | p95/p99 340/1083 | 33/66 | Improved strict path. |
| Allow-stale indexed match | p95 11 | p95 15 | Regression retained in the conclusion. |
| Allow-stale entity scan | p95 380 | p95 401 | Regression retained in the conclusion. |
| Large indexed stream | p50 18; min-version 19 | p50 33; min-version 34 | Streaming p50 increased. |

The candidate ended with 13,267 entities and 22,191 edges versus 7,867 baseline
entities. Because successful writes and graph size differ inside the same time
envelope, these service results are dynamic-growth comparisons rather than
fixed-data A/B results.

## Round 2 isolated service results (round1 → final candidate)

The isolated round2 service run used the same envelope without test overlap
(09:48:56–09:49:56 UTC). It completed **28 HTTP 200 writes and 4 test-ending
HTTP 499 cancellations**; the
successful-write p50/p95/p99 was **7119.210/13043.534/13584.758 ms**, versus
**8719.891/16805.706/18430.341 ms** for the round1 candidate. Snapshot export
completed **193 HTTP 200** requests with p50/p95/p99 **203/340/364 ms**, versus
**133** and **291/621/679 ms** in the round1 candidate. Post-soak RSS was
**1,266,884 kB** versus **1,098,816 kB**; the final graph was larger, so this is
not a blanket memory reduction claim. Rebuild reached `succeeded` but reported
`cleanup skipped` because active reader watermark 29 was behind manifest version
30. After restart, index health was `ready` with **17 orphan index-object
warnings**; integrity was `status=ok` with **1,747 checks and 0 issues**, and
reader freshness was manifest/visible version **30/30** with lag **0 ms**.

Selected read operations below show p50/p95/p99 in milliseconds:

| Operation | Round1 | Round2 isolated |
|---|---:|---:|
| Strict indexed match | 8/33/123 | 5/16/40 |
| Range aggregate | 10/33/105 | 7/15/33 |
| Entity scan | 15/34/56 | 10/25/33 |
| Edge scan | 9/33/66 | 6/17/40 |
| Large indexed stream | 33/60/167 | 23/43/221 |
| Large indexed stream, min-version | 34/58/131 | 23/43/224 |

The large-stream p99 regressions remain part of this result. The final service ended at
**17,467 entities and 29,191 edges**, versus **13,267/22,191** in round1, so
these are dynamic-growth comparisons rather than fixed-data A/B results.

## Round 2 isolated resources and profile

| Signal | Round1 candidate | Round2 isolated |
|---|---:|---:|
| CPU profile window | 45.20 s | 45.12 s |
| Total CPU samples | 160.39 s | 144.82 s |
| Heap in use | 538,591,389 B | 618,379,015 B |
| Cumulative `alloc_space` | 116,792,887,844 B | 152,551,286,669 B |

In the isolated cumulative CPU profile, `commitOnceLocked` was 39.64 s
(27.37%), index refresh 37.37 s (25.80%), and GC background marking 38.03 s
(26.26%); these stacks overlap and must not be added. The allocation profile's
largest flat owner is the Arrow allocator (40.65 GiB in the pprof view), while
the heap profile's largest flat owner is `WriterObjectCache.cachePositive`
(213.77 MiB). `alloc_space` is cumulative allocation, not RSS; heap in-use and
post-soak RSS are separate measurements.

## Round 1 historical resource and profile evidence

| Signal | v1.3.2 baseline | Round1 candidate |
|---|---:|---:|
| Post-soak RSS | 2,022,824 kB | 1,098,816 kB |
| Heap in use | 449.52 MiB | 513.64 MiB |
| Cumulative alloc | 346,105,358,163 B | 116,792,887,844 B |
| CPU sample / profile window | 159.41 s / 45.19 s | 160.39 s / 45.20 s |
| GC / Parquet decode / commit | 36.91% / 22.90% / 14.05% | 23.63% / 7.24% / 24.06% |
| Index refresh / stream | 13.44% / 14.80% | 22.58% / 16.04% |

The profile percentages are cumulative and overlap. Cumulative alloc is not RSS;
the three memory signals must not be substituted for one another. The candidate
profile is summarized in `cpu-top-extended.txt`, `alloc-top-cum.txt`, and
`heap-top.txt`.

## Round 1 historical maintenance, freshness, and correctness

Retry3 produced no 429/5xx responses. Compact returned HTTP 200, and async index
rebuild reached natural `succeeded` termination. Before maintenance, manifest was
23 while the visible/catalog version was 22 and reader lag was 4,922 ms. The
post-compact sample was still `index_stale`; after rebuild and restart, manifest,
snapshot, and catalog were version 23 with freshness lag 0.

The after/restart integrity responses reported `status=ok`, 1,747 checks, and
zero integrity issues. Index health was `ready`, but retained 14 orphan warnings.
The rebuild task reported `cleanup skipped` because active reader watermark 22
was behind manifest 23; this is not a zero-warning maintenance result.

The cold/warm probe used the same retry3 seed and byte-identical original query
(`SHA256 b160a56e9885340bf9c9ac3b2dff168f7773f81e04b76e76a31e3a4ad3540a27`).
Each version was probed once: v1.3.2 cold/warm **3.113153/0.105618 s**, candidate
cold/warm **1.512838/0.018382 s**. Four response bodies and cursors were byte
identical. This is a one-shot fixed-seed probe, not p95 or multi-round steady
state. An earlier limit-10/profile-mismatched probe is excluded.

The local task ownership fix is implemented at
[storage/index_task.go](../internal/storage/index_task.go#L306) with the recovery
regression in [storage/task_recovery_test.go](../internal/storage/task_recovery_test.go#L178).
Strict committed graph reuse and public clone isolation are in
[storage/cache.go](../internal/storage/cache.go#L427) and
[storage/cache.go](../internal/storage/cache.go#L718).

## Round 2 baseline and final-candidate split (round1 → final)

The near-time round2 baseline recheck uses the same OrbStack linux/arm64, Go 1.25.14,
8-CPU/8-GiB, GOMAXPROCS=8 envelope, serial workloads, and setup outside the
timer. The corrected reverse workloads are the valid baseline; an earlier
fixed-payload counter attempt became a no-op after its first iteration and is
excluded. The three-sample medians for the final 128-edge physical-pack choice
are reported below. Focused validation, the new multi-pack regression, and the
source-final-v3 local static gate exited 0.

Round2's frozen production changes expose an entity `JSONValue` alias so the
outer encoder performs one JSON encoding, merge reverse `To` data into physical
packs while preserving larger logical shards, and decode edge Parquet shards
through `RecordReader` with row-group pruning. The code paths are visible in
[httpapi/scan_graph.go](../internal/httpapi/scan_graph.go#L59),
[storage/reverse_index_incremental.go](../internal/storage/reverse_index_incremental.go#L147),
and [storage/parquet_edge.go](../internal/storage/parquet_edge.go#L275).

## Round 1 microbenchmarks (v1.3.2 → round1)

These are process-local medians and do not establish HTTP, object-storage, or
mixed read/write capacity.

| Benchmark | Baseline | Candidate | Note |
|---|---:|---:|---|
| 10K indexed entity commit | 19.800526 ms; 30,861,005 B/op; 199,869 alloc | 16.930604 ms; 24,414,410 B/op; 166,178 alloc | Reduced time and allocation. |
| Single indexed entity commit | 17.610089 ms | 12.367888 ms | Reduced median. |
| Fixed 50K range page | 1.369774 ms | 8.840 µs | Bytes +3.63%; alloc 121→123. |
| Exact range aggregate | 9.261253 ms | 6.718179 ms | Range result, not row-group/MD5 evidence. |
| Paired traverse | — | +2.37% | Small regression. |
| Paired storage copy | — | -5.14% | Earlier +10.4% suspicion did not reproduce. |

The direct and `wal_tenant_flush` ingest microbenchmarks each package eight
requests per operation; `wal_tenant_flush` is a storage path without HTTP or WAL
fsync and is not an end-to-end WAL result.

## Round 2 measured microbenchmarks (round1 → final candidate)

All rows below are medians of three samples from the corrected workloads. The
final production choice caps each merged physical reverse `To` pack at 128
edges; a larger logical shard remains independent. The earlier 2048-edge
candidate was rejected after its 16-target cold lookup grew from about 18 ms to
about 76 ms.

`write bytes/op` is object-storage output from the benchmark. `Go B/op` is Go
allocation. `reverse bytes/op` and `reverse-objects/op` cover the reverse path;
the object count includes one reverse catalog and is not a Parquet-file count.
All byte values are B, not KiB or MiB.

| Reverse batch | Baseline → final ns/op | Write bytes/op | Go B/op | Reverse bytes/op | Reverse objects/op |
|---:|---:|---:|---:|---:|---:|
| 16 edges | 29,782,426 → 26,106,274 | 394,307 → 295,084 | 54,051,797 → 44,845,173 | 252,844 → 153,989 | 17 → 7 |
| 64 edges | 72,487,201 → 62,228,319 | 800,753 → 542,584 | 126,627,916 → 102,539,322 | 575,492 → 317,201 | 42 → 16 |
| 256 edges | 111,335,242 → 100,765,556 | 1,167,976 → 769,984 | 193,118,023 → 155,918,300 | 863,814 → 465,832 | 64 → 24 |
| 1024 edges | 121,995,237 → 114,507,482 | 1,207,514 → 799,075 | 214,221,710 → 176,032,204 | 878,513 → 470,181 | 65 → 24 |

Against this near-time baseline, write latency falls roughly 6–14% across the
four batch sizes. Each column uses the same near-time recheck as its baseline.

Lookup medians from the near-time recheck and final candidate are:

| Reverse lookup | Baseline → final ns/op | Read bytes/op | Go B/op | Data objects read |
|---|---:|---:|---:|---:|
| 16-target cold | 15,624,397 → 17,935,065 | 205,537 → 288,563 | 42,389,896 → 55,337,096 | 16 → 16 |
| 16-target warm | 11,671 → 11,612 | 0 → 0 | 30,800 → 30,800 | 0 → 0 |
| Single-target cold | 838,964 → 970,186 | 12,897 → 18,469 | 2,673,576 → 3,554,568 | 1 → 1 |

The near-time 16-target cold comparison is a **14.8% latency increase**, with
more read bytes and Go allocation. The earlier summary's **18,302,552 ns/op**
is retained only as a prior reference, not as a stable-improvement baseline.
Catalog and reverse catalog objects were loaded before lookup timing and object
counting, so the 16 and 1 counts above are data-object GETs. The 128-pack choice
is therefore a bounded write/read trade-off; it does not claim a cold lookup
improvement or a service-tail improvement.

The round2 entity stream path measured **6,480 ns/op, 6,585 B/op, and 88
allocs/op**, versus **13,136 ns/op, 8,676 B/op, and 90 allocs/op** in the
`entity-stream-path` baseline. This is evidence for the round2 JSONValue change;
the path was retained without another change in the later candidate rerun.

The single-target row above is a process-local microbenchmark, not a service
result; microbenchmarks do not establish service capacity.

## Compatibility and follow-up work

No object-storage layout or WAL record format migration is introduced. JSON
response shapes and cursor semantics remain unchanged. Focused validation and
the new multi-pack regression passed for the frozen reverse-index packing. The
source-final-v3 local static gate also exited 0.

The round2 candidate also contains JSONValue-based single-pass entity encoding
and edge streaming decode. Their service performance and memory numbers must
remain separate from the round1 historical results; the stream micro row above
is the round2 JSONValue evidence, while the later candidate rerun retained that
path.

## Remaining boundaries

- The top-level entity/edge map remains O(N); COW still has top-level map-copy
  cost and this work does not turn the graph into a sharded structure.
- Logical MD5 still streams the complete JSON bytes; immutable cache sharing
  reduces copying but does not remove that full-stream cost.
- The default roughly 512-MiB tenant cache limit still has a cliff, and shared
  multi-tenant eviction can trigger reloads.
- HTTP all-mode scan still walks the in-memory map and bounded heap. Warm
  materialized pages use cached sorted IDs, binary search, and visit limit+1;
  the first range query builds ordered keys and keyset changes rebuild them.
- Remote S3, PostgreSQL multi-writer, cross-host behavior, very large tenant
  counts, long steady state, and million-scale end-to-end capacity remain
  outside this evidence.

The measurements above document the current candidate and its limits. They do
not by themselves state that the GitHub release workflow or any separate release
gate has passed.
