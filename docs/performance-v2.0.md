# GGraphDB 2.0 performance and validation

## Conclusion / 结论

2.0 removes the full-graph copy/hash cost from ordinary small updates and shares
unchanged graph and field-index shards across versions. The isolated copy-and-hash
benchmark shows a substantial reduction in time and allocations. **An overall
throughput or tail-latency improvement is not established by this run.** The
shared OrbStack machine had material A/A variance; the tables retain regressions
rather than turning a narrow microbenchmark into a system-wide claim.

小批更新的整图复制和摘要成本已消除；字段索引的值目录与 ID 集合同样按分片复制。
本次整体性能验收标记为未定：共享环境存在明显波动，不能宣称所有读写路径都提升了 10%。
单机、单进程边界保持不变；本轮不提供 1.x 数据或摘要迁移。

Final observations against the previous local implementation:

- 100K WAL with maintenance: mixed QPS **299 → 441**, published batches **49 → 71**,
  ingest P95 **22.03 → 16.61 s**, match P95 **232.8 → 92.3 ms** and match P99
  **480.9 → 116.8 ms**. Peak RSS was **2.94 → 2.86 GB**.
- 10K direct: ingest P95 **283.0 → 220.7 ms**, mixed QPS **2,256 → 2,206**,
  peak RSS **615 → 582 MB**. Match P95 was **16.3 → 17.1 ms**.
- 10K WAL with maintenance still regressed: mixed QPS **2,748 → 2,055**, match
  P95 **13.0 → 19.0 ms**, despite ingest P95 improving **570.4 → 295.4 ms**.
  Several export/compact/cold-read observations also missed the original targets.
  These regressions are retained below; host variation does not establish their cause.

## Implemented scope

- Entity, edge, adjacency and field-index copy-on-write shards; sets of at most
  32 entries stay compact. Request-local lookup results remain ordinary maps.
- Incremental `sha256-shards-v2` digest, with a fixed 32 KiB root and no retained
  full-graph canonical JSON. Cold construction still visits the entire graph.
- Reused entity partitions for incremental index pages; bounded backlog batches.
- Shared estimated memory admission for queued/active index and snapshot work;
  queued work cannot reserve memory behind an unreserved synchronous predecessor.
- Avoid redundant automatic full rebuilds while ordered incremental updates are active.
- GC uses 50 ms retry windows and yields to queued reads even with overlapping
  maintenance waiters. Canonical edge shards avoid a redundant JSON round trip.
- Cooperative GC scan/byte/time budgets and file publication budgets. These are
  not hard RSS limits, atomic multi-file transactions, or strict per-file deadlines.
- Original durability barriers, tenant generations, pinned read views and backup
  verification remain in place. Rare schema/identity/alias changes and whole
  snapshots/rebuilds still have larger work than a common field update.

## Method

- Baseline: `5632cd82`, the previous local implementation with the maintenance fixes.
  Candidate runtime: `d90e554c`; later report-only commits do not change that runtime.
- OrbStack Linux arm64, Go 1.26.7, Linux named volume, 4 CPU / 4 GiB container.
  CI, container and release builds use Go 1.26.7 too. Architecture and build flags
  still differ; local arm64 timings are not measurements of the amd64 packages.
- Deterministic seeds: 10,002 / 100,002 entities and 5,001 / 50,001 edges.
  Fresh data directory per format. Four writers, 16 readers, 20 entities per update.
- WAL: synchronous durability, 60 s warmup and 120 s measurement, one final run
  per size following targeted regression investigations. Compact/GC/index-rebuild
  tasks start at 10/30/50/70/90/110 s.
- Direct: 10K only, 20 s warmup / 60 s measurement, one before/after run. Direct
  maintenance timings are separate from mixed load. 100K direct was not measured.
- Each writer targets a 2 s interval but waits for its previous request. This is
  paced, bounded-client load, not an open-loop fixed-arrival capacity test. Actual
  elapsed time includes final in-flight completions; published counts are retained.
- WAL uses GOMEMLIMIT=3GiB and a 512 MiB maintenance estimate budget; read/write
  caches each have a 512 MiB limit. The direct helper uses default maintenance/Go
  memory settings and the same read/write cache limits for both binaries.
- Cold reads mean a restarted process. OS page cache is uncontrolled and was not
  globally evicted; five WAL and three direct restart samples are insufficient
  for a reliable tail distribution.
- Other user workloads remained running. This focused comparison replaces the
  originally proposed three-way, three-repeat matrix; it is not that full gate.

Raw JSON, profiles, binary hashes and reproduction scripts are in the separately
checksummed `graphdb-v2.0.0-performance.tar.gz` release asset. `compare-candidate6.py`
uses the repository's benchmark helpers; `measure-candidate6.sh` records the execution
sequence and `REPRODUCE.md` records the build procedure. The archived paths identify
the isolated Linux test volume.

## Copy + insert + content hash microbenchmark

One new entity is inserted into a shared immutable graph, then its digest is read.
The 1.x side computes its canonical-JSON MD5; 2.0 uses the new public hash contract.
These are different algorithms by design, not equivalent MD5 implementations.
The microbenchmark was recorded at `a3ff0a7f`; that graph implementation is unchanged
in the final runtime.

| Entities | Previous time (two runs) | 2.0 time | Previous allocated bytes/op | 2.0 allocated bytes/op |
|---:|---:|---:|---:|---:|
| 10,000 | 3.472 / 3.479 ms | 37.617 µs | 4,410,659 / 4,411,285 | 56,064 |
| 100,000 | 39.096 / 37.872 ms | 191.959 µs | 41,625,392 / 41,623,810 | 269,933 |

## Mixed workload

QPS counts completed queries plus published writes, not merely accepted WAL items.
All listed request error counts are zero; WAL-readable latency is measured from
acceptance until the batch becomes readable. Values are milliseconds.

| Case | Actual seconds | Mixed QPS | Published batches | Peak RSS MiB | Server CPU seconds | Mean host load |
|---|---:|---:|---:|---:|---:|---:|
| baseline/100002-main-local | 128.2 | 299.0 | 49 | 2802.7 | 283.3 | 15.3 |
| baseline/10002-main-local | 120.1 | 2747.9 | 240 | 478.9 | 207.0 | 12.3 |
| release/100002-new-local | 122.1 | 441.0 | 71 | 2728.9 | 322.4 | 14.2 |
| release/10002-new-local | 120.1 | 2054.7 | 240 | 494.8 | 244.2 | 13.9 |
| direct/main-local-direct | 60.0 | 2255.8 | 120 | 586.6 | 113.1 | 16.6 |
| direct/new-local-direct | 60.1 | 2206.1 | 120 | 555.4 | 120.6 | 15.5 |

| Case | Operation | p50 ms | p95 ms | p99 ms | Count |
|---|---|---:|---:|---:|---:|
| baseline/100002-main-local | ingest | 7559.32 | 22032.19 | 22078.09 | 49 |
| baseline/100002-main-local | query-match | 27.28 | 232.84 | 480.89 | 9566 |
| baseline/100002-main-local | query-min-version | 23.25 | 233.31 | 501.96 | 9568 |
| baseline/100002-main-local | query-traverse | 16.82 | 220.30 | 481.47 | 9566 |
| baseline/100002-main-local | scan | 21.85 | 207.69 | 464.57 | 9571 |
| baseline/100002-main-local | wal-accept | 14.16 | 41.14 | 51.37 | 49 |
| baseline/100002-main-local | wal-readable | 7546.19 | 21990.94 | 22036.95 | 49 |
| baseline/10002-main-local | ingest | 182.00 | 570.45 | 2205.93 | 240 |
| baseline/10002-main-local | query-match | 4.23 | 12.98 | 50.23 | 82384 |
| baseline/10002-main-local | query-min-version | 3.74 | 12.36 | 23.82 | 82385 |
| baseline/10002-main-local | query-traverse | 2.21 | 9.60 | 37.45 | 82382 |
| baseline/10002-main-local | scan | 6.61 | 17.73 | 40.07 | 82386 |
| baseline/10002-main-local | wal-accept | 11.19 | 23.32 | 27.45 | 240 |
| baseline/10002-main-local | wal-readable | 169.47 | 556.42 | 2197.41 | 240 |
| release/100002-new-local | ingest | 5354.64 | 16606.69 | 20712.28 | 71 |
| release/100002-new-local | query-match | 29.91 | 92.32 | 116.77 | 13438 |
| release/100002-new-local | query-min-version | 23.61 | 91.16 | 116.35 | 13441 |
| release/100002-new-local | query-traverse | 21.50 | 81.80 | 104.45 | 13440 |
| release/100002-new-local | scan | 27.49 | 90.60 | 117.09 | 13439 |
| release/100002-new-local | wal-accept | 14.64 | 42.96 | 87.67 | 71 |
| release/100002-new-local | wal-readable | 5337.10 | 16590.32 | 20682.75 | 71 |
| release/10002-new-local | ingest | 208.33 | 295.43 | 2267.70 | 240 |
| release/10002-new-local | query-match | 5.56 | 19.03 | 61.99 | 61580 |
| release/10002-new-local | query-min-version | 5.01 | 17.10 | 57.62 | 61584 |
| release/10002-new-local | query-traverse | 2.98 | 13.69 | 59.74 | 61581 |
| release/10002-new-local | scan | 8.65 | 24.92 | 64.02 | 61583 |
| release/10002-new-local | wal-accept | 13.35 | 27.26 | 40.71 | 240 |
| release/10002-new-local | wal-readable | 195.37 | 269.76 | 2244.98 | 240 |
| direct/main-local-direct | ingest | 117.48 | 283.02 | 751.68 | 120 |
| direct/main-local-direct | query-match | 5.49 | 16.32 | 25.54 | 33811 |
| direct/main-local-direct | query-min-version | 4.93 | 16.06 | 25.11 | 33810 |
| direct/main-local-direct | query-traverse | 2.87 | 12.15 | 19.98 | 33809 |
| direct/main-local-direct | scan | 8.92 | 23.45 | 34.27 | 33813 |
| direct/new-local-direct | ingest | 86.32 | 220.74 | 732.34 | 120 |
| direct/new-local-direct | query-match | 5.87 | 17.07 | 26.26 | 33063 |
| direct/new-local-direct | query-min-version | 5.29 | 15.97 | 23.73 | 33065 |
| direct/new-local-direct | query-traverse | 3.10 | 12.35 | 19.47 | 33062 |
| direct/new-local-direct | scan | 9.29 | 23.57 | 33.75 | 33062 |

## Maintenance and process-cold reads

Each export/compact/backup/restore timing is one observation, not a percentile.
All restores were compared against the exact pre-backup exported graph.

| Case | Export ms | Compact ms | Backup ms | Restore ms | Cold read sample p95 ms |
|---|---:|---:|---:|---:|---:|
| baseline/100002-main-local | 1022.4 | 6582.5 | 4941.5 | 47406.1 | 115.9 |
| baseline/10002-main-local | 82.5 | 730.8 | 514.4 | 4351.3 | 39.5 |
| release/100002-new-local | 1106.9 | 8250.5 | 4949.4 | 48339.8 | 128.5 |
| release/10002-new-local | 125.4 | 911.8 | 625.8 | 5171.7 | 49.9 |
| direct/main-local-direct | 80.1 | 619.9 | 517.4 | 4628.4 | 47.6 |
| direct/new-local-direct | 116.6 | 803.3 | 518.0 | 5293.8 | 41.2 |

| Case | Disk read MiB | Disk write MiB | Sync calls | Sync failures | Peak file descriptors |
|---|---:|---:|---:|---:|---:|
| baseline/100002-main-local | 1306.6 | 631.4 | 4999 | 0 | 36 |
| baseline/10002-main-local | 103.8 | 858.1 | 10373 | 0 | 33 |
| release/100002-new-local | 527.2 | 821.4 | 6557 | 0 | 34 |
| release/10002-new-local | 141.1 | 823.8 | 10304 | 0 | 39 |
| direct/main-local-direct | 50.4 | 513.0 | 6744 | 0 | 32 |
| direct/new-local-direct | 65.1 | 511.1 | 6735 | 0 | 29 |

## Calibration, limitations and regressions

The 10K baseline/new/baseline calibration (before the final field-index adjustment)
returned 2,715.8 / 2,483.1 / 2,390.8 mixed QPS. The identical baseline changed by
12.0%; mean host load increased from 10.2 to 14.4. There is no 100K A/A calibration.
Accordingly the +10% throughput/cold-read and +5% tail/export targets are
**INCONCLUSIVE**, not passed. RSS results are observations rather than a proven
production envelope. A controlled host is required to qualify these targets.

An earlier prototype regressed 10K query throughput by about 25%. Request-local
sharded maps and empty-map allocations were removed; field-index copies were
bounded, and entity lookup inlining was restored. Those development measurements
are retained separately and are not presented as measurements of the final binary.

The 100K development run at `a3ff0a7f` reached a 35.0 s ingest P95. Profiles found
redundant JSON normalization in incremental edge indexes; scheduling logs also
showed automatic full rebuilds racing ordered incremental updates. Removing those
costs reduced the observed P95 to 20.4 s at `06827b7a`. GC phase metrics then exposed
long read-admission waits around pinned views. Longer read windows and fair
cancellation of overlapping maintenance waiters produced the final 16.6 s ingest
and 92.3 ms match P95 observations. These fixes preserve the exclusive deletion guard.

The remaining cost includes Parquet page rewrites, durability syncs, full rebuilds,
query filtering/sorting and snapshots. A 50 ms batch budget is checked between
files, so a single large file or four in-flight files can exceed it. Large tenants
may be admitted alone above the estimate budget; this does not cap process RSS.

## Correctness and publication gates

- OrbStack full Go unit tests and vet passed on the field-index candidate;
  graph/query race tests passed after the final read-path adjustment.
- [GitHub CI for the final runtime](https://github.com/SamuelSupe/graphdb/actions/runs/36409781536)
  passed full unit/vet/race, Go/Python SDKs,
  direct/WAL HTTP, container persistence/restart, and actual S3 multipart backup
  followed by fresh-directory restore/restart.
- Every completed WAL measurement ran six maintenance tasks without task errors,
  drained the index backlog, preserved entity counts, and verified restore content.
- The downloadable stable release is produced only after the tag workflow also
  passes the 30-minute compact/GC/index-rebuild soak. Its actual reports are in
  the archive's `release/evidence/`; a queued workflow is not a successful gate.

Correctness results do not prove an absence of all bugs or qualify arbitrary
production capacity. This is a single-process local-disk release with S3 snapshots,
not a replication or high-availability release.
