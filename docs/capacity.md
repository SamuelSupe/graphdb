# Local disk capacity and measurement

[中文](capacity.zh-CN.md)

This edition runs one process with concurrent clients and tenants. Memory,
local disk latency, graph fields, relation density, and active indexes determine
capacity. Entity count alone is insufficient. Writer and reader graph caches
remain bounded; a graph exceeding the cache budget is loaded without caching.
Size both caches for the workload when predictable query latency is required.

## Validation profiles

- `scripts/capacity_baseline.sh`: short API smoke and finite load profiles.
- `scripts/local_disk_benchmark.py`: fixed-size, timed comparison of baseline
  `ffa85414` S3, baseline local, and the new local implementation. It requires
  prebuilt baseline/candidate/loadtest binaries and an isolated Linux volume.
- `GRAPHDB_GATE_SOAK=1 RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh`:
  30 minutes, four ingestion clients, sixteen query clients, and background
  compact, GC, and index rebuild. Its richer CMDB graph uses 2 GiB read/write
  cache budgets and a five-second writer interval to bound graph growth.

See [local disk operations](local-disk.md) for durability and configuration.
The comparison uses 10,002 and 100,002 entities, one minute warmup, five minutes
measurement, and three repetitions with rotating server order. Reports retain
published throughput, latency percentiles, WAL acceptance-to-readable latency,
RSS/CPU, process I/O, GraphDB fsync counters, and descriptors. MinIO runs only
for the historical baseline and is not a dependency of this edition.

Process-cold restarts and OS page-cache state must be reported separately.
A shared OrbStack host is not dedicated hardware: record variation and treat
noisy results as inconclusive. Historical distributed throughput reports remain
historical evidence and do not certify local disk performance.
