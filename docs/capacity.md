# Capacity Envelope And Topology

[中文](capacity.zh-CN.md)

GGraphDB 1.2 publishes a reproducible release envelope instead of an unbounded
“large graph” claim. The machine-readable contract is
`release/capacity-envelope.yaml`.

## 1.2 Release Gates

The primary write-throughput gate is one local writer using sync WAL and
segment metadata on a fixed 8 CPU/8 GiB OrbStack container. It runs eight
tenants and 16 collectors for 30 minutes, five times for v1.1.5 and five times
for v1.2.0. Every candidate run must sustain at least 10,000 committed
mutations/s; the candidate median must be at least 1.5x the baseline and its
run-to-run spread at most 5%. Accepted p95/p99 are capped at 20/50 ms,
committed p95/p99 at 8/15 seconds, RSS at 7 GiB and 110% of baseline, CPU per
1,000 mutations at 75% of baseline, and direct-write/query regression at 10%.
RSS is sampled from the writer process `VmRSS`; CPU is read from the writer
container cgroup and normalized per 1,000 committed mutations.

Run the complete commit-bound matrix with:

```sh
scripts/wal_performance_matrix.sh
```

The GitHub release job depends on a self-hosted `orbstack` runner and refuses
to publish unless all five candidate runs and every relative gate pass.

### Local-WAL runtime defaults

These are v1.2.0 local-writer defaults, separate from the fixed-host capacity
profile and its intentionally stricter test thresholds:

| Setting | Default |
| --- | ---: |
| WAL durability | `sync` |
| Maximum pending age | `2m` |
| Graph flush | `250ms`, flush trigger 8 requests / 2 MiB (busy tenants may merge the same-round queue), 2 workers |
| Metadata flush | `500ms`, trigger 256 requests / 8 MiB, 2 workers |
| Write cache | `4GiB` |
| Commit-tail limit | `20,000` |
| Heavy background task execution | 1 concurrent task |
| Maintenance ingest-idle window | `1m` per tenant |

The PostgreSQL direct-write path remains a correctness and regression gate:

For each release candidate, CI must pass:

- an eight-writer concurrency correctness gate against generic S3/RustFS;
- two active writers sustain 20 successful commits per second for one tenant
  for 30 minutes;
- at least 90% of target throughput after pacing and retries;
- zero lost commits, duplicate graph versions, or terminal write errors;
- the final graph entity count and graph version equal the scheduled commit
  count;
- each server request keeps the public eight-replay CAS limit; the load client
  retries a returned `write_conflict` with capped jitter up to two seconds,
  rotates retries across writer instances, and retries at most 64 times;
- online compaction runs whenever the commit tail reaches 1,000 entries and must
  preserve concurrent commits without lowering the graph version;
- legacy-mirror and derived-index workers run during the load, then converge to
  zero mirror lag and zero pending backlog;
- the tagged 1.0 binary reads the final mirrored graph version.

This is a concurrency and durability envelope, not the maximum supported graph
size. A short 200-commit run is only a smoke test and cannot certify a release.
Set `GRAPHDB_TEST_CAS_STRESS_REPORT=/path/report.json` when running
`scripts/postgres_cas_gate.sh soak` to retain the machine-readable release
evidence. CI binds that report to the tested commit and packages it with the
release archive.

### Historical throughput baseline

An earlier 30-minute data-path run completed on 2026-07-23 against local
OrbStack PostgreSQL and RustFS:

| Metric | Result |
| --- | ---: |
| Active writers / duration | 2 / 30 minutes |
| Target / committed | 36,000 / 36,000 |
| Throughput | 19.96 commits/s (99.78% of target) |
| Online compactions / recoverable CAS conflicts | 36 / 377 |
| Final graph version / head revision | 36,000 / 36,036 |
| Final entities / snapshot / commit tail | 36,000 / 36,000 / 0 |

The exact report is
[`release/evidence/cas-stress-orbstack-2026-07-23.json`](../release/evidence/cas-stress-orbstack-2026-07-23.json).
That schema-1 report predates the mirror/derived backlog checks, tagged 1.0
binary read, and tested-commit binding now required by the full release gate.
It is therefore a sustained-throughput baseline, not current release
certification or a hardware guarantee. Every release candidate must produce a
fresh schema-2 report in CI, and the same gate must be rerun in the target
deployment environment.

## Reproducible Baseline

Run against the OrbStack RustFS writer/reader stack:

```sh
CAPACITY_PROFILE=smoke scripts/capacity_baseline.sh
CAPACITY_PROFILE=baseline scripts/capacity_baseline.sh
```

`smoke` creates a small mixed read/write check. `baseline` runs:

| Case | Writers | Readers | Batches | Groups per batch | Planned graph |
| --- | ---: | ---: | ---: | ---: | ---: |
| `mixed-10k` | 4 | 8 | 50 | 200 | 20,002 entities / 10,001 edges |
| `write-25k` | 8 | 0 | 50 | 500 | 50,002 entities / 25,001 edges |

Each run writes JSON reports under `capacity-runs/<timestamp>/`, including
configuration, planned graph size, status counts, errors, and p50/p95/p99/max
latency. These local artifacts are intentionally ignored by Git; attach them
to the release evidence or performance system.

## Recommended Topologies

| Use case | Coordination | Writers | Readers | Notes |
| --- | --- | ---: | ---: | --- |
| Development or evaluation | local files | 1 | 0 | `GRAPHDB_MODE=all`; no HA |
| Small production | local + generic/native object store | 1 | 2+ | writer lease; readers scale independently |
| Concurrent production | PostgreSQL + generic S3 | 2–8 | 2+ | Two active contenders per hot tenant in the published envelope; PG head is authoritative |

Starting validation resources—not support guarantees—are 4 vCPU/8 GiB for a
writer, 2 vCPU/4 GiB for a reader, and an HA PostgreSQL service sized for the
write rate. Increase reader count for query concurrency. Deploying up to eight
writers provides placement and failover capacity, but sending one hot tenant
through eight continuously active contenders is not a linear throughput scale
strategy.

## Sizing Rules

- A writer materializes the current tenant graph and applies copy-on-write
  mutations. Size memory from the target graph, fields, and commit batch, then
  retain headroom for one mutation and compaction.
- Query result `limit` is capped at 1,000. Use scans/streams for bulk export.
- Keep normal collector batches near 200–500 logical groups. Very small batches
  amplify commits, manifests, idempotency records, and collector state.
- PostgreSQL direct mode supports 2–8 deployed writers and retains the 20 commits/s
  per hot tenant with two active contenders. Eight-way same-tenant concurrency
  remains a correctness gate, not a sustained-throughput claim. Higher
  throughput requires graph/entity partitioning and a new capacity envelope.
- The published 20 commits/s profile keeps automatic compaction enabled, uses
  a 1,000-entry compact threshold and a 1,500-entry write-backpressure limit,
  and runs maintenance at least every 30 seconds.
- Object-store latency, PostgreSQL latency, field width, index count, and
  relation density materially change capacity. Re-run the baseline in the
  deployment region with production-equivalent data.

Do not size production from entity count alone. Record graph size, average
field bytes, relation density, active indexes, commit rate, query mix, p95/p99,
memory high-water mark, object count/bytes, and PostgreSQL CAS conflict rate.
