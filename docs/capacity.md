# Capacity Envelope And Topology

[中文](capacity.zh-CN.md)

GGraphDB 1.3 defines a release-gated multi-writer envelope instead of an
unbounded “large graph” claim. The machine-readable contract is
`release/capacity-envelope.yaml`. Historical 1.2 throughput numbers below are
kept as historical evidence only and do not certify the 1.3 WAL profile.

## 1.3 Contract (evidence pending)

The 1.3 PostgreSQL-CAS WAL profile requires evidence for:

- 2, 4, and 8 writers concurrently receiving one tenant's ingest batches;
- zero lost requests, duplicate graph versions, or duplicate effects for
  cross-writer idempotency races;
- successful CAS-order publication with batch rebase and repeated-conflict
  shrinking, without terminal failure caused by a retry budget;
- local durable acceptance during a temporary PostgreSQL outage until the WAL
  high-water policy rejects new admission;
- process-crash recovery from `ACCEPTED` through `FINALIZED` with the original
  writer volume and stable `GRAPHDB_INSTANCE_ID`;
- lifecycle fencing, owner-routed status, and `recovery_pending` behavior; and
- rolling coexistence of 1.2 direct writers and 1.3 WAL writers, including
  drain-before-downgrade.

No 1.3 performance result is claimed until these runs produce commit-bound
evidence. The supported durability boundary is process failure with the
original WAL volume; permanent loss of that volume is outside the contract.
The scaling target is across tenants. Eight same-tenant writers are a
correctness and availability boundary, not a claim of linear single-tenant
throughput.

## Historical 1.2 Release Gate

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
- the tagged 1.0 binary reads the final mirrored graph version;
- both directions of the real 1.0/1.1 binary compatibility test.

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
| Concurrent production | PostgreSQL + generic S3 | 2–8 | 2+ | 1.3 WAL uses independent writer volumes and owner routing; PG provides coordination CAS while object storage remains graph authority |

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
- The historical 1.2 envelope supports 2–8 deployed writers and measured about
  20 commits/s per hot tenant with two active contenders. That result does not
  certify 1.3 WAL. In 1.3, eight-way same-tenant concurrency remains a
  correctness/availability boundary, not a sustained-throughput claim; the
  throughput scale target is across tenants. Higher single-tenant throughput
  requires graph/entity partitioning and a new capacity envelope.
- The published 20 commits/s profile keeps automatic compaction enabled, uses
  a 1,000-entry compact threshold and a 1,500-entry write-backpressure limit,
  and runs maintenance at least every 30 seconds.
- Object-store latency, PostgreSQL latency, field width, index count, and
  relation density materially change capacity. Re-run the baseline in the
  deployment region with production-equivalent data.

Do not size production from entity count alone. Record graph size, average
field bytes, relation density, active indexes, commit rate, query mix, p95/p99,
memory high-water mark, object count/bytes, and PostgreSQL CAS conflict rate.
