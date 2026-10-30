# GGraphDB 2.4.4 validation

This is the candidate-time record. Exact-tag CI, publication and independently
downloaded assets remain **PENDING** until their terminal results are recorded.
The release workflow blocks publication until every required gate passes; no
waiver is requested. Older tags and assets remain unchanged.

## Scope and local evidence

The candidate fixes composite identity collisions, invalid ingest identifiers
blocking WAL progress, quadratic append_unique behavior and numeric aggregation
responses. Commit identity encoding preserves old replay semantics; Raft protocol
4 separates new semantics from protocols 1–3. The default remains protocol 1.

OrbStack Linux ARM64, Go 1.27.3 checks passed during remediation: full unit tests,
vet, complete graph/query/HTTP race tests and focused storage/HA races. Final
supplemental tests cover preserving entity timestamps, versions and ownership
while caching the upgraded identity index after a no-op write. CI runs the full
suite again against the immutable release commit, including the full race suite.

A real-process probe against the released v2.4.3 binary reproduced an accepted
256-byte source blocking subsequent valid writes. After SIGKILL and upgrade,
the invalid unprepared batch becomes failed, the valid batch commits, and both
outcomes survive another restart. Old identity-collision commits cold-load with
their historical result; new CI/source tuples remain distinct after restart.
Two finite 1e308 values average to 1e308; their overflowing sum returns a
structured error instead of an empty success response.

Three same-host HTTP samples per size compared the v2.4.3 binary with the local
candidate (2 CPU quota, same synthetic string arrays, independent fresh tenants,
initial population before measurement). Median repeated append_unique latencies:

| Elements | v2.4.3 | Candidate |
| --- | ---: | ---: |
| 4,000 | 273.75 ms | 2.56 ms |
| 8,000 | 1,152.27 ms | 4.27 ms |
| 16,000 | 4,254.61 ms | 7.04 ms |

This is a short focused regression measurement, without a dedicated warmup or
isolated page-cache protocol. It is not a production latency, throughput or
capacity claim. No-op writes preserve logical data, versions and provenance.

## Required release gates

- Exact-source unit, vet, full race, SDK, console-boundary and script checks.
- Direct/WAL HTTP, load, backup/restore and process restart.
- S3 snapshot and gateway authorization gates.
- Existing released-v2.3.0 rolling window, protocol-2 enhanced snapshots/recovery,
  protocol-3 GC/recovery, and new protocol-4 process/recovery/rolling checks.
- The unchanged standalone and Raft protocol-3 thirty-minute mixed-workload soaks.
- Three binary builds, checksum/package checks and evidence-to-binary identity.
- After publication: remote tag/commit, independently downloaded checksums,
  source contents, binary identity and actual downloaded-binary recovery.

## Compatibility limits

Single-node new writes enable tuple matching automatically. All tenants retain
the existing automatic metadata recovery policy. New commits are not readable
by older binaries, and previously merged entities are not automatically split;
rollback requires a verified pre-upgrade backup. Existing WAL-marker and
range/adjacency-index downgrade boundaries still apply.

Raft activation requires every voter to support protocol 4. Old logged commands
retain their original execution semantics; protocol 4 can retire invalid legacy
accepted requests without changing successful neighbors. Already prepared local
publication cohorts retain their checks and are not split or discarded.

Cross-host reliability, day-scale stability, production capacity and a general
latency SLO remain unqualified. Unrelated local design documents are excluded.
