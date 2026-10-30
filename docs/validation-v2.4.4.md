# GGraphDB 2.4.4 validation

Published **2026-10-30T02:30:27Z**: [GitHub release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.4).
All required exact-tag gates and independent downloaded-asset/runtime checks
passed with **Go 1.27.3**, without a waiver. The immutable archive retains its
candidate-time record and completed gate evidence. This final record is added
on main after publication; its documentation CI is checked separately.

## Release identity and gates

- Annotated tag `v2.4.4`, object `366d3aa6943ec4f41aef76a2eaeb151fe710fa30`.
- Release commit `66eb2144e3f0b03c75d80bdcbbf9c38ebcc40b04`; build date `2026-10-30T09:03:57+08:00`.
- [Exact candidate main CI](https://github.com/SamuelSupe/graphdb/actions/runs/40983575226): all required jobs passed.
- [Exact-tag release CI](https://github.com/SamuelSupe/graphdb/actions/runs/40986795151): all required jobs passed, including publication.
- Both standalone and Raft thirty-minute soaks completed with zero operation,
  query and write errors. Original workloads and failure rules are unchanged.
- Protocol-4 CI/source identity tuples remain distinct on every replica after
  runtime recovery and serial restart. Protocols 1–3 and the released-v2.3.0
  rolling window retain their existing gates.

## Independently downloaded source and assets

All **1163 GitHub source files** match the tag's
Git blobs. All **1131 packaged source and
configuration files** match the tag, and required source trees are complete.
Archive SHA-256: `0853e11e4da17b31302fc5934dd3875b51846199439e8a5f431f80bda02b26c1`; both checksum layers and GitHub asset
digests match. The packaged AMD64 binary matches every Raft, rolling and gateway
qualification report, including protocol 4.

All three binaries passed architecture/build-setting checks and actual version,
commit and build-date execution. Linux AMD64 version execution used OrbStack's
compatibility runtime; ARM64 service execution was native.

| Binary | SHA-256 |
| --- | --- |
| bin/graphdb-darwin-arm64 | `2d03a8273363c10ca7cd21f08ddc382c21d56c5c3a8fc186a3ddea02780d56d4` |
| bin/graphdb-linux-amd64 | `8f58b679d3ffb97169a9c2cde4506cba6efd4ab715e47ce1f75520f4d8868c2b` |
| bin/graphdb-linux-arm64 | `97e2479168fea673e86bd1e78097329d91d8e52b5d5585bd2d0d000f758befa3` |

The downloaded Linux ARM64 binary passed SDK, direct/WAL HTTP, concurrent load,
SIGKILL replay and fresh-directory runtime backup/restore. Rebuilding the
verified packaged source with identical build settings reproduced it byte for
byte. Additional real-process probes passed old-commit cold replay, distinct
new identities, invalid v2.4.3 WAL recovery with durable failed outcomes, and
finite average/overflowing-sum response checks.

Online metadata probes also passed missing-both-copy active recovery without
graph changes, corrupt-primary recovery preserving disabled state and fields,
backup-only repair retaining the primary inode, and corrupt-purge rejection.

## Scope and local evidence

The candidate fixes composite identity collisions, invalid ingest identifiers
blocking WAL progress, quadratic append_unique behavior and numeric aggregation
responses. Commit identity encoding preserves old replay semantics; Raft protocol
4 separates new semantics from protocols 1–3. The default remains protocol 1.

OrbStack Linux ARM64, Go 1.27.3 checks passed during remediation: full unit tests,
vet, complete graph/query/HTTP race tests and focused storage/HA races. Final
supplemental tests cover preserving entity timestamps, versions and ownership
while caching the upgraded identity index after a no-op write. CI also passed the full
suite against the immutable release commit, including the full race suite.

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

## Completed release gates

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

[Machine-readable evidence](validation-v2.4.4.json).
