# GGraphDB 2.4.2 validation

Published **2026-10-23T17:30:18Z**: [GitHub release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.2).
All required gates and independent download/runtime checks passed with **Go1.27.3** and no waiver.
The immutable archive retains its candidate-time record and completed gate evidence.
This final record is on main; this documentation commit's own CI is checked separately after push.

## Release identity and gates

- Annotated tag `v2.4.2`, object `4508023e34661e10c3524500fb024de6bdf28fb3`.
- Release commit `3f72cac3a8dfe2d8f6ae068fc48533714bb30bf5`; build date `2026-10-23T23:44:24+08:00`; Go1.27.3, CGO disabled.
- [Candidate main CI](https://github.com/SamuelSupe/graphdb/actions/runs/39983403910): all seven jobs successful, including the standard Dockerfile build/start.
- [Exact-tag release CI](https://github.com/SamuelSupe/graphdb/actions/runs/39988418183): all seven jobs successful, including actual asset publication.

Complete unit/vet/race/SDK, direct/WAL/recovery, S3 backup, production gateway,
real released v2.3.0 rolling, enhanced snapshots/maintenance/recovery, protocol3,
both thirty-minute soaks, package extraction and checksums retain their original
workloads, deadlines, query budgets and zero-error criteria. The packaged AMD64
binary matches the Raft, enhanced, protocol3, rolling and gateway qualification
identities. Gateway configuration, fixture and client hashes also match.

| Exact-tag workload, four writers / sixteen readers | Result |
| --- | --- |
| Standalone WAL, 30 minutes | PASS; 1,350 ingest HTTP 200 responses; zero operation, query and write errors |
| Three-replica Raft WAL, 30 minutes | PASS; 883 ingest HTTP 200 responses, 132 allowed HTTP 429 responses and two attempts canceled at the configured end; zero operation, query and write errors |

The original expected-backpressure and end-cancellation rules remain unchanged.
Measured start-to-finish durations were 1,800.162 and 1,802.443 seconds respectively.
These results are not production capacity or latency claims.

## Stable regression and local qualification evidence

The immutable [v2.4.1 qualification failed](validation-v2.4.1.md) with 23 query
errors in its original Raft soak and published no assets. Its log and workload
evidence were read before remediation; its public tag remains unchanged.

A control operation revoking a completing read and a lost POST-query response
reported as an unknown write each reproduced **3/3 FAIL** before repair. Both
regressions plus the original cancellation, blocked-socket control deadlines and
nonretryable unknown-write assertions passed **three race repetitions** in
OrbStack Go1.27.3 (37.856s). Read drain permits at most one second before slow
readers are revoked; it adds no storage I/O, extends no control deadline and does
not automatically retry unknown mutations.

The initial full race run then exposed a test completion notification sent before
its deferred maintenance-slot release. A diagnostic scheduling gate reproduced
the same capacity assertion 3/3 times; the same gate passed three race repetitions
(15.598s) after moving notification behind release. Ordinary baseline repetitions
also remain recorded. This was a test-only ordering repair; batch, capacity and
deadline assertions were retained, with no production leak inferred from it.

The exact final release source passed full OrbStack Go1.27.3 unit/vet/race with
actual exit zero. Race times: HA 640.019s, HTTP 20.969s, replication 162.271s and
storage 479.688s. The local original-parameter protocol3/Raft/sharded thirty-minute
gate also passed with zero errors; the only subsequent source change was the
test ordering repair. Exact final-build main and tag CI separately passed.

## Independent downloaded source and assets

- All **1,153 GitHub source files** match the immutable tag's Git blobs; source archive SHA-256 `2450c8cdb70ece531d2d06c9230221b8e053d21d8d53e0ecb74e192ca490502d`.
- Published archive SHA-256 `a3e9b35114ea506cdc9dda0751322aa3950391c0361f9655a416b9bb5ea903d6` matches the external checksum and GitHub asset digest. The checksum asset digest also matches.
- **1,121 packaged source/configuration files** match the tag; required source trees are complete.
- All three binary checksums, advertised architectures, CGO-disabled build settings, Go1.27.3 and actual version/commit/date execution passed. Linux AMD64 version execution used OrbStack's existing compatibility runtime; ARM64 service verification was native.

| Binary | SHA-256 |
| --- | --- |
| Linux AMD64 | `c4041621c0a17d0b30a871f45de8fef6f91fe8273d58a809886878cad969348e` |
| Linux ARM64 | `7c104a6c356c7fe108ff2ab04b076a27c1fce2b0f7f5f780d7fa27580865ec4e` |
| macOS ARM64 | `e7f373f522ea790a7c6804b738655ebf5a804d865f7a8c484aae0842578a4442` |

The downloaded Linux ARM64 binary is byte-identical to the qualified final
candidate and to a rebuild from downloaded source with the original release
flags and build identity.

## Downloaded-binary recovery

The actual downloaded Linux ARM64 binary and verified downloaded-source
SDK/e2e/load tools passed in fresh OrbStack Ubuntu directories with range and
adjacency pages explicitly enabled, independently for direct and WAL modes:

- Four writers / sixteen readers, 98 measured operations per mode, zero errors.
- Fourteen live Python SDK tests per mode and e2e API checks passed.
- SIGKILL and restart preserved accepted batch identity/version/data and the complete graph.
- Runtime backup followed by restore into a new directory preserved replay results and the complete graph.
- Actual runtime and byte-identical rebuild verification exited zero.

## Compatibility and limits

Range and adjacency pages remain disabled by default. Upgrade all related
readers/writers/GC nodes and replicas before enabling them; rebuild the legacy
layout with the new binary before an index downgrade. Existing layouts and
global ListEdges cursor contracts remain supported. Read optimization does not
add storage writes or weaken bounded-cost regressions.

Opening a v1 WAL durable marker migrates it to two-slot v2, which old binaries
cannot read. Drain WAL before downgrade and preserve the whole WAL directory
plus verified pre-upgrade backups. This does not qualify a whole WAL/Raft
directory downgrade. Cross-host recovery, production capacity and day-scale
stability are not qualified here. Four pre-existing user documents remain
untouched by these release commits. Existing public tags and release assets
remain immutable; v2.3.9 and v2.4.1 retain no published assets.

[Machine-readable identities and results](validation-v2.4.2.json). Final main CI
for this record is checked after push; no document certifies its own later run.
