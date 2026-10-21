# GGraphDB 2.4.0 validation

Published **2026-10-21T09:29:54Z**: [GitHub release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.0).
All required gates and independent download/runtime checks passed without a waiver.
This final record is on main; the immutable archive retains its candidate-time
record and the actual completed gate evidence. This documentation commit's own
main CI is checked separately after push and does not rebuild the release.

## Release identity and gates

- Annotated tag: `v2.4.0`, object `c881d4b5517ee0ece95ccff6c47b198aeb064260`.
- Release commit: `4e08c5ad5cf7e88e7ca31f8d2ff79ba2026acefb`; build date `2026-10-21T16:01:01+08:00`; Go 1.26.7.
- [Candidate main CI](https://github.com/SamuelSupe/graphdb/actions/runs/39566127462): all seven jobs successful.
- [Exact-tag release CI](https://github.com/SamuelSupe/graphdb/actions/runs/39570572123): all seven jobs successful, including actual asset publication.

Required gates retain their original workloads, deadlines and failure standards:
complete unit/vet/race/SDK, direct/WAL/recovery, S3 backup, production gateway,
real released v2.3.0 rolling, enhanced snapshots/maintenance/recovery, protocol-3
GC and both thirty-minute soaks. The packaged AMD64 binary matches the Raft,
enhanced, protocol-3, rolling and gateway qualification identities. Gateway
configuration, identity fixture and client hashes also match their evidence.

| Exact-tag thirty-minute workload | Result |
| --- | --- |
| Standalone WAL, four writers / sixteen readers | PASS; 1,347 ingest HTTP 200 responses; zero operation, query and write errors |
| Three-replica Raft WAL, four writers / sixteen readers | PASS; 861 ingest HTTP 200 responses and 145 allowed HTTP 429 backpressure responses; zero operation, query and write errors |

The 429 responses retain the original gate's expected-backpressure treatment;
the load, duration, query budgets and zero-error criteria were not changed.

## Independent downloaded source and assets

- GitHub tag source: all **1135 tracked files** match the immutable tree's Git blobs; source archive SHA-256 `57be52f5915d8a70d84d429fb88d4c7edfe86d2929e0af777303ec8a62e20dbc`.
- Published archive SHA-256 `7662c8f814a0751f4791ac8634c53a5a3ecf202a7403956e45c4c5947edc5fbb` matches the external checksum and GitHub asset digest. The checksum asset's digest also matches.
- **1103 packaged source/configuration files** match the tag, with required source trees complete.
- All three packaged binaries match SHA256SUMS, Go 1.26.7, their advertised architecture, CGO disabled and actual version/commit/build-date execution. Linux AMD64 version execution used OrbStack's existing compatibility runtime; ARM64 service validation was native.

| Binary | SHA-256 |
| --- | --- |
| Linux AMD64 | `95c16426a54e24fe17ac921735c891ce7ee3e64f8f76dbe2f46185dedbae7abf` |
| Linux ARM64 | `fe7d8d8d18cfd0cfcc6b8427d0fd08a74fced926fd56a7c85140ec16b3e9be47` |
| macOS ARM64 | `9139a3eb8e8d03166079afc767542a405cfa35ffc606a5117ba81e4f3b6cff55` |

The downloaded Linux ARM64 binary is byte-identical both to the locally qualified
candidate and to a rebuild from downloaded source using the original release
flags and build identity.

## Downloaded-binary runtime recovery

OrbStack Ubuntu ARM64, Go 1.26.7, with both range and adjacency pages explicitly
enabled. The actual downloaded Linux ARM64 binary and verified package SDK/e2e/load
tools passed direct and WAL modes independently:

- Live Python SDK: fourteen tests per mode; e2e API checks passed.
- Four writers / sixteen readers: 98 measured operations per mode, zero errors.
- SIGKILL and restart preserve the complete graph and accepted batch ID/version/data.
- Runtime backup followed by restore into a fresh directory preserves the complete graph and replay result.
- Actual process exit was zero; no running local session is counted as a pass.

Candidate preparation separately passed OrbStack docs/SDK/cmd tests, whole-repo
vet, version contracts, hygiene, Python SDK, backup automation and rolling-barrier
regressions. The [preceding source review](validation-review-3d6bdaed-2026-10-21.md)
records the stable before/after reproducers and complete local unit/vet/race for
the unchanged production core; the candidate and exact-tag CI qualify the actual
release revision.

## Compatibility and limits

Range and adjacency write flags remain disabled by default. Upgrade all readers,
writers, GC nodes and replicas before activation; explicitly rebuild existing
adjacency indexes. Legacy layouts and global ListEdges cursors retain their
contracts. Old binaries cannot read new catalogs; rebuild the legacy layout
using the new binary before an index downgrade. This does not qualify a full
WAL/Raft directory downgrade. See [implementation boundaries](entity-range-paging-implementation.zh-CN.md)
and [version compatibility](naming-and-compatibility.md).

Parent-directory cost still grows with page count. Cross-host recovery,
production capacity, production latency and day-scale stability are not
qualified by these checks. Thirty-minute gate results are not capacity claims.
The three pre-existing user documents stay outside release commits. Earlier
tags and release assets are preserved; v2.3.9 remains an unsuccessful qualification
tag without assets. Machine-readable identities and results: [JSON](validation-v2.4.0.json).
