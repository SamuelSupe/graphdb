# GGraphDB 2.4.3 validation

Published **2026-10-29T13:30:00Z**: [GitHub release](https://github.com/SamuelSupe/graphdb/releases/tag/v2.4.3).
All required gates and independent downloaded-asset checks passed with
**Go 1.27.3**, without a waiver. The immutable archive contains its candidate-time
record and completed gate evidence. This final record is on main; the CI for this
later documentation commit is checked separately after push.

The later documentation CI exposed a timing dependency in the controlled
index-downgrade fixture: background compaction could still publish a catalog
while the fixture changed layout configuration and rebuilt. The fixture now
waits for that work before changing configuration; cursor and downgrade
assertions are unchanged. This test-only correction does not alter the released
runtime, tag or assets. Twelve focused race repetitions across both old layouts
passed on OrbStack Linux ARM64 (437.878s). Its final CI is checked separately
from the release gate.

## Release identity and gates

- Annotated tag `v2.4.3`, object `72635c319bb7d75310c12d5281190e6a03e97554`.
- Release commit `e50be0c383eeb4ea1224f2103ab25fac7c484f59`; build date `2026-10-29T20:05:01+08:00`.
- [Exact candidate main CI](https://github.com/SamuelSupe/graphdb/actions/runs/40870664907): all required jobs passed.
- [Exact-tag release CI](https://github.com/SamuelSupe/graphdb/actions/runs/40875224140): all required jobs passed, including publication.

Full unit/vet/race/SDK, direct/WAL/restart, S3 backup, gateway authorization,
released-v2.3.0 rolling upgrade, enhanced snapshots/recovery, protocol-3 GC,
standalone and Raft thirty-minute soaks, package extraction and checksums retain
their original workloads and failure rules. Both packaged soaks have zero
operation, query and write errors. Expected backpressure and configured-end
cancellation semantics are unchanged. The packaged AMD64 binary matches the
Raft, enhanced, protocol-3, rolling and gateway gate identities.

## Recovery regression and local qualification

The candidate includes inode admission/cleanup, protected legacy prepared-WAL
replay, automatic standalone metadata recovery, metadata-first initialization,
validated migration inputs, metadata-only creation and secondary-copy repair.
It also includes the read-drain, mutation-forwarding and cached-router status
repairs merged after v2.4.2.

An independent process probe reproduced a stale conditional-write cache blocking
recovery after actual metadata files were removed or damaged. Missing-primary,
missing-both, corrupt-primary and corrupt-pending regressions failed before the
fix. Fresh recovery reads now invalidate only cached digests that disagree with
disk. Conditional writes, healthy cache entries and original state/purge rules
remain intact. Focused race regressions and actual process recovery passed.

The exact final source passed full OrbStack Linux ARM64 Go 1.27.3 unit/vet/race,
SDK and direct/WAL real HTTP/restart gates. Full race package times were HA
461.805s, storage 338.689s, replication 156.154s and HTTP 15.229s.

Three one-second metadata-candidate benchmark repetitions with GOMAXPROCS=4
reported a final median of 1.933 ms and 1,044,784 bytes allocated per operation;
the initial baseline was 3.913 ms and 2,068,871 bytes. This measures local
metadata reads/decoding, not service throughput, peak RSS or production latency.
Backup-only repair separately verifies that the primary file inode is retained.

## Independent source and assets

All **1161 GitHub source files** match the immutable
tag's Git blobs. The archive SHA-256 `e5bc58254c35c408c7845d78ca49693aa4b89123d9f25d6972af00d6f38105de` matches both
the external checksum and GitHub asset digest; the checksum asset digest also
matches. All **1129 packaged source/configuration
files** match the tag, and required source trees are complete.

All three binary checksums, target architectures, CGO-disabled build settings,
Go 1.27.3 and executed version/commit/date identities passed. Linux AMD64 version
execution used OrbStack's compatibility runtime; ARM64 service execution was native.

| Binary | SHA-256 |
| --- | --- |
| bin/graphdb-darwin-arm64 | `64ad94fe9bdcade72dae2add2db5dd3df10bf5b6c41b74b5c516ac5cfb2450d6` |
| bin/graphdb-linux-amd64 | `481c254b2b1b19e3abad42fba6a2f0da75439979d74e78b74635c4f7eb9ebed4` |
| bin/graphdb-linux-arm64 | `c36aeeb72a7cce1f5702c5b94a1e60abdea0ca1ae107c89409d440cbe917486b` |

The downloaded ARM64 binary matches the locally qualified exact candidate.

## Downloaded-binary runtime and recovery

The actual downloaded Linux ARM64 binary and verified packaged SDK/e2e/load
tools passed in fresh OrbStack directories with both range and adjacency pages
enabled, independently in direct and WAL modes:

| Mode | Load operations | Errors | SDK, SIGKILL replay, backup/fresh restore |
| --- | ---: | ---: | --- |
| direct | 98 | 0 | PASS |
| wal | 98 | 0 | PASS |

The complete exported graph and accepted batch identity/version survived
SIGKILL/restart and runtime backup/restore to a new directory. A separate actual
downloaded-binary probe passed missing-both-copy active reconstruction with an
unchanged graph, corrupt-primary recovery retaining disabled state and fields,
backup-only repair retaining the primary inode, and corrupt-purge rejection of
automatic reconstruction.

## Compatibility and limits

Standalone recovery is enabled for every tenant without configuration. Valid
pending/backup records retain state and fields. Without a valid copy, only a
valid manifest and no active purge fence permit an active fallback; active,
unknown or corrupt purge state does not authorize revival. Raft reads never
perform independent local repair writes.

One stable metadata backup and a temporary pending record are added per tenant.
Complete pending recovery with the new binary before downgrade. The existing
two-slot WAL-marker restriction from 2.4.2 remains. Range/adjacency indexes stay
off by default; upgrade all participants before activation and rebuild legacy
layouts before downgrade. HTTP/module namespaces, logical hashes and Raft
protocol identifiers remain unchanged.

Inode admission is not an atomic resource reservation. An exhausted filesystem
may still need capacity recovery or migration before replay. Automatic metadata
recovery does not reconstruct missing graph data, bypass PREPARED checks or
establish the historical cause of missing metadata. Migration validation still
requires staging-copy IO; non-local target IO failures lack whole-copy rollback.
Cross-host recovery, production capacity/latency and day-scale stability remain
unqualified. Existing tags/assets are unchanged; unrelated local design documents
are excluded from this release.

[Machine-readable evidence](validation-v2.4.3.json) ·
[Operations](product-operations.zh-CN.md) · [Compatibility](naming-and-compatibility.md).
