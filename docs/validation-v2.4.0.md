# GGraphDB 2.4.0 validation

Candidate preparation: 2026-10-21. This record separates prior source qualification,
candidate checks, exact-tag release gates and independent downloaded-asset checks.
No gate waiver applies. A source push or tag alone is not a completed release.

## Source and compatibility

The candidate carries the fixes through `dc4a6ca43b427a1b881d464aa86e855c480c9eda`.
Its [source-review validation](validation-review-3d6bdaed-2026-10-21.md) records
stable before/after reproducers, complete OrbStack Go 1.26.7 unit/vet/race and
direct/WAL runtime results. That source also passed all seven jobs in
[main CI](https://github.com/SamuelSupe/graphdb/actions/runs/39561408303).
These are baseline evidence, not an exact v2.4.0 tag qualification.

2.4.0 synchronizes VERSION, SDKs, OpenAPI, deployment examples and current
documentation. Range and adjacency write flags remain disabled by default.
Read/write bounds, legacy layouts and global ListEdges cursor contracts remain
as documented in the [implementation](entity-range-paging-implementation.zh-CN.md)
and [compatibility rules](naming-and-compatibility.md). Parent-directory costs
still grow with page count. Cross-host recovery, production capacity and
day-scale stability are not qualified by these checks.

## Candidate and release status

| Stage | Status |
| --- | --- |
| Candidate version/SDK/OpenAPI/docs checks in OrbStack | PASS; Go 1.26.7, docs/SDK/cmd tests and whole-repository vet |
| Latest candidate main CI, all seven jobs | Pending |
| Immutable annotated v2.4.0 tag | Not created |
| Exact-tag unit/vet/race/SDK and runtime gates | Pending |
| Single-node and Raft thirty-minute soaks | Pending |
| Actual GitHub release and assets | Not published |
| Independent source/archive/binary/checksum verification | Pending |
| Downloaded Linux ARM64 binary, fresh-directory replay and backup restore | Pending |

Required release gates retain their original workloads, deadlines and failure
standards: direct/WAL/recovery, S3 backup, production gateway, real released
v2.3.0 rolling, enhanced snapshots/maintenance/recovery, protocol-3 GC and both
thirty-minute soaks. The release archive includes their evidence and exact binary
identity. Final independent verification is recorded on main after publication;
the record commit's own CI is checked separately without replacing the release.

Candidate checks also passed workspace hygiene, version freeze, Python SDK
(13 tests, one optional live-server skip), backup automation (11) and rolling
barrier regressions (5). The production storage/replication code is unchanged
from the source review linked above; only SDK version constants and release
documentation change for candidate preparation.

The three pre-existing user documents are preserved outside release commits.
Earlier tags and release assets, including v2.3.8 and v2.3.10, remain unchanged;
v2.3.9 remains an unsuccessful qualification tag without release assets.
