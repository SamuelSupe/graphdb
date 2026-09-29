# GGraphDB 2.1.2 validation / 验证记录

Date: 2026-09-29. The annotated `v2.1.2` tag identifies the release; archives
record its exact commit in `BUILD-METADATA.json` and binary `version` output.

## Scope

GC checks active read views before opening orphan index files. Files that become
eligible still require content/tenant validation, and deletion rechecks read-view
protection. Retirement epochs survive preflight checks until actual file changes.
The existing regression also covers same-version catalog changes, eventual
reclamation with newer readers, and validation before deletion. The new assertion
fails on the old implementation because GC opens a protected orphan.

No data format, HTTP contract, WAL semantics, memory budget or durability default
changes. The release synchronizes Go/Python SDK and OpenAPI versions to 2.1.2.

## Local evidence

Validation runs in OrbStack, Linux/arm64, Go 1.26.7, on container-local storage.
Clean Git source snapshots include intended changes and exclude four unrelated
pre-existing untracked files. Their original hashes remain unchanged.

- Final release source full Go unit tests and vet: PASS.
- Final GC/read-view, same-version catalog, checkpoint/delete-budget regressions
  and focused race checks: PASS.
- Actual HTTP direct/WAL write/query/export/backup/restore/restart and SDK gate:
  PASS.
- Restart of the measured data directory: all 80 batches committed at 80 unique
  versions; manifest/catalog version 393. Deep integrity audit: 1,747 objects,
  zero issues.
- Exploratory candidate full Go tests/vet and focused storage race checks: PASS.
  Parallel index-encoding changes were subsequently reverted. These exploratory
  checks alone do not qualify the final release.

Raw optimization evidence remains in `.workflow/write-tail/`. Release checks are
retained in `.workflow/release-v2.1.2/`.

## Publication gate

The exact tag passed GitHub unit/vet/race, Python SDK and version-contract
checks, actual direct/WAL HTTP and restart flows, S3-compatible backup/restore,
and main-branch container startup/restart checks. The full OrbStack static gate
also passed, including race and SDK checks.

**The 30-minute mixed workload was stopped at the user's explicit request. It is
NOT COMPLETED and is not counted as a passing endurance test.** The tag workflow
was cancelled after its static, HTTP and S3 jobs succeeded. Publication uses the
same immutable tag source with archive/binary checksums, build identity and a
container-local binary startup check. Default future release workflows retain
their existing soak requirement.

The archive contains direct/WAL and S3 evidence under `release/evidence/`, plus
this explicit endurance-test waiver. `BUILD-METADATA.json` records the tagged
code commit and the later documentation commit separately. The performance
report's 180-second measurements do not replace the skipped endurance test.

## Performance and limits

See [write-tail measurements](performance-write-tail.md) for the complete method,
baseline variation and observed improvements. Writes still have second-scale
tails. Compaction duration showed an unresolved regression signal with few samples
and different maintenance overlap; the capacity envelope remains
`performance_unqualified`.

No new 100K comparison matrix, OS-cold-cache measurement, or real AWS IAM/Object
Lock/versioned-bucket acceptance was performed. S3 retention still requires an
installation-specific writable bucket/prefix. These checks do not prove absence
of all faults or protection from physical volume loss. Replication/HA and 1.x
migration remain outside this release.
