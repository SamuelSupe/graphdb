# GGraphDB 2.4.3 validation

Candidate record, 2026-10-29. Publication is **PENDING**; this document does not
certify its own future main/tag CI or downloaded assets. No gate waiver.

## Scope and local evidence

This patch includes inode admission/cleanup, protected prepared-WAL replay,
automatic standalone tenant metadata recovery, metadata-first initialization,
validated migration inputs and metadata-only create/secondary-copy repair.
It also includes the read-drain, forwarding and cached-router status repairs
already merged after the immutable v2.4.2 release.

OrbStack Linux ARM64 with Go 1.27.3 passed the full repository tests and vet on
the final implementation before the version/documentation update. Focused race
tests passed metadata recovery, pending replay, disabled/deleted state, purge
fences, initialization, ingest retry and migration boundaries. Baseline
regressions demonstrate duplicate decoding, primary replacement during backup
repair and unnecessary graph reads before missing-metadata rejection.

The same local metadata-candidate microbenchmark, three one-second repetitions
with GOMAXPROCS=4, changed its median from 3.913 ms / 2,068,871 B allocated per
operation to 1.948 ms / 1,044,528 B. This measures metadata reads and decoding,
not service throughput, peak RSS or production latency. Primary-inode retention
is covered separately by the backup-repair regression.

Local evidence remains under `.workflow/inode-analysis-20261029/` and
`.workflow/release-v2.4.3/`. These directories are not published as source.

## Required release qualification

| Stage | Status |
| --- | --- |
| Versioned candidate local static/integration checks | PENDING |
| Exact candidate main CI | PENDING |
| Exact annotated-tag release CI | PENDING |
| Standalone and Raft thirty-minute soaks | PENDING |
| Stable GitHub Release and archive checksums | PENDING |
| Independent downloaded source/binary/runtime verification | PENDING |

All original unit/vet/full-race/SDK, direct/WAL/restart, S3 backup, gateway,
released-v2.3.0 rolling, enhanced snapshot and protocol-3 gates remain required.
The tag workflow verifies packaged binary identity against its gate evidence.
Existing tags and assets remain immutable; failed historical qualification tags
are not repurposed.

## Upgrade and recovery boundaries

Standalone recovery is on for every tenant without an allowlist. A valid
pending/backup preserves the original state and fields. Without a valid copy,
only a valid manifest and no active purge fence permit an active fallback;
running/completed, unknown or corrupt purge state does not authorize revival.
Raft reads never repair local metadata independently of replication.

One stable recovery copy per tenant and a temporary pending record are added.
Complete pending recovery with this version before downgrade; old programs do
not honor that intent. The existing two-slot WAL-marker downgrade restriction
from 2.4.2 remains. Range and adjacency indexes remain off by default; upgrade
all participants before activation and rebuild legacy layouts before downgrade.

Inode admission reduces exhaustion risk but is not an atomic resource
reservation. A filesystem already exhausted may still require capacity recovery
or migration before replay can proceed. Automatic metadata recovery does not
reconstruct missing graph data, bypass prepared-result checks, or prove the
historical cause of missing metadata. Migration validation does not eliminate
staging-copy IO; non-local target IO failures still lack whole-copy rollback.

Cross-host recovery, production capacity/latency and day-scale stability remain
unqualified. See [operations](product-operations.zh-CN.md) for recovery and
[compatibility](naming-and-compatibility.md) for version boundaries.
