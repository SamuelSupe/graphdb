# GGraphDB v2.5.0 validation

The [v2.5.0 tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.5.0) records exact-tag qualification. Publication requires unit, vet, race, SDK, standalone recovery, S3 backup, gateway and Raft gates, including 30-minute standalone and Raft soaks. The full archive ships runtime gate evidence under `release/evidence/` and binds its Linux AMD64 binary to the Raft and gateway qualification digests.

Both Linux platform archives carry a SHA-256 sidecar and build metadata. Packaging checks compare the executable and metadata against the full archive. See [release deployment](user/release-deployment.md) for download and verification steps.

## Published release

[GGraphDB v2.5.0](https://github.com/SamuelSupe/graphdb/releases/tag/v2.5.0) was published on 2026-11-09 from commit `5d654cb2b3f111084e8c835d3d48c1955b916073`. The [exact-tag run](https://github.com/SamuelSupe/graphdb/actions/runs/42627507131) passed all gates, including both 30-minute soaks and the package-to-gate binary digest checks.

Both Linux archives were independently downloaded and checked against their SHA-256 sidecars and the GitHub asset digests. The AMD64 and ARM64 executables matched their ELF architecture, reported the tagged commit and Go 1.27.3, and reached `/v1/readiness` on OrbStack Linux.

An upgrade probe from the published v2.4.4 ARM64 binary exercised standalone direct and WAL ingestion on the same data directories. In both modes v2.5.0 preserved batch status and the graph snapshot on identical retries. The v2.4.4 binary rejected the upgraded directory.

## Development fault testing

Five QEMU/Linux hard power-cut cases passed on the receipt durability fix at `6eb9c60`. Each case stopped the VM at the persistence boundary and restarted against the same disk.

| Interruption point | Recovery check |
| --- | --- |
| Concurrent reuse of an immutable graph file with a pending directory sync | The durable receipt still resolved to the published graph |
| First receipt database creation, with an empty or initialized staging file | Both interruption points recovered and completed the first batch |
| Durable WAL acceptance before graph publication | Restart replayed the accepted batch |
| Durable receipt before the terminal WAL frame | Restart finished the batch and drained pending WAL |

All five cases recovered one entity at graph version 1, drained pending WAL, returned the original result on identical retry and rejected a conflicting payload with HTTP 409. These were development fault-injection runs on a virtual disk. Protocol 5 large-graph partitioning is opt-in preview. The release gates do not qualify protocol 5 rolling upgrades, cross-host disaster recovery, day-scale stability or production capacity.
