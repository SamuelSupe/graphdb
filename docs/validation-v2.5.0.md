# GGraphDB v2.5.0 validation

The [v2.5.0 tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.5.0) records exact-tag qualification. Publication requires unit, vet, race, SDK, standalone recovery, S3 backup, gateway and Raft gates, including 30-minute standalone and Raft soaks. The full archive ships runtime gate evidence under `release/evidence/` and binds its Linux AMD64 binary to the Raft and gateway qualification digests.

Both Linux platform archives carry a SHA-256 sidecar and build metadata. Packaging checks compare the executable and metadata against the full archive. See [release deployment](user/release-deployment.md) for download and verification steps.

## Development fault testing

Five QEMU/Linux hard power-cut cases passed on the receipt durability fix at `6eb9c60`. Each case stopped the VM at the persistence boundary and restarted against the same disk.

| Interruption point | Recovery check |
| --- | --- |
| Concurrent reuse of an immutable graph file with a pending directory sync | The durable receipt still resolved to the published graph |
| First receipt database creation, with an empty or initialized staging file | Both interruption points recovered and completed the first batch |
| Durable WAL acceptance before graph publication | Restart replayed the accepted batch |
| Durable receipt before the terminal WAL frame | Restart finished the batch and drained pending WAL |

All five cases recovered one entity at graph version 1, drained pending WAL, returned the original result on identical retry and rejected a conflicting payload with HTTP 409. These were development fault-injection runs on a virtual disk. Protocol 5 large-graph partitioning is opt-in preview. The release gates do not qualify protocol 5 rolling upgrades, cross-host disaster recovery, day-scale stability or production capacity.
