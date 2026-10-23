# GGraphDB 2.4.1 validation

Candidate preparation; **not yet published**. This record distinguishes the reviewed source, Go1.27.3 candidate checks, main CI, exact-tag release gates and independent downloaded-asset checks. No pending item below is a PASS.

- Required release compiler: **Go1.27.3** across CI, containers and all packaged binaries; verify actual build information after download.
- The prior local repair qualification used Go1.26.7: full unit/vet, complete storage/HA/HTTP race and the other packages, final GC integration, and native macOS filesystem boundaries passed. Those results are historical evidence and do not substitute for Go1.27.3 qualification.
- Go1.27.3 SDK/docs/CLI, release contracts and focused lost-response envelope regressions (3 repeats): **PASS**. Python SDK13 tests (one optional live skip), backup11 and rolling5 script regressions: **PASS**. Official LinuxARM64 SDK SHA256 was checked.
- Go1.27.3 full unit/vet/race and real runtime recovery: **PENDING**.
- Latest main seven-job CI: **PENDING**; require actual success at the candidate SHA before a new annotated tag.
- Exact-tag static/SDK, direct/WAL/recovery, S3/gateway, released v2.3.0 rolling, enhanced/protocol3, standalone/Raft each30-minute soak and packaging/publication: **PENDING**. All original deadlines, workloads and zero-error rules remain.
- Independent GitHub source/bundle/checksum/three-binary identities, packaged gate evidence and downloaded LinuxARM64 fresh-directory direct/WAL SIGKILL/replay/backup/restore: **PENDING**.

## Compatibility and limits

WAL v1 durable markers migrate to checksummed two-slot v2. Old binaries cannot read the new marker: drain WAL before downgrade, keep verified pre-upgrade backups and preserve the whole WAL directory. The index downgrade procedure does not qualify a WAL/Raft directory downgrade.

Range/adjacency layout flags remain off by default. Upgrade all relevant readers/writers/GC/replicas before explicit activation/rebuild. Legacy layouts and global `ListEdges` cursor contracts remain supported. Streaming snapshots are now enabled by default with capability negotiation/legacy fallback; explicit compatibility mode still has inline encoding costs. Unknown committed write outcomes require identity-based resolution rather than blind replay.

Checkpoint construction still scans WAL and serializes append work; prepared queue accounting is not a process RSS bound. Current catalog checks remain fresh per GC batch while retained history is marked once per unchanged root generation. Large result sets still own their result data; no production-capacity, cross-host or day-scale claim is made.

Existing tags and assets are immutable. Final machine-readable status: [JSON](validation-v2.4.1.json).
