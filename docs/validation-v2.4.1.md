# GGraphDB 2.4.1 validation

**Release qualification failed; no release assets were published.** The public annotated tag is retained unchanged.

- Tag object: `8877f66e27ccf7d8bd2a12b662e8f8dba44de359`; source `ff0592897a138f963d98eb9986f2b706c115627b`.
- Go1.27.3 OrbStack full unit/vet/race, direct/WAL SIGKILL/replay/fresh-directory backup recovery and the local Raft/sharding gate: **PASS**. [Candidate main CI](https://github.com/SamuelSupe/graphdb/actions/runs/39964701917): all seven jobs **PASS**.
- [Exact-tag release CI](https://github.com/SamuelSupe/graphdb/actions/runs/39969581663): **FAIL**. Static/SDK, direct/WAL, S3, gateway and standalone thirty-minute soak passed. Raft's protocol3 stage completed the thirty-minute workload but recorded **23 query errors** (EOF/502/503); packaging/publication was skipped.
- The long quiet interval was the original thirty-minute three-replica WAL soak, whose output was captured in `soak.log`. It was not an indefinite runner wait.
- Independent release-asset download and downloaded-binary recovery: **NOT RUN**, because there are no release assets.

The failure is retained as a release blocker. Follow-up source repairs use [2.4.2 qualification](validation-v2.4.2.md); no tag is moved and no existing asset is replaced. Original deadlines, workloads, accepted backpressure rules and zero-query-error requirements remain unchanged.

Range/adjacency flags remain off by default, with legacy layouts/global ListEdges cursor compatibility retained. WAL v1 markers migrate to two-slot v2; old binaries cannot read v2. Drain WAL before downgrade and preserve the entire WAL directory and verified pre-upgrade backups. See [machine-readable status](validation-v2.4.1.json).
