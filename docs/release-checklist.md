# GGraphDB 2.x release checklist

Complete this checklist against the exact candidate binary and retained evidence.
A passing short test does not establish a performance claim. This is the default
checklist, not a record that every published version passed every item.

## Current documented exception

For [2.1.2](validation-v2.1.2.md), the release owner explicitly requested stopping
the 30-minute workload and publishing after the other checks. That run is
**not completed**, not passed. The tag workflow was cancelled after its static,
HTTP and S3 jobs succeeded; the archive was built and verified separately.
The default workflow still requires the soak. Record any release-specific waiver,
actual job conclusions and missing validation in its notes and packaged report;
never convert a skipped/cancelled check to PASS.

The 2.1.2 capacity envelope remains `performance_unqualified`.

## Runtime and compatibility

- [ ] Standalone accepts local storage, local coordination, and `all`; complete Raft configuration enables the separate replicated path.
- [ ] The data directory is exclusively locked; clean exit and process death permit reopening.
- [ ] PostgreSQL-marked data is rejected without automatic takeover.
- [ ] The local Parquet/WAL format introduced in 2.0, `data_hash` and matching SDKs pass; existing 2.0/2.1 directory reuse is documented.
- [ ] `expected_version`, `min_version`, cursors, idempotency, and WAL state contracts pass.
- [ ] File/parent-directory sync and publication failures preserve a readable committed head.
- [ ] Restore/delete wait for active read views; GC defers pinned files without excluding new reads, including shared loads after HTTP cancellation.

## Verification

- [ ] Full unit/integration, vet, race, binary identity, and SDK gates pass.
- [ ] Real HTTP direct/WAL ingest, queries, export, backup, restore, and restart pass.
- [ ] S3 snapshot gate passes multipart publication/cancellation, corrupt backup rejection, retry after reopen, and restore on an empty local directory.
- [ ] Thirty-minute mixed workload with compact/GC/index rebuild passes, or its release-specific waiver and NOT COMPLETED status are explicitly recorded.
- [ ] Focused performance reports state fixed inputs, warmup/measurement durations, repetitions, latency, published throughput, resources, and page-cache conditions; omitted large comparisons are explicit.
- [ ] Targets in `release/capacity-envelope.yaml` pass, or inconclusive results and regressions are explicitly reported without a performance claim.

## Deployment and artifact

- [ ] Compose starts GraphDB with a persistent local volume and no external storage service.
- [ ] The service account exclusively controls the data directory.
- [ ] Gateway authentication overwrites `X-Tenant-ID`; admin endpoints and pprof are private.
- [ ] English and Chinese instructions match the supported configuration.
- [ ] Binary checksums, source revision/diff, and verification reports are retained. Record the image digest when distributing an image; record a separate documentation revision when applicable.
- [ ] The release archive contains matching binaries, SDKs, OpenAPI, deployment examples, and evidence.

## Optional Raft and sharding release

- [ ] The candidate passes `scripts/raft_gate.sh` using real containers; retain the image ID, binary SHA256, per-scenario results and logs.
- [ ] Standalone direct/WAL and Raft remain usable concurrently with independent directories.
- [ ] Live network isolation rejects reads/writes at the old leader while the majority continues; accepted writes survive takeover and reconnect.
- [ ] Large restore, replica replacement, shard addition, migration interruption/cancellation and old-directory role protection pass.
- [ ] Thirty-minute Raft WAL mixed workload with compact, GC and index rebuild passes; record its actual size and rate without a capacity claim.
- [ ] Three independent fault domains pass host-loss/network/recovery qualification, or record NOT RUN with the release boundary explicitly restricted.
- [ ] Qualified rolling windows, separate protocol activation, unsupported downgrade combinations, protected private transport, redundant entry points and backup recovery are documented and validated for the deployment.
- [ ] Include both Raft Compose examples, HAProxy configuration, gate fixtures and reports in the extracted release archive; validate their configuration there.

See [Raft release readiness](raft-release-readiness.zh-CN.md) for current gaps and
[operations](raft-operations.zh-CN.md) for the qualified upgrade boundary.

## Main release

- [ ] Fast-forward GitHub `main` to the verified standalone/Raft implementation; preserve historical tags.
- [ ] Publish a new annotated tag matching `VERSION` on that commit.
- [ ] Publish as a stable Release with `latest=true`, after gates pass or the release-specific exception is recorded as above.
- [ ] Verify remote main/tag, workflow conclusions and SDK/module versions; verify GitHub Pages when site content changes.
- [ ] Download published assets, verify both checksum layers and execute the matching binary.
