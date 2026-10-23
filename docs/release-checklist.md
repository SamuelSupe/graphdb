# GGraphDB 2.x release checklist

Complete this checklist against the exact candidate binary and retained evidence.
A passing short test does not establish a performance claim. This is the default
checklist, not a record that every published version passed every item.

## Current candidate qualification

[2.4.1](validation-v2.4.1.md) records local qualification; exact-tag CI and independent
downloaded-asset verification must complete before qualification. No gate waiver.

## Previous 2.4.0 qualification

[2.4.0](validation-v2.4.0.md) passed all exact-tag gates and independent downloaded-asset checks. Its tag and assets remain unchanged.

## Previous 2.3.10 qualification

[2.3.10](validation-v2.3.10.md) passed all exact-tag gates, both thirty-minute soaks and independent downloaded-asset/runtime checks. Its immutable tag and assets remain unchanged.

## Unpublished 2.3.9 qualification

The [2.3.9 tag](validation-v2.3.9.md) did not pass all required gates and produced no release assets. Its tag remains unchanged.

## Previous 2.3.8 qualification

[2.3.8](validation-v2.3.8.md) passed all required exact-tag jobs, both thirty-minute
soaks and independent downloaded-asset/runtime/fault probes. No gate waiver.

## Previous 2.3.6 qualification

[2.3.6](validation-v2.3.6.md) passed every exact-tag gate, both thirty-minute
soaks and independent downloaded-asset/runtime checks, without a waiver. Matched
C3/C7 performance replay is separate from official release qualification; stale
frequency and seconds-long tails remain explicit limitations.
The [v2.3.5 failed tag](validation-v2.3.5.md) remains unreleased.

## Previous 2.3.4 qualification

[2.3.4](validation-v2.3.4.md) passed every exact-tag gate, both thirty-minute
soaks and independent downloaded-asset/runtime checks, without a waiver. The failed
2.3.2/2.3.3 tags and their limits remain recorded; production latency is unqualified.

## Previous 2.3.1 qualification

[2.3.1](validation-v2.3.1.md) passed all exact-tag gates, both thirty-minute
soaks, released-v2.3.0 rolling and independent downloaded-asset checks without
a waiver. Seconds-long standalone/Raft tails remain explicitly recorded; this
qualification does not establish production latency or capacity.

## Previous release qualification

[2.3.0](validation-v2.3.0.md) passed every exact-tag gate and downloaded-asset
verification with no waiver. The immutable tag and packaged evidence identify the
qualified binary; later documentation commits do not rebuild or replace it.

## Historical 2.1.2 exception

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
- [ ] The local Parquet/WAL format introduced in 2.0, `data_hash` and matching SDKs pass; existing 2.0/2.1/2.2 directory reuse is documented.
- [ ] `expected_version`, `min_version`, cursors, idempotency, and WAL state contracts pass.
- [ ] File/parent-directory sync and publication failures preserve a readable committed head.
- [ ] Restore/delete wait for active read views; GC defers pinned files without excluding new reads, including shared loads after HTTP cancellation.

## Verification

- [ ] Full unit/integration, vet, race, binary identity, and SDK gates pass.
- [ ] Real HTTP direct/WAL ingest, queries, export, backup, restore, and restart pass.
- [ ] The real TLS gateway rejects ordinary and encoded writes from readers, preserves authenticated tenant headers, and separates admin routes (`scripts/gateway_gate.sh`).
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
  The large-restore fixture allows 120 seconds for each 4 MiB setup commit; ordinary and isolation probes retain their five-second client budget. Rolling gates observe gateway readiness after transfer before starting a non-idempotent operation; background requests keep their 25-second identity-preserving retry budget.
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
