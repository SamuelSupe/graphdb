# GGraphDB 2.x release checklist

Complete this checklist against the exact candidate binary and retained evidence.
A passing short test does not establish a performance claim. This is the default
checklist, not a record that every published version passed every item.

## 2.7.2 qualification

The [release notes](../release/local-disk.md) describe query correctness and
response integrity fixes. Check sorted path pagination with a path limit,
neighbor grouping and aggregation over edge fields, and final NDJSON metadata
for partitioned graph queries.

For tenant sharding, compare direct Router traffic and traffic through HAProxy.
Exercise fresh and reused connections, including while indexes rebuild. Read
each JSON body to completion and check every NDJSON stream for a final
`done: true` record. Validate result order, aggregate values, and pagination
cursors.

The [tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.2)
records the exact-tag gates, native Linux archive checks, and publication.

## 2.7.1 qualification

The [release notes](https://github.com/SamuelSupe/graphdb/blob/v2.7.1/release/local-disk.md)
describe the index health fix.
Regression tests inject a reverse catalog write failure during a full rebuild
and an incremental update. Both paths must leave the current graph version
unindexed until the reverse catalog is durable.

The [tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.1)
records the exact-tag gates, the native Linux archive checks and publication.

## 2.7.0 qualification

The [release notes](https://github.com/SamuelSupe/graphdb/blob/v2.7.0/release/local-disk.md) cover upgrades from 2.6.5, including
storage/WAL/backup/query-IR convergence, Raft ReadIndex, inline snapshot
transfer, retained Raft log-tail indexing, Raft protocol 1–14 support, sharded
copy-on-write adjacency/partition maps, and multi-round stability and
performance hardening across all packages.

Pre-tag local verification passed workspace hygiene, release-freeze contract
checks, `go vet -mod=readonly ./...`, full `-race -count=1` unit and regression
suites across all 18 Go packages, management console boundary tests, Python SDK
tests, and automation script unit tests. The
[tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.7.0)
runs the static and integration gates, object backup gate, gateway gate,
30-minute local-disk soak, Raft gate, native Linux AMD64/ARM64 archive
verification, and automated GitHub Release publication.

## 2.6.5 qualification

The [release notes](https://github.com/SamuelSupe/graphdb/blob/v2.6.5/release/local-disk.md) cover upgrades from 2.6.2, including
partition GC recovery and local GC cursor progress during concurrent writes.
The [tag workflow](https://github.com/SamuelSupe/graphdb/actions/runs/45300753426)
passed standalone disk and WAL recovery, object backup and restore, and gateway
checks. This release was packaged directly from tag commit `17f86dd`. The
long-running load and Raft checks were waived. The full static job was still
running when the workflow was stopped. The full archive includes the completed
gateway and backup evidence and a record of the release scope.

Both Linux archives passed SHA-256 verification. The extracted binaries reported
`v2.6.5`, commit `17f86dd` and Go 1.27.3 on native AMD64 and ARM64 Linux.
The packaged AMD64 binary also matched the digest recorded by the GitHub gateway job.

## 2.6.4 qualification

The [2.6.4 candidate run](https://github.com/SamuelSupe/graphdb/actions/runs/45294821351)
reported a stalled GC cursor during the local-disk soak. The
[main CI run](https://github.com/SamuelSupe/graphdb/actions/runs/45294822075)
also found missing HELP metadata for the interrupted-stream counter.

## 2.6.2 qualification

The [2.6.2 release notes](https://github.com/SamuelSupe/graphdb/blob/v2.6.2/release/local-disk.md) record the interrupted NDJSON
regression and the three-node HAProxy checks. The
[tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.2)
runs the release gates, verifies the Linux AMD64 and ARM64 archives on native
runners, and packages the gate evidence.

## 2.6.1 qualification

The [2.6.1 release notes](https://github.com/SamuelSupe/graphdb/blob/v2.6.1/release/local-disk.md) describe the Raft read interruption
fix and three-node HAProxy regression. The
[tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.1)
runs the release gates and verifies the Linux AMD64 and ARM64 archives on native
runners. The full archive includes the gate evidence.

## 2.6.0 qualification

The [2.6.0 release notes](https://github.com/SamuelSupe/graphdb/blob/v2.6.0/release/local-disk.md) record the stability fixes and
sustained partitioned WAL comparison. The
[tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.6.0)
runs the full release gates and verifies the Linux AMD64 and ARM64 archives
on native runners. The full archive includes the collected gate evidence.

## Previous 2.5.1 qualification

See the [2.5.1 verification record](validation-v2.5.1.md) for the exact-tag gates, Linux archives and download checks.

## 2.6.0-preview.3 qualification

See the [WAL and GC comparison](performance-wal-gc-2026-11-18.zh-CN.md) for
queue rebuild costs and the measured protocol 6 commit P99 regression. The
tag workflow verifies the packaged candidate and protocol 12 recovery.

## 2.6.0-preview.2 qualification

The [preview validation](validation-v2.6.0-preview.2.md) records protocol 12
activation and cold-write measurements. The tag workflow requires the
protocol 11 to 12 upgrade and recovery report for the packaged AMD64 binary.
Both Linux archives must pass checksum, version and read/write checks on
their corresponding native runners before publication.

## Previous 2.6.0-preview.1 qualification

The [preview validation](validation-v2.6.0-preview.1.md) records the opt-in
protocols and measured cold-write limits. The tag workflow verifies protocol 11
partitioned WAL recovery against the packaged Linux AMD64 binary.

## Previous 2.5.0 qualification

See the [2.5.0 verification record](validation-v2.5.0.md) for the exact-tag gates, Linux archives and download checks.

## Previous 2.4.4 qualification

[2.4.4](validation-v2.4.4.md) passed every exact-tag gate, both thirty-minute soaks and independent downloaded-asset/runtime verification. No gate waiver; its tag and assets remain immutable.

## Previous 2.4.3 qualification

[2.4.3](validation-v2.4.3.md) passed all exact-tag gates and independent downloaded-asset/runtime checks. No gate waiver; its tag and assets remain immutable.

## Previous 2.4.2 qualification

[2.4.2](validation-v2.4.2.md) passed all exact-tag gates and independent downloaded-asset checks. Its tag and assets remain unchanged.

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
- [ ] Publish after all required gates pass. A stable tag uses `latest=true`.
  A SemVer prerelease tag uses `--prerelease --latest=false`.
- [ ] Verify remote main/tag, workflow conclusions and SDK/module versions; verify GitHub Pages when site content changes.
- [ ] Download published assets, verify both checksum layers and execute the matching binary.
