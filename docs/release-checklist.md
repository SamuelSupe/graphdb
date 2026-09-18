# Local disk release checklist

Complete this checklist against the exact candidate binary and retained evidence.
A passing short test does not establish a performance claim.

## Runtime and compatibility

- [ ] Only local storage, local coordination, and `all` mode are accepted.
- [ ] The data directory is exclusively locked; clean exit and process death permit reopening.
- [ ] PostgreSQL-marked data is rejected without automatic takeover.
- [ ] Existing Parquet/WAL formats and API/SDK compatibility checks pass.
- [ ] `expected_version`, `min_version`, cursors, idempotency, and WAL state contracts pass.
- [ ] File/parent-directory sync and publication failures preserve a readable committed head.
- [ ] Restore/delete/GC wait for active read views, including shared loads after HTTP cancellation.

## Verification

- [ ] Full unit/integration, vet, race, binary compatibility, and SDK gates pass.
- [ ] Real HTTP direct/WAL ingest, queries, export, backup, restore, and restart pass.
- [ ] Optional S3 snapshot gate passes multipart publication/cancellation, corrupt backup rejection, retry after reopen, and restore on an empty local directory.
- [ ] Thirty-minute mixed workload with compact/GC/index rebuild passes.
- [ ] Focused performance reports state fixed inputs, warmup/measurement durations, repetitions, latency, published throughput, resources, and page-cache conditions; omitted large comparisons are explicit.
- [ ] Targets in `release/capacity-envelope.yaml` pass, or inconclusive results and regressions are explicitly reported without a performance claim.

## Deployment and artifact

- [ ] Compose starts GraphDB with a persistent local volume and no external storage service.
- [ ] The service account exclusively controls the data directory.
- [ ] Gateway authentication overwrites `X-Tenant-ID`; admin endpoints and pprof are private.
- [ ] English and Chinese instructions match the supported configuration.
- [ ] Binary checksums, source revision/diff, image digest, and verification reports are retained.
- [ ] The release archive contains matching binaries, SDKs, OpenAPI, deployment examples, and evidence.

## Independent local release

- [ ] Publish only `codex/local-disk-v2` and its new `v*-local.*` tag to GitHub.
- [ ] Mark the Release as a prerelease with `latest=false`; preserve the previous Latest.
- [ ] Verify the default branch, `main` commit and existing stable tags are unchanged.
- [ ] Download published assets, verify both checksum layers and execute the matching binary.
