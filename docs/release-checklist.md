# GGraphDB 1.2 Release Checklist

## Contract And Compatibility

- [ ] `release/freeze-1.1.yaml` is present and
      `scripts/check_release_freeze.sh` passes.
- [ ] Public product naming is GGraphDB; GraphQL is served by
      `/v1/query/graphql`; legacy technical identifiers remain compatible.
- [ ] `CHANGELOG.md` and API/error documentation match the tag.
- [ ] `graphdb version` reports tag, commit, build date, and Go version.
- [ ] Go and Python SDK versions match the release.
- [ ] The v1.1.5 to v1.2.0 data upgrade is documented as one-way; no reverse
      writer compatibility is claimed.
- [ ] Object layout remains unchanged or has an approved one-way migration.

## Verification

- [ ] Unit, vet, race, Python SDK, and OpenAPI contract tests pass.
- [ ] RustFS e2e, load, restart, freshness, outage, repair, and restore drill pass.
- [ ] PostgreSQL coordinator and RustFS CAS integration tests pass.
- [ ] Five 30-minute v1.1.5 and five 30-minute v1.2.0 local-WAL runs complete
      on the fixed OrbStack host with 8 tenants and 16 collectors.
- [ ] Every v1.2.0 run commits at least 10,000 mutations/s; median throughput is
      at least 1.5x v1.1.5 and run spread is no more than 5%.
- [ ] Accepted p95/p99 are at most 20/50 ms and committed p95/p99 are at most
      8/15 seconds in every candidate run.
- [ ] Candidate RSS is at most 7 GiB and 110% of baseline; CPU per 1,000
      mutations is at most 75% of baseline.
- [ ] Direct-write and query benchmark regressions are at most 10%.
- [ ] 8-writer concurrency correctness gate passes with no loss or duplicate version.
- [ ] 2-active-writer, 20 commit/s, 30-minute CAS soak passes at 90% or better throughput.
- [ ] Soak finishes with mirror lag, legacy outbox, and derived-task backlog at zero.
- [ ] The tagged 1.0 binary reads the winning mirrored manifest after the soak.
- [ ] CAS soak JSON is schema 2, reports `success=true`, is bound to the
      release commit, and is included in the release archive.
- [ ] Formal rollback JSON is schema 1, reports `success=true`, is bound to the
      release commit, and is included in the release archive.
- [ ] Real-process WAL recovery evidence is schema 1, reports
      `kind=wal_metadata_segment_process_recovery` and `success=true`, is bound
      to the release commit, and proves `wal + metadata segment + local`
      configuration against RustFS.
- [ ] WAL evidence covers a durable `202` followed by SIGKILL/restart,
      idempotency replay without a new graph version, same-tenant FIFO and
      collector totals, plus a durable `202` accepted before a RustFS outage,
      pending/error WAL readiness at `503`, and recovery to `200`/committed.
- [ ] The WAL evidence JSON is archived as
      `release/evidence/wal-recovery-<tag>.json` and is a hard dependency of
      the GitHub release job.
- [ ] The commit-bound local-WAL performance matrix, regression report, and
      final gate JSON are archived and are a hard dependency of the release job.

## Security

- [ ] pprof is disabled or private on a separate admin listener.
- [ ] Gateway authenticates callers and overwrites `X-Tenant-ID`.
- [ ] Admin paths require operator/admin RBAC and TLS.
- [ ] Data/admin listeners, PostgreSQL, and object store are private.
- [ ] 1.0 writer routes and write credentials are revoked before PG bootstrap.

## Rollout

- [ ] PostgreSQL schema migration and coordinator backup are complete.
- [ ] Existing manifests pass integrity audit and bootstrap dry-run.
- [ ] Old writers are stopped before writing the coordination marker.
- [ ] Start one PG writer, verify mirror lag zero, then scale to 2–8.
- [ ] Route 1.0 readers only after mirror lag reaches zero.
- [ ] Rollback rehearsal confirms legacy manifest equals PG head.
- [ ] Rollback rehearsal report is bound to the release commit, fences stale
      PostgreSQL writers, removes the marker conditionally, and proves a local
      writer can advance the mirrored manifest.
- [ ] Rollback reports zero legacy mirror lag and outbox backlog before local
      writer traffic is restored.

## Artifact

- [ ] Release archive contains binaries, checksums, build metadata, license,
      changelog, SDKs, OpenAPI, Compose files, and operations docs.
- [ ] Extracted binaries run `graphdb version` on every target architecture.
- [ ] Container image digest and archive checksum are recorded.
