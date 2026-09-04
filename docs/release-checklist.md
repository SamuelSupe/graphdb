# GGraphDB 1.3.2 Release Checklist

The checklist is intentionally unchecked until commit-bound evidence is
available. The 1.3 PostgreSQL-CAS multi-writer WAL profile is release-gated;
the historical 1.2 gates below remain compatibility inputs, not proof of 1.3.
The v1.3.2 patch preserves the existing WAL record format. Local verification
summaries remain separate from the release workflow gates below.

## 1.3 Contract And Durability

- [ ] `GRAPHDB_COORDINATION=postgres`, `GRAPHDB_WRITER_TOPOLOGY=cas`, and
      `GRAPHDB_INGEST_MODE=wal` accept a unique stable `GRAPHDB_INSTANCE_ID`.
- [ ] Every writer uses an independent persistent WAL volume; no WAL directory
      is shared between writers.
- [ ] `POST /v1/ingest/batches` returns `202` only after local WAL `fsync`, and
      the response contains `writer_id` plus an owner-routed `status_url`.
- [ ] PostgreSQL schema v5 stores coordination metadata/head CAS only; payload,
      WAL records, commit segments, and graph data stay in object storage.
- [ ] PostgreSQL outage permits bounded local durable admission, then rejects
      before another WAL payload when the high-water policy is reached.
- [ ] CAS and temporary dependency failures rebase, back off, and shrink the
      batch without terminally failing an accepted request.
- [ ] Permanent semantic and lifecycle fencing errors are visible as final
      batch failures; lifecycle fencing wins over unpublished WAL.
- [ ] WAL recovery reports `recovery_pending` through the owner route and
      recovers with the original volume and stable writer identity.
- [ ] Ingest accepts `expected_version`, `failure_mode` (`best_effort` or
      `atomic`), and bounded entity/edge `preconditions`; terminal results expose
      the corresponding `error_code` when a request is rejected.
- [ ] Go and Python SDKs are version `1.3.2` and preserve direct `200/207`
      behavior while exposing WAL `202` acceptance, `Location`/owner status,
      and explicit poll/wait helpers without hiding HTTP status.
- [ ] GraphQL, OpenAPI, and user-facing documentation expose only the supported
      `graph` query root; unsupported retrieval extensions are not advertised.

## 1.3 Multi-Writer And Rolling Compatibility

- [ ] Same-tenant 2-, 4-, and 8-writer runs preserve request effects, versions,
      and cross-writer idempotency without loss or duplication.
- [ ] Cross-tenant load demonstrates the intended horizontal scaling boundary;
      no single-tenant linear-throughput claim is made.
- [ ] 1.2 direct writers and 1.3 WAL writers coexist through schema v5 and the
      existing object layout.
- [ ] A WAL writer is not downgraded until new admission is stopped and its
      pending WAL is finalized.
- [ ] Permanent loss of a writer's WAL volume is documented as outside the
      durability guarantee.

## Historical 1.2 Contract And Compatibility Inputs

- [ ] `release/freeze-1.1.yaml` is present and
      `scripts/check_release_freeze.sh` passes.
- [ ] Public product naming is GGraphDB; GraphQL is served by
      `/v1/query/graphql`; legacy technical identifiers remain compatible.
- [ ] `CHANGELOG.md` and API/error documentation match the tag.
- [ ] `graphdb version` reports tag, commit, build date, and Go version.
- [ ] Go and Python SDK versions match the historical compatibility release.
- [ ] `scripts/compatibility_v1_0_v1_1.sh` passes both binary directions.
- [ ] Object layout versions remain unchanged or have an approved migration.

## Historical 1.2 Verification Inputs

- [ ] Unit, vet, race, Python SDK, and OpenAPI contract tests pass.
- [ ] RustFS e2e, load, restart, freshness, outage, repair, and restore drill pass.
- [ ] PostgreSQL coordinator and RustFS CAS integration tests pass.
- [ ] Local WAL recovery, idempotency, FIFO, readiness, and shutdown tests pass.
- [ ] 8-writer concurrency correctness gate passes with no loss or duplicate version.
- [ ] 2-active-writer, 20 commit/s, 30-minute CAS soak passes at 90% or better throughput.
- [ ] Soak finishes with mirror lag, legacy outbox, and derived-task backlog at zero.
- [ ] The tagged 1.0 binary reads the winning mirrored manifest after the soak.
- [ ] CAS soak JSON is schema 2, reports `success=true`, is bound to the
      release commit, and is included in the release archive.
- [ ] Formal rollback JSON is schema 1, reports `success=true`, is bound to the
      release commit, and is included in the release archive.

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
