# GGraphDB 2.1.0 validation / 验证记录

Date: 2026-09-28. Candidate identity is the annotated `v2.1.0` tag; published
archives record the exact commit in `BUILD-METADATA.json` and binary `version`.

## Changes under review

- Ordinary/index tasks share admission, persistence and shutdown ownership.
- Direct/WAL publication and query/cache view selection share storage-owned paths.
- GC defers files retained by older readers; new readers remain admitted.
- S3 automation adds persistent scheduling/retries, verification/drills, bounded
  retention and recovery/status/reset APIs. It remains disabled by default.
- Policy disablement still observes existing task outcomes and reclaims successful
  captures; schedule persistence honors tenant generations.
- File publication locates affected read views by path prefixes instead of scanning
  all active tenants. Retention stops further pagination after 100 deletions.

## Checks

Linux validation uses OrbStack/arm64, Go 1.26.7 and a dedicated RustFS S3-compatible
repository on container-local storage. The local snapshot excludes four unrelated
pre-existing untracked files, including two Finder-style Go duplicates; none are
part of the release. No new production dependency was introduced.

- Full local unit/vet/race and Python SDK gate: PASS (`RELEASE_GATE_VERIFY_ONLY=1 scripts/release_gate.sh`).
- Real HTTP direct/WAL ingest/query/export/restore/restart and SDK gate: PASS.
- S3 multipart, publication failures, cancellation, corruption, retry, retention,
  restore drills, and fresh-directory HTTP restore gate: PASS.
- Updated disable-during-backup HTTP flow: PASS; successful status survives restart.
- Tenant-generation fencing, GC/read-view and maintenance regressions: PASS.
- 205-backup retention regression: deletes 100/100/2 per cycle, keeps the newest
  three with equal timestamps, and makes no additional list requests after the
  deletion budget. The previous implementation made four additional requests.
- OpenAPI: 402 internal references resolved; Compose backup overlay validated.

The tag workflow independently repeats static and HTTP/S3 gates and runs the
required 30-minute mixed workload with compaction, GC and index rebuilding before
publishing assets. A release exists only after those jobs and artifact checks
succeed. Raw CI evidence is included under `release/evidence/` in the archive;
local evidence is retained in `.workflow/release-v2.1.0/`.

## Focused cost measurement

`BenchmarkLocalViewInvalidation`, Linux/arm64, 200 ms per sample, three samples
per implementation, same container and toolchain. These in-memory samples ran
alongside the final local race phase; they are a diagnostic microbenchmark, not
an isolated service benchmark. Medians:

| Active tenant views | Previous scan | Prefix lookup | Allocations |
| --- | ---: | ---: | ---: |
| 1 | 30.92 ns/op | 44.36 ns/op | 0 in both |
| 1000 | 7794 ns/op | 38.09 ns/op | 0 in both |

The 1000-tenant path removes a scan under the shared runtime lock. The single-tenant
path costs about 13 ns more. No conclusion about endpoint latency follows from
these figures. The benchmark remains in `internal/storage/index_cleanup_test.go`;
raw samples are retained with local release evidence.

## Limits

No new whole-database throughput, tail-latency or RSS improvement claim is made.
The earlier [2.0 measurements](performance-v2.0.md) are historical and do not
qualify this candidate. Function-level work reductions do not establish a full
workload speedup. This round does not repeat the 10K/100K comparison matrix.

No real AWS IAM/Object Lock/versioned-bucket acceptance or cross-instance cleanup
was performed. Automated retention requires an installation-specific writable
bucket/prefix. Local tests and the release soak do not prove absence of all faults
or protection from physical volume loss. Existing 2.0 data formats remain supported;
there is no 1.x migration or new replication/HA implementation.
