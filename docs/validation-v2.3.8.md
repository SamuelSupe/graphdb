# GGraphDB 2.3.8 validation

Status: **CANDIDATE — exact-tag qualification and publication pending**.
This report will be finalized on `main` after immutable-tag gates and independent
asset verification; that documentation update will not rebuild or move the tag.

## Scope

1. Bound background index failures and preserve ordered cancellation. Three total
   attempts for recognized transient errors; release old view/build resources
   between attempts and recapture the head. Permanent/exhausted failures leave
   stale indexes eligible for maintenance repair.
2. Separate WAL HTTP concurrency admission from graph publication pressure.
   Keep predecode bounding and durable batch replay during pressure.
3. Use remaining commit-tail capacity by splitting only unprepared groups; retain
   full-tail backpressure, prepared atomicity, order and idempotent recovery.

## Local regression evidence

Ubuntu arm64 via OrbStack, Go 1.25.0:

- Focused regressions and adjacent WAL recovery tests: five repetitions with
  `-race` passed for storage and HTTP API.
- The same regression assertions fail on v2.3.7 for canceled ordered waits,
  partial commit-tail capacity, and HTTP acceptance/replay under tail, CAS and
  object-store pressure. Persistent fault behavior was also reproduced before
  implementation; all new sustained-fault assertions pass after the fix.
- Full `go test ./...`, `go vet ./...` and `go test -race -timeout 20m ./...`
  passed. After final focused test changes, the affected regressions were rerun
  with race detection; retry/purge and fresh-head capture passed three repetitions.
- Local `scripts/release_gate.sh` integration passed: real direct/WAL HTTP,
  Python SDK against the server, concurrent load, restart/export equality and
  recovery. Static SDK run: 12 tests, one HTTP integration skipped without its
  endpoint; the real-server gate exercises that endpoint separately.
- Backup automation: 11 tests passed. Management-console Node tests: 12 passed.
  Freeze/hygiene checks passed. Native Chrome displayed the 2.3.8 site label.
- Required exact-tag GitHub jobs and downloaded-asset verification remain pending.

## Required exact-tag gates

Full unit/vet/race/contracts/SDK tests; local direct/WAL load, restart and restore;
real TLS gateway and MinIO backup; Raft base/enhanced/runtime/protocol-3/rolling
checks; standalone and Raft thirty-minute mixed workloads; qualified-binary
binding, three platform builds and checksummed archive. No waiver.

## Independent download checks

Pending release publication: outer and inner SHA256, API asset digest, build/tag
identity, packaged tracked source comparison, Linux arm64 and macOS arm64 real
write/restart/idempotency/runtime-backup/restore probes. Linux amd64 is exercised
by native GitHub release gates.

## Limits

Permanent storage faults still require repair. Background worker count remains
bounded during backoff, while views and build/memory reservations are released.
Failed indexing does not undo graph publication. A full commit tail still needs
compaction. This patch does not claim to eliminate index lag or seconds-long
latency tails. Large capacity, cross-host fault domains and day-scale stability
are not qualified by these tests.
