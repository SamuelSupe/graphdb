# Errors And Troubleshooting

[中文](errors-troubleshooting.zh-CN.md)

## Error Envelope

All non-2xx HTTP errors use:

```json
{
  "error": "message",
  "code": "stable_code",
  "message": "message",
  "retryable": false,
  "detail": {}
}
```

Use `code` for automation. `error` exists for backward compatibility.

Full contract: [../error_codes.md](../error_codes.md).

## Common Codes

| Code | Meaning | Operator action |
| --- | --- | --- |
| `tenant_required` | Missing `X-Tenant-ID` | Add tenant header. |
| `invalid_tenant` | Bad tenant id | Fix tenant id format. |
| `tenant_disabled` | Mutations blocked | Enable tenant or route to active tenant. |
| `tenant_deleted` | Soft-deleted tenant | Restore/clone or stop using tenant. |
| `operation_disabled` | Reader mode write attempt | Send write/config/task mutation to writer. |
| `reader_not_fresh` | Reader cannot reach required version | Retry later, lower `min_version`, or allow stale read. |
| `write_admission_queue_timeout` | Write queue full | Retry with same idempotency key and reduce concurrency. |
| `write_backpressure` | System pressure | Honor `Retry-After`; inspect `reasons`. |
| `commit_tail_too_long` | Too many visible commits | Run compact or wait for auto compact. |
| `index_rebuild_running` | Index rebuild blocks writes | Wait or cancel task if appropriate. |
| `quota_exceeded` | Tenant quota would be exceeded | Raise quota or delete data. |
| `idempotency_conflict` | Same key, different payload | Use a new key or resend exact original body. |
| `idempotency_in_progress` | Another writer owns the same key | Retry the exact request with the same key after backoff. |
| `write_conflict` | PG head changed through the retry budget | Retry with the same idempotency key; inspect CAS conflict rate if sustained. |
| `version_conflict` | `expected_version` no longer matches | Reload the head and resolve the caller's precondition; do not blind-retry. |
| `coordinator_unavailable` | PostgreSQL coordinator unavailable | Restore PG connectivity; writes never fall back to local mode. |
| `lease_held` | Duplicate/stale local writer protection | Ensure only one local-coordination writer per tenant. |
| `index_stale` | Index missing/stale | Rebuild indexes or allow fallback where supported. |
| `repair_required` | Integrity issue blocks operation | Run audit and repair. |

## 429 Handling

GGraphDB uses 429 for admission and backpressure. Clients should:

1. Read `Retry-After` and `retry_after_ms`.
2. Retry with the same `idempotency_key`.
3. Apply source-side concurrency backoff if repeated.
4. Alert if the same reason remains for multiple retry windows.

Example body:

```json
{
  "code": "write_backpressure",
  "retry_after_ms": 2000,
  "reasons": [
    {"code": "commit_tail_too_long", "current": 1501, "threshold": 1500}
  ]
}
```

## Reader Freshness Problems

Symptoms:

- `reader_not_fresh`
- `version_lag` in `/v1/control/reader-freshness`
- fleet readiness not ready

Checks:

```sh
curl -sS "$READER/v1/control/reader-freshness" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/control/reader-fleet-readiness?min_ready=1" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/control/reader-traffic-gate?min_ready=1" -H 'X-Tenant-ID: demo'
```

Actions:

- Confirm reader can reach object storage.
- Check `GRAPHDB_POLL_INTERVAL` and `GRAPHDB_READER_CATCHUP_TIMEOUT`.
- Use `allow_stale=true` only for workflows that tolerate stale reads.
- If a reader remains behind, remove it from traffic and restart it.

## Slow Or Expensive Queries

Checks:

```sh
curl -sS "$READER/v1/queries/running" -H 'X-Tenant-ID: demo'
```

Actions:

- Add `timeout_ms` and `cost_limit`.
- Add `kind`, relation type, and indexed filters.
- Use `EXPLAIN` or `PROFILE`.
- Kill bad in-process queries:

```sh
curl -sS -X DELETE "$READER/v1/queries/running/<query-id>" \
  -H 'X-Tenant-ID: demo'
```

## Index Problems

Checks:

```sh
curl -sS "$READER/v1/indexes/health" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/indexes" -H 'X-Tenant-ID: demo'
```

Actions:

- Run async rebuild from writer.
- Use `?deep=true` only for explicit validation.
- After rebuild, GC can clean orphan index objects when reader watermark allows.

## Integrity Problems

Checks:

```sh
curl -sS "$WRITER/v1/control/integrity-audit?deep=true" \
  -H 'X-Tenant-ID: demo'
```

Repair:

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":false}'
```

Apply only after reviewing the planned actions:

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":true}'
```

## Logs And Metrics

Metrics:

```sh
curl -sS "$BASE/metrics"
```

Useful signals:

- write backpressure total by reason.
- write admission queue latency.
- object store operation latency and errors.
- manifest CAS conflicts.
- commit tail length.
- reader visible version and lag.
- slow query logs and query profile operator timings.

Logs are JSON lines on stdout. Search by `tenant_id`, `event`, `query_id`,
`task_id`, and `reason`.
