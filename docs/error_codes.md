# Error Code Contract

All non-2xx HTTP errors use this envelope:

```json
{"error":"message","code":"stable_code","message":"message","retryable":false}
```

`error` is kept for backward compatibility. New clients should use `code`,
`message`, `retryable`, and optional `detail`.

The following top-level `code` values are stable:

| Code | Typical status | Retryable | Meaning |
| --- | --- | --- | --- |
| `bad_request` | 400 | no | Generic malformed request or invalid parameter. |
| `invalid_tenant` | 400 | no | Tenant ID format is invalid. |
| `tenant_required` | 400 | no | `X-Tenant-ID` is missing. |
| `tenant_disabled` | 403 | no | Tenant is disabled. |
| `tenant_deleted` | 410 | no | Tenant is soft-deleted. |
| `operation_disabled` | 405 | no | Operation is disabled in the current mode. |
| `not_found` | 404 | no | Requested resource does not exist. |
| `method_not_allowed` | 405 | no | HTTP method is not supported for the route. |
| `request_too_large` | 413 | no | Request body exceeds the configured limit. |
| `too_many_requests` | 429 | yes | Generic admission/backpressure rejection. |
| `internal_error` | 500 | no | Unclassified server-side error. |
| `invalid_json` | 400 | no | Request body is not a single valid JSON document. |
| `invalid_query` | 400 | no | Query DSL is invalid. |
| `query_limit_exceeded` | 429 | yes | Query admission or cost limit was exceeded. |
| `index_stale` | 400 | no | Persisted index is missing, stale, or unavailable. |
| `reader_not_fresh` | 503 | yes | Reader or reader fleet is behind the required version. |
| `quota_exceeded` | 429 | no | Tenant entity or edge quota would be exceeded. |
| `lease_held` | 409 | yes | Writer lease is held by a different process, usually an accidental duplicate or stale writer. |
| `manifest_cas_conflict` | 409 | yes | Manifest CAS publish failed; retry may succeed. |
| `object_write_conflict` | 409 | yes | Object conditional write conflict. |
| `object_store_unavailable` | 503 | yes | Object store is unavailable or timing out. |
| `ingest_wal_unavailable` | 503 | yes | The local ingest WAL writer was fenced after fatal I/O; preserve the WAL, repair the underlying storage, and restart before retrying. Durable accepted records remain recoverable and no new LSN is assigned while fenced. |
| `task_conflict` | 409 | no | Task state does not allow the requested operation. |
| `repair_required` | 409 | no | Operation requires repair before it can proceed. |
| `version_conflict` | 409 | no | Expected version precondition failed. |
| `idempotency_conflict` | 409 | no | Idempotency key belongs to a different request. |
| `idempotency_in_progress` | 409 | yes | Another writer is still processing the same idempotency key. |
| `write_conflict` | 409 | yes | The tenant head changed until the optimistic retry budget was exhausted. |
| `coordinator_unavailable` | 503 | yes | The configured external write coordinator is unavailable. |
| `commit_tail_too_long` | 429 | yes | Commit tail is above the write threshold. |
| `index_rebuild_running` | 429 | yes | Index rebuild is running for this tenant. |
| `maintenance_task_running` | 429 | yes | Maintenance work is blocking ordinary writes. |
| `write_admission_queue_timeout` | 429 | yes | Write admission queue timed out. |
| `write_backpressure` | 429 | yes | Generic write backpressure category. |
| `request_timeout` | 504 | yes | Request execution timed out. |
| `request_canceled` | 499 | no | Client or caller canceled the request. |

Write backpressure responses may include `reasons[]` with more specific reason
codes. Those reason codes are intentionally scoped to the backpressure detail
payload and do not replace the top-level contract above.
