# Write And Ingest

[中文](write-ingest.zh-CN.md)

GGraphDB has three write paths:

- `POST /v1/commits`: direct atomic graph mutation.
- `POST /v1/ingest/batches`: collector-oriented batch ingestion with source,
  cursor, idempotency, partial failure, dead-letter, and collector status.
- `POST /v1/imports`: asynchronous CSV or JSONL bulk import built on ingestion
  batches, with task checkpoints and resumability.

All three require `X-Tenant-ID`. In reader mode they return `405`.

These are general graph write APIs. The request and file examples below use
CMDB-style collector data where that makes the ingestion and reconciliation
behavior concrete; other domains can omit CI types and source governance.

## Direct Commit

Request shape:

```json
{
  "expected_version": 0,
  "idempotency_key": "cmdb-sync-001",
  "mutations": {
    "upsert_entity_types": [],
    "upsert_relation_types": [],
    "upsert_entities": [],
    "upsert_edges": [],
    "delete_entities": [],
    "delete_entity_requests": [],
    "delete_edges": [],
    "delete_edge_requests": []
  }
}
```

`upsert_entity_types` and `delete_entity_types` are the domain-neutral 1.1
names. The 1.0 names `upsert_ci_types` and `delete_ci_types` remain accepted and
use the same persisted structures. Do not send both names for the same mutation.

`expected_version` is optional. When set, the commit is accepted only if the
tenant manifest is currently at that version.

`idempotency_key` is optional but recommended for upstream retry. Reusing the
same key with the same payload returns the stored result; reusing it with a
different payload returns `idempotency_conflict`.

Example:

```sh
curl -sS -X POST "$WRITER/v1/commits" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/commit-cmdb.json
```

## Entities

Entity fields:

- `id`: stable entity id.
- `kind`: application-defined entity kind, for example `host`, `service`, or
  `database` in the CMDB examples.
- `fields`: schemaless JSON object.
- `source`, `external_id`, `confidence`, `source_priority`: optional source metadata.
- `identity_keys`: optional identity metadata used by CI type identity rules.

Array field merge:

- Define array fields as `{"type":"array","merge_strategy":"append_unique"}`
  in the CI type to append unique incoming values by default.
- Use a field name suffix `!` to force replace for that write, for example
  `"tags!": ["blue"]`.
- `!` only changes array merge vs replace. It does not bypass source priority.

Example:

```json
{
  "id": "host:aws:i-001",
  "kind": "host",
  "labels": ["asset", "production"],
  "source": "aws",
  "external_id": "i-001",
  "confidence": 0.9,
  "fields": {
    "hostname": "app-01",
    "region": "us-east-1",
    "tags!": ["prod"]
  }
}
```

`labels` is a top-level 1.1 convenience field. GGraphDB normalizes and stores it
inside the 1.0-compatible fields map, so it can be queried with `labels
CONTAINS "production"` without changing the persisted entity layout.

## Relation Types And Edges

Relation type fields:

- `name`: relation type.
- `from_kind` / `to_kind`, or `from_kinds` / `to_kinds`.
- `directed`: whether direction is semantically meaningful.
- `cardinality`: `many_to_many`, `one_to_many`, `many_to_one`, `one_to_one`.
- `impact_direction`: used by impact queries.

Edges use `(type, from, to)` as canonical identity. Incoming `edge.id` is kept
as source alias; GGraphDB rewrites the stored edge id to a stable canonical id.

Example:

```json
{
  "id": "collector-edge-123",
  "type": "runs_on",
  "from": "service:api",
  "to": "host:aws:i-001",
  "source": "aws",
  "fields": {
    "status": "active"
  }
}
```

To move an edge endpoint, delete the old `(type, from, to)` and create a new
edge. Endpoint fields are identity, not mutable fields.

Optional relation property schemas are managed separately from relation types:

```sh
curl -sS -X PUT "$WRITER/v1/relation-schemas/cites" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{
    "strict": true,
    "fields": {
      "confidence": {"type": "number", "required": true},
      "source": {"type": "string", "default": "unknown"}
    }
  }'
```

The referenced relation type must already exist. Defaults and validation apply
to direct commits, ingestion batches, and file-import batches. Delete its
property schema before deleting the relation type.

## Source Policy

Source policy is tenant scoped:

```sh
curl -sS -X PUT "$WRITER/v1/source-policy" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/source-policy.json
```

Priority rules:

- If `source` exists in policy, policy priority overrides request
  `source_priority`.
- If policy exists but source is unknown, `default_priority` is used.
- If no policy exists, request `source_priority` is used.

Field priority rules:

- `field_priorities` can give selected canonical entity fields an absolute
  effective priority for a source, without changing the entity-level
  `source_priority`.
- `source + kind + field` rules override source-global field rules.
- Field priorities run after field aliases, so configure canonical field names.
- Direct commit entities without `source` do not use field priorities. Ingest
  entities inherit the batch `source` first, then field priorities are applied.

Field alias rules:

- `field_aliases` maps incoming top-level `entity.fields` names to canonical
  field names before merge, indexing, MD5 skip, query, scan, and export.
- `source + kind` rules are applied before source-global fallback rules.
- Direct commit entities without `source` do not use aliases. Ingest entities
  inherit the batch `source` first, then aliases are applied.
- If canonical and alias are both present in one payload, canonical wins. A
  different alias value is returned as a suppressed conflict with
  `alias_field`.
- Multiple aliases for the same canonical field are resolved by alias field
  name sort order, so the result is deterministic.
- Aliases do not support nested paths, wildcard matching, regex, or value/type
  conversion. Query DSL and scan/export APIs always use canonical fields.

Field merge rules:

- Higher priority overwrites lower priority.
- Equal priority uses higher `confidence`.
- Equal priority and confidence keeps last-write-wins behavior.
- Lower priority writes are suppressed, not failed.

Suppressed conflicts appear in commit and ingest responses. They are not
dead-lettered.

## Deletes

Admin-force deletes:

- `delete_entities`: list of entity ids.
- `delete_edges`: list of canonical edge ids or known aliases.

Source-aware deletes:

- `delete_entity_requests`
- `delete_edge_requests`

Use source-aware edge delete for collectors:

```json
{
  "type": "runs_on",
  "from": "service:api",
  "to": "host:aws:i-001",
  "source": "aws",
  "reason": "collector no longer observes relation"
}
```

Low-priority delete requests cannot remove higher-priority edge existence; they
return suppressed conflicts.

## Ingestion Batch

Request shape:

```json
{
  "source": "aws",
  "collector_id": "collector-a",
  "batch_id": "aws-batch-001",
  "idempotency_key": "aws-batch-001",
  "cursor": "next-source-cursor",
  "expected_version": 42,
  "failure_mode": "best_effort",
  "preconditions": [
    {"resource_type": "entity", "id": "host:aws:i-001", "op": "exists"},
    {"resource_type": "entity", "id": "host:aws:i-001", "field": "state", "op": "eq", "value": "ready"}
  ],
  "items": [
    {
      "external_id": "i-001",
      "entity": {
        "id": "host:aws:i-001",
        "kind": "host",
        "fields": {"hostname": "app-01"}
      }
    }
  ]
}
```

Transactional options are evaluated against the graph snapshot used for the
mutation. `expected_version` is an optional tenant-version CAS guard;
`failure_mode` is `best_effort` by default and may be set to `atomic`; atomic
requests publish no graph version when an item is invalid, a precondition fails,
or source governance suppresses a mutation. `preconditions` accepts at most 256
entity/edge checks. Use `exists` or `not_exists` without a field, or compare a
field with `eq`, `ne`, `lt`, `lte`, `gt`, or `gte` and a JSON `value`. Set
`value_from` to `accepted_at` instead of `value` when the comparison must use the
WAL acceptance timestamp; `value` and `value_from` are mutually exclusive.

In direct mode, successful requests return `200`; ordinary item-level failures
return `207`, a stale `expected_version` returns `409`, a failed precondition
returns `412`, and atomic validation or suppression returns `422` or `409`
respectively. In WAL mode, the initial response is `202` after durable local
takeover; these statuses are reported by the terminal `result` in the status
resource (or by `Prefer: wait=committed`). The terminal result includes
`error_code` (`version_conflict`, `precondition_failed`,
`atomic_validation_failed`, or `atomic_suppressed`) when applicable.

Supported item payloads:

- `entity`
- `edge`
- `delete_entity`
- `delete_edge`
- `relation_type`
- `ci_type`
- `entity_type` (1.1 alias of `ci_type`)

Response fields:

- `applied`: items included in a commit.
- `failed`: invalid items or commit failures.
- `suppressed`: lower-priority field/delete conflicts.
- `skipped`: the batch did not create a graph-data commit.
- `skip_reason`: `logical_noop` when the resulting logical graph is unchanged,
  or `idempotent_replay` when an earlier batch result is replayed.
- `cursor`: returned collector cursor.
- `error_code`: terminal request error classification, when the request is
  rejected by version/precondition/atomic semantics.
- `failures`: item-level errors.
- `conflicts`: suppressed conflicts and failed commit reasons.

### Per-writer WAL CAS cohorts

For either `GRAPHDB_COORDINATION=local` or `GRAPHDB_COORDINATION=postgres`, a
flush containing at least two non-atomic mutation requests from one writer with
present, identical `expected_version` values is a CAS cohort. The writer
compares that value and every request precondition once against the common graph
snapshot at flush start. Each request keeps an independent result; changed
requests receive consecutive logical versions in that writer's WAL order, while
the changed cohort is built with one copy-on-write apply, one commit segment,
and one candidate manifest. A request-local precondition failure only fails
that request. If the shared expected version is stale, every cohort member
returns `version_conflict` (HTTP `409`) and no graph version is published.
Atomic requests, different expected-version values, and requests with
preconditions but no expected version are isolation barriers; requests before
and after a barrier can still form their own cohort or ordinary fast batch. A
batch-apply fallback reuses the already-checked cohort guards rather than
rechecking them against versions created earlier in the same flush.

### Local WAL mode (1.2 compatibility profile)

`GRAPHDB_INGEST_MODE=direct` remains the default synchronous `200/207`
behavior. A single-writer deployment using `GRAPHDB_COORDINATION=local` can
explicitly enable `GRAPHDB_INGEST_MODE=wal`. Requests are first appended to one
process-wide segmented WAL, so tenants share group fsync while the default
single graph-write worker preserves per-tenant FIFO order. One tenant flush
keeps the logical commit order of its requests, writes those commits to one
Parquet commit segment, and publishes the manifest once.

Sync durability returns `202 Accepted`, `Location`, and a status URL after the
WAL fsync. Send `Prefer: wait=committed` to wait for the final `200/207` result,
or query:

```sh
curl -sS "$WRITER/v1/ingest/batches/aws/collector-a/aws-batch-001" \
  -H 'X-Tenant-ID: demo'
```

In the PostgreSQL-CAS profile the acceptance body identifies the stable owner;
the `Location` header and `status_url` carry the same owner-routed resource:

```http
HTTP/1.1 202 Accepted
Location: /v1/ingest/writers/writer-a/aws/collector-a/aws-batch-001
Content-Type: application/json

{
  "writer_id": "writer-a",
  "batch_id": "aws-batch-001",
  "state": "accepted",
  "durability": "durable",
  "accepted_at": "2026-07-30T00:00:00Z",
  "estimated_flush_at": "2026-07-30T00:00:10Z",
  "status_url": "/v1/ingest/writers/writer-a/aws/collector-a/aws-batch-001"
}
```

Poll the returned resource until `state` is `committed` or `failed`. A terminal
status embeds the final result; `recovery_pending=true` means the owner is
rebuilding WAL state and the batch remains queryable:

```json
{
  "writer_id": "writer-a",
  "tenant_id": "demo",
  "source": "aws",
  "collector_id": "collector-a",
  "batch_id": "aws-batch-001",
  "state": "committed",
  "durability": "durable",
  "result": {"batch_id": "aws-batch-001", "version": 43, "applied": 1, "failed": 0}
}
```

Main settings are `GRAPHDB_INGEST_WAL_DIR` (default
`${GRAPHDB_DATA_DIR}/wal/ingest`), `GRAPHDB_INGEST_WAL_DURABILITY=sync|os`,
`GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB`,
`GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=5ms`,
`GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB`,
`GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB`,
`GRAPHDB_INGEST_FLUSH_INTERVAL=10s`,
`GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=256`,
`GRAPHDB_INGEST_FLUSH_MAX_BYTES=8MiB`,
`GRAPHDB_INGEST_FLUSH_WORKERS=1`, and
`GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s`.

Recovery finishes before HTTP starts and holds an exclusive process lock on the
WAL directory. Middle corruption fails closed; only an incomplete final record
in the last segment is truncated. A durable accepted request continues after
client disconnect. The fsynced PREPARED commit plan in the WAL prevents another
graph version when recovering a manifest published before ingest metadata was
finalized. The first flush that encounters a historical loose-commit tail folds
it into the segment; later flushes do not pay that migration cost again.

`/metrics` exposes `graphdb_ingest_wal_*`, `graphdb_ingest_queue_*`, and
`graphdb_ingest_flush_*` metrics for append/fsync activity, WAL memory and disk
usage, written/durable LSNs, pending work and oldest age, status-cache
hits/evictions, flush latency and request/commit/segment/manifest counts, and
recovery results. These metrics use only fixed status labels; tenant, source,
collector, and batch identifiers are deliberately excluded.

JSON logs include `ingest_wal_recovery`, `ingest_wal_accepted`,
`ingest_flush_started`, `ingest_flush_completed`, WAL rotate/prune/fsync
failures, and shutdown events. Tenant, batch, LSN, flush ID, latency, and error
details remain in logs. When `GRAPHDB_OTLP_ENDPOINT` is set, accept, WAL
append/group write, flush, batch apply, publish, and metadata-finalization spans
are exported over OTLP/HTTP. Asynchronous group writes and flushes use OTel
links to the originating request span. Accepted records persist that trace
context, so recovery can retain the association after a restart.

### PostgreSQL-CAS multi-writer WAL (1.3 contract)

The 1.3 profile combines an independent local WAL on every writer with
PostgreSQL head CAS. It is release-gated; the contract below does not claim
that a particular build has passed the required multi-writer or crash matrix.
Use the [1.3 design](../ingest-wal-multiwriter-design.md) for the complete
protocol.

Configure every writer with `GRAPHDB_COORDINATION=postgres`,
`GRAPHDB_WRITER_TOPOLOGY=cas`, `GRAPHDB_INGEST_MODE=wal`, generic S3-compatible
object storage, and a unique stable `GRAPHDB_INSTANCE_ID`. Each writer must
mount its own persistent `GRAPHDB_INGEST_WAL_DIR`; two writers must never share
one WAL directory or volume. PostgreSQL schema v5 stores coordination metadata
only (tenant head/generation, idempotency reservations/results, collector state,
and batch metadata). It does not store ingest payloads, WAL records, commit
segments, or graph data. Object storage remains the graph-data authority.

After static validation and local WAL `fsync`, the writer returns `202 Accepted`
without requiring PostgreSQL to be reachable. The response includes
`writer_id` and an owner-routed `status_url`. Route that status URL to the
writer named by the stable instance ID; while that writer is recovering, status
must remain available and may report `recovery_pending=true`. A `202` means
durable takeover by that writer, not a committed graph version.

The 1.3 owner route is:

```text
GET /v1/ingest/writers/{writer_id}/{source}/{collector_id}/{batch_id}
```

The legacy `/v1/ingest/batches/{source}/{collector_id}/{batch_id}` status path
remains available for compatibility, but an owner-routed URL must not be sent
to a random writer.

WAL flushes are bounded per-tenant batches. A writer preserves its local WAL
FIFO; different writers are ordered by successful PostgreSQL head CAS, not by
HTTP arrival time. CAS, PostgreSQL, and temporary object-store failures remain
retryable: reload the newest head, rebase, apply exponential backoff with
jitter, and shrink the batch prefix at request/cohort barriers after repeated
conflicts. An accepted request cannot become terminally failed merely because a
retry budget was exhausted. When a PostgreSQL writer loses head CAS, its
candidate remains invisible and it reloads the new head. A losing
`expected_version` cohort is not merged with another writer's payload or
rebased to a different expected version; if the expected version is now stale,
all of that cohort's members finalize as `version_conflict` without publication.
Deterministic semantic errors and lifecycle fencing (freeze/delete/restore) may
finalize as `failed`; lifecycle fencing wins over unpublished WAL and never
rolls back a version already published by CAS.

The profile supports two to eight concurrent writers for one tenant. This is a
correctness and availability boundary; the throughput scale target is across
tenants, not linear single-tenant speed-up. PostgreSQL unavailability does not
fall back to local coordination. A writer may accept locally durable requests
until its WAL high-water policy is reached, then rejects new admission before
writing another payload while it continues to drain and serve owner status.

For rolling upgrade, first deploy the 1.3 binary in direct mode and validate
the existing v5 coordination plane. Enable WAL writer by writer. Before a
downgrade, stop new WAL admission and wait for that writer's WAL to finalize;
a pending WAL is a downgrade blocker. The 1.2 direct and 1.3 WAL profiles may
coexist only under the documented rollout and shared object layout.

Collector batch sizing for CMDB workloads:

- Start with 200 logical CMDB groups per batch, then move toward 500 when the
  object store and writer timeout budget are stable.
- Prefer larger batches over many small concurrent batches. Every batch has
  fixed commit, manifest, idempotency, and collector metadata cost.
- When `batch_id` already identifies the collector checkpoint, reuse the same
  value as `idempotency_key`; the writer can then store one coalesced ingest
  record instead of two metadata objects.
- Keep retrying the same `batch_id` and `idempotency_key` after a `429`, with
  exponential backoff and jitter. Do not generate a new idempotency key for a
  retry of the same source page.
- Increase collector HTTP timeouts when moving from 200 to 500 groups because
  each group commonly expands to multiple entities and edges.

Collector status:

```sh
curl -sS "$WRITER/v1/ingest/collectors/aws/collector-a" \
  -H 'X-Tenant-ID: demo'
```

Dead letters:

```sh
curl -sS "$WRITER/v1/ingest/deadletters/aws" -H 'X-Tenant-ID: demo'
curl -sS -X POST "$WRITER/v1/ingest/deadletters/aws/replay?limit=10" -H 'X-Tenant-ID: demo'
```

## CSV And JSONL Import

JSONL contains one existing ingestion item per non-empty line:

```jsonl
{"external_id":"doc-1","entity":{"id":"document:1","kind":"document","labels":["article"],"fields":{"title":"Graph Storage"}}}
{"external_id":"cite-1","edge":{"type":"cites","from":"document:1","to":"document:2","fields":{"confidence":0.95}}}
```

```sh
curl -sS -X POST \
  "$WRITER/v1/imports?source=knowledge-base&collector_id=files&batch_size=500&on_error=continue" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @graph.jsonl
```

CSV requires `record_type`. Entity/node and edge/relationship rows use the
reserved columns below; any other non-empty column becomes a typed property.
`labels` accepts a JSON string array or pipe-delimited text.

```csv
record_type,id,entity_type,labels,relation_type,from,to,title,confidence
entity,document:1,document,article|published,,,,Graph Storage,
edge,,,,cites,document:1,document:2,,0.95
```

```sh
curl -sS -X POST "$WRITER/v1/imports?format=csv&on_error=abort" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: text/csv' \
  --data-binary @graph.csv
```

Supported CSV `record_type` values are `entity`/`node`, `edge`/`relationship`,
`delete_entity`/`delete_node`, `delete_edge`/`delete_relationship`,
`entity_type`, and `relation_type`. Type-definition rows carry their JSON in a
`payload` or `definition` column.

The endpoint returns `202` with a normal `bulk_import` task and a `Location`
header. Poll `/v1/tasks/{id}` for checkpoint, progress, issue samples, and the
final counts. `format` can be inferred from JSONL/CSV content type; `batch_size`
defaults to 500 and is capped at 5000; `on_error` is `abort` or `continue`.
The current upload limit is 32 MiB and only one bulk import runs per tenant.

## MD5 Skip

Commits and ingestion apply mutations to the current graph. If the resulting
data MD5 matches the stored current graph, GGraphDB skips writing a new commit
and returns `skipped=true`. This avoids commit tail growth for repeated
collector payloads.

## Write Backpressure

Write admission can return `429` with structured reasons:

```json
{
  "error": "write backpressure",
  "code": "write_backpressure",
  "retry_after_ms": 2000,
  "reasons": [
    {
      "code": "commit_tail_too_long",
      "current": 301,
      "threshold": 1500,
      "message": "compact required"
    }
  ]
}
```

Collectors should honor `Retry-After`, retry with the same `idempotency_key`,
and reduce concurrency if the same reason repeats.

Common reasons:

- object store latency or errors.
- manifest CAS conflicts.
- index rebuild or maintenance task running.
- commit tail too long.
- tenant entity/edge/object/byte quota exceeded.
