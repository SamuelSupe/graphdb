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
- `failures`: item-level errors.
- `conflicts`: suppressed conflicts and failed commit reasons.

### Local WAL mode

GGraphDB 1.2 defaults local writers to `GRAPHDB_INGEST_MODE=wal`,
`GRAPHDB_INGEST_METADATA_MODE=segment`, and sync durability. Requests are first
appended to one process-wide segmented WAL, so tenants share group fsync while
two graph-write workers process independent tenants and preserve per-tenant FIFO
order. The graph flush trigger is 8 requests / 2 MiB; busy tenants may merge the
same-round queue. One tenant flush keeps the logical commit order of its requests,
writes those commits to one
Parquet commit segment, and publishes the manifest once.

PostgreSQL coordination has no distributed WAL. Set
`GRAPHDB_INGEST_MODE=direct` explicitly for PostgreSQL writers; startup fails
closed when that choice is omitted. Reader processes select direct mode
automatically because they do not accept writes.

Sync durability returns `202 Accepted`, `Location`, and a status URL after the
WAL fsync. Send `Prefer: wait=committed` to wait for the final `200/207` result,
or query:

```sh
curl -sS "$WRITER/v1/ingest/batches/aws/collector-a/aws-batch-001" \
  -H 'X-Tenant-ID: demo'
```

The WAL directory is part of the durability boundary. Container deployments
must mount `GRAPHDB_DATA_DIR` on persistent local storage; the provided Compose
profiles mount `/var/lib/graphdb` for the writer.

Main settings are `GRAPHDB_INGEST_WAL_DIR` (default
`${GRAPHDB_DATA_DIR}/wal/ingest`), `GRAPHDB_INGEST_WAL_DURABILITY=sync`,
`GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB`,
`GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=5ms`,
`GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB`,
`GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB`,
`GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK=80`,
`GRAPHDB_INGEST_WAL_HIGH_WATERMARK=70`,
`GRAPHDB_INGEST_WAL_STOP_WATERMARK=85`,
`GRAPHDB_INGEST_MAX_PENDING_AGE=2m`,
`GRAPHDB_INGEST_FLUSH_INTERVAL=250ms`,
`GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=8`,
`GRAPHDB_INGEST_FLUSH_MAX_BYTES=2MiB`,
`GRAPHDB_INGEST_FLUSH_WORKERS=2`,
`GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms`,
`GRAPHDB_INGEST_METADATA_MAX_REQUESTS=256`,
`GRAPHDB_INGEST_METADATA_MAX_BYTES=8MiB`,
`GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2`,
`GRAPHDB_WRITE_CACHE_MAX_BYTES=4GiB`,
`GRAPHDB_WRITE_MAX_COMMIT_TAIL=20000`, and
`GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s`.

At the memory queue's 80% watermark, WAL disk's 70% watermark, or a two-minute
old pending request, the writer returns `429` with `Retry-After`. At 85% WAL
disk usage it enters drain-only readiness until committed work is reclaimed.
Retry the same batch and idempotency identity after the advertised delay.

#### Metadata segment mode (1.1.3+)

GGraphDB 1.1.3 introduced metadata batching on top of local WAL ingest. It is
the local-writer default in 1.2:

```ini
GRAPHDB_INGEST_MODE=wal
GRAPHDB_COORDINATION=local
GRAPHDB_INGEST_METADATA_MODE=segment
GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms
GRAPHDB_INGEST_METADATA_MAX_REQUESTS=256
GRAPHDB_INGEST_METADATA_MAX_BYTES=8MiB
GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2
```

Direct mode uses `legacy`; local WAL mode defaults to `segment`. Segment mode stores full requests, results,
digests, and final collector snapshots from graph flushes of the same tenant in
one content-addressed Parquet segment, then publishes it with one independent
ingest-manifest CAS. Batch and idempotency identities point to the same segment
row, so a normal 256-request window creates no per-request batch, idempotency,
or collector-status objects.

The manifest keeps 32 recent segment references. Older references move into
tiered catalogs with at most eight catalogs per level. Catalog compaction
rewrites only references and Bloom filters, never historical payload segments.
Lookup order is active WAL, segment/index, then legacy objects. Existing history
is neither migrated nor deleted; the first segment collector snapshot is seeded
from legacy status or legacy batch records.

A request becomes `published` after graph-manifest publication and `committed`
only after the metadata manifest is durable. `Prefer: wait=committed` forces the
current tenant metadata window; normal `202` requests may wait for a threshold
or the 500 ms interval. The WAL persists a metadata flush ID, LSN range, and
content identity so crashes between segment PUT, manifest CAS, and `FINALIZED`
do not duplicate graph versions, collector totals, or results.

Upgrade all readers and writers to 1.1.3 in `legacy` mode before enabling
`segment`. After a tenant has a segment manifest, a 1.1.3 legacy writer refuses
old-format ingest. Do not run a 1.1.2 writer after activation; stop writes and
export or convert with 1.1.3-compatible tooling before rollback. Direct mode,
graph commits, logical versions, FIFO order, tenant isolation, and dead-letter
object layout are unchanged.

Recovery finishes before HTTP starts and holds an exclusive process lock on the
WAL directory. Middle corruption fails closed; only an incomplete final record
in the last segment is truncated. A durable accepted request continues after
client disconnect. The fsynced PREPARED commit plan in the WAL prevents another
graph version when recovering a manifest published before ingest metadata was
finalized. The first flush that encounters a historical loose-commit tail folds
it into the segment; later flushes do not pay that migration cost again.

#### 1.1.4 sparse-tenant operation

Metadata flush workers are independent of graph write workers. The default is
two, with 500 ms and 256 requests / 8 MiB as scheduling triggers. For hundreds
of mostly sparse tenants, tune those values against
object-store p95 latency and the metadata deadline-overshoot metric. A tenant
still has at most one in-flight metadata flush, and metadata segments never
mix tenants; more workers therefore improve fairness rather than changing
ordering or isolation.

Per-tenant automatic maintenance waits for one minute of ingest idleness before
running compaction, GC, or index catch-up. Heavy background task execution is
single-concurrency by default.

Each writer keeps a bounded, process-local LRU for ingest metadata manifests,
catalogs, and immutable segments: at most 1,024 objects and 64 MiB of charged
residency. A decoded segment is charged by the greater of its encoded bytes or
4 KiB per retained item. Manifest entries expire after one second; immutable index and segment
entries expire after 15 minutes. Concurrent cold reads of one object share a
single object-store request; a warm lookup does not issue another GET. The
cache is disposable and is rebuilt after restart. A successful metadata
manifest CAS replaces the local manifest entry; it is not a cross-writer
coherence mechanism.

When WAL pruning advances past a known-safe position, the writer atomically
records `checkpoint.json` in `GRAPHDB_INGEST_WAL_DIR`. Startup resumes from a
valid checkpoint, scanning only the active tail. Missing, torn, or invalid
checkpoint files fall back to a full WAL scan; corruption in the WAL itself
continues to fail startup. Do not edit, copy independently, or restore a
checkpoint without its corresponding WAL directory. It is an acceleration
record, not a replacement for the WAL or object-store backup.

Use `graphdb_ingest_metadata_deadline_overshoot_seconds`,
`graphdb_ingest_metadata_cache_total`, and
`graphdb_ingest_wal_checkpoint_{total,scanned_bytes,duration_seconds}` to
separate worker saturation, lookup pressure, and recovery cost. JSON logs add
`ingest_wal_checkpoint_written` and `ingest_wal_checkpoint_recovery`; their
outcomes are `used`, `miss`, `fallback`, `written`, or `error` as applicable.

`/metrics` exposes `graphdb_ingest_wal_*`, `graphdb_ingest_queue_*`,
`graphdb_ingest_flush_*`, and `graphdb_ingest_metadata_*` metrics for
append/fsync activity, WAL memory and disk
usage, written/durable LSNs, pending work and oldest age, status-cache
hits/evictions, flush latency, logical and physical segment/object counts,
manifest conflicts, Bloom candidates, metadata cache results, checkpoint scan
cost, replay bytes, and recovery results.
These metrics use only fixed status labels; tenant, source,
collector, and batch identifiers are deliberately excluded.

JSON logs include `ingest_wal_recovery`, sampled `ingest_wal_accepted`
(the first success and every 1,024th success),
`ingest_flush_started`, `ingest_flush_completed`,
`ingest_metadata_flush_started`, `ingest_metadata_segment_completed`,
`ingest_metadata_manifest_published`, WAL rotate/prune/fsync
failures, and shutdown events. Tenant, batch, LSN, flush ID, latency, and error
details remain in logs. Metrics, traces, failure logs, and WAL records are not
sampled. When `GRAPHDB_OTLP_ENDPOINT` is set, accept, WAL
append/group write, flush, batch apply, publish, metadata encode/PUT, manifest
CAS, and Bloom/index lookup spans
are exported over OTLP/HTTP. Asynchronous group writes and flushes use OTel
links to the originating request span. Accepted records persist that trace
context, so recovery can retain the association after a restart.

#### 1.1.5 WAL fault handling

Transient object-store or metadata-flush errors are retried. While retry work is
pending, readiness remains unavailable; after a successful retry the writer
clears its last error and becomes ready again without a restart. This does not
change the durable `202` contract.

A fatal WAL append, short-write, rotation, or fsync error fences the local WAL
writer. New ingest appends are rejected with `503` and the stable
`ingest_wal_unavailable` code (`retryable=true`), no new LSN is assigned, and the
durable records already accepted in the WAL remain the recovery source of truth.
Keep the WAL directory intact, repair or replace the failed storage, and restart
only after the failure is understood. A fenced writer must not continue
appending to a possibly damaged tail.

The v1.1.5 release gate exercises this behavior with the real binary in
`GRAPHDB_INGEST_MODE=wal`, `GRAPHDB_COORDINATION=local`, and explicit
`GRAPHDB_INGEST_METADATA_MODE=segment`. Its commit-bound evidence covers a
durable accepted batch across process restart and object-store interruption;
those explicitly selected modes.

#### 1.2.0 performance-first defaults

The v1.1.5 to v1.2.0 data upgrade is one-way. Stop the old writer, retain the
WAL and object data, then start v1.2.0; do not run a v1.1.5 writer after v1.2.0
has activated segment metadata. There is no reverse-compatibility release gate.
The graph model, logical commit/version order, FIFO semantics, and object layout
remain unchanged; only physical batching and default runtime policy change.

The release gate runs v1.1.5 and v1.2.0 five times each for 30 minutes on one
fixed OrbStack host. v1.2.0 must commit at least 10,000 mutations/s and reach
1.5x the v1.1.5 median, with accepted p95/p99 at most 20/50 ms, committed
p95/p99 at most 8/15 seconds, RSS at most 7 GiB and 110% of baseline, CPU per
1,000 mutations at least 25% lower, throughput spread at most 5%, and direct
write/query regression at most 10%.

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
