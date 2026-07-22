# Write And Ingest

[中文](write-ingest.zh-CN.md)

GraphDB has two write paths:

- `POST /v1/commits`: direct atomic graph mutation.
- `POST /v1/ingest/batches`: collector-oriented batch ingestion with source,
  cursor, idempotency, partial failure, dead-letter, and collector status.

Both require `X-Tenant-ID`. In reader mode both return `405`.

## Direct Commit

Request shape:

```json
{
  "expected_version": 0,
  "idempotency_key": "cmdb-sync-001",
  "mutations": {
    "upsert_ci_types": [],
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
- `kind`: CI kind, for example `host`, `service`, `database`.
- `fields`: schemaless JSON object.
- `source`, `external_id`, `confidence`, `source_priority`: source metadata.
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
  "source": "aws",
  "external_id": "i-001",
  "confidence": 0.9,
  "fields": {
    "hostname": "app-01",
    "region": "us-east-1",
    "tags": ["prod"],
    "labels!": ["owned"]
  }
}
```

## Relation Types And Edges

Relation type fields:

- `name`: relation type.
- `from_kind` / `to_kind`, or `from_kinds` / `to_kinds`.
- `directed`: whether direction is semantically meaningful.
- `cardinality`: `many_to_many`, `one_to_many`, `many_to_one`, `one_to_one`.
- `impact_direction`: used by impact queries.

Edges use `(type, from, to)` as canonical identity. Incoming `edge.id` is kept
as source alias; GraphDB rewrites the stored edge id to a stable canonical id.

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

Response fields:

- `applied`: items included in a commit.
- `failed`: invalid items or commit failures.
- `suppressed`: lower-priority field/delete conflicts.
- `skipped`: idempotent replay or MD5-identical graph write.
- `cursor`: returned collector cursor.
- `failures`: item-level errors.
- `conflicts`: suppressed conflicts and failed commit reasons.

Collector batch sizing:

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

## MD5 Skip

Commits and ingestion apply mutations to the current graph. If the resulting
data MD5 matches the stored current graph, GraphDB skips writing a new commit
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
      "threshold": 300,
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
