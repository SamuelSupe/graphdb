# Data Model

GraphDB stores one current graph per tenant. It does not expose historical
version queries; each read observes a manifest snapshot version.

## Tenant

Tenant id is supplied by `X-Tenant-ID` for data APIs. Each tenant has:

- manifest and commit tail.
- optional compacted snapshot.
- entity pages and entity by-id records.
- edge shards.
- persisted secondary indexes.
- source policy, tenant config, saved queries, tasks, dead letters, and reader
  heartbeats.

## CI Type

`CIType` defines optional kind-level metadata for CMDB entities:

```json
{
  "name": "host",
  "display_name": "Host",
  "fields": {
    "hostname": {"type": "string", "required": true, "unique": true, "indexed": true},
    "region": {"type": "string", "default": "unknown", "indexed": true},
    "tags": {"type": "array", "merge_strategy": "append_unique"}
  },
  "identity_keys": [
    {"name": "hostname", "fields": ["hostname"], "strategy": "merge"}
  ]
}
```

Validation is intentionally light because upstream systems are expected to
validate payloads. CI type fields are mainly used for defaults, indexing,
identity reconciliation, and operator understanding.

Array fields can opt into merge behavior with `merge_strategy:
"append_unique"`. Existing array order is preserved and incoming unique values
are appended. A write can force replace for that field by using a `!` suffix in
the entity payload, for example `"tags!": ["blue"]`.

## Entity

Entity shape:

```json
{
  "id": "host:aws:i-001",
  "kind": "host",
  "source": "aws",
  "external_id": "i-001",
  "confidence": 0.9,
  "source_priority": 50,
  "fields": {
    "hostname": "app-01",
    "region": "us-east-1"
  }
}
```

Important fields:

- `id`: stable internal id.
- `kind`: entity kind.
- `fields`: schemaless JSON object.
- `source` and `external_id`: upstream identity.
- `confidence`: tie breaker for equal source priority.
- `source_priority`: used only when no tenant source policy overrides it.
- `field_sources`: stored by GraphDB to record field ownership.
- `sources`: accumulated source observations.

## Relation Type

Relation type shape:

```json
{
  "name": "runs_on",
  "from_kind": "service",
  "to_kind": "host",
  "directed": true,
  "cardinality": "many_to_one",
  "impact_direction": "reverse"
}
```

Supported cardinality values:

- `many_to_many`
- `one_to_many`
- `many_to_one`
- `one_to_one`

`impact_direction` controls impact query propagation for that relation type.

## Edge

Incoming edge shape:

```json
{
  "id": "collector-edge-123",
  "type": "runs_on",
  "from": "service:api",
  "to": "host:aws:i-001",
  "source": "aws",
  "external_id": "collector-edge-123",
  "fields": {"status": "active"}
}
```

Stored edge identity is canonicalized from `(type, from, to)`:

```text
edge:<sha256(type + "\x00" + from + "\x00" + to) first 32 hex chars>
```

The incoming `id` remains in source metadata as an alias. Repeated upserts of
the same triple merge into the same edge, even if different collectors use
different ids.

## Source Governance

Tenant source policy defines effective priority:

```json
{
  "default_priority": 0,
  "sources": [
    {"name": "manual", "priority": 1000},
    {"name": "agent", "priority": 100},
    {"name": "aws", "priority": 50}
  ],
  "field_priorities": [
    {"source": "aws", "kind": "host", "fields": {"hostname": 1200}}
  ]
}
```

`field_priorities` applies only to top-level entity fields and uses canonical
field names after write-time aliases. It changes the field owner priority, not
the entity-level `source_priority`.

Entity field, edge field, and edge existence merges use:

1. Higher priority wins.
2. Equal priority uses higher `confidence`.
3. Equal priority and confidence uses last write.
4. Lower priority writes/deletes are suppressed and reported.

Admin-force delete arrays bypass source governance. Collector paths should use
source-aware delete requests.

## Snapshot Version

Every visible commit increments the tenant manifest `version`. Read responses
include the `version` they observed. Use `min_version` for read-after-write.

If a write is MD5-identical to the current graph, GraphDB returns
`skipped=true` and does not publish a new commit.
