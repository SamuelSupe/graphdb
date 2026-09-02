# Data Model

[中文](data-model.zh-CN.md)

GGraphDB stores one current-state property knowledge graph per tenant. It does
not expose historical version queries; each read observes a manifest snapshot
version. GGraphDB 1.1 does not implement RDF/OWL storage, SPARQL, ontology
reasoning, or vector retrieval.

The core model is domain-neutral: applications can use schemaless entities and
typed edges without defining an entity type. `EntityType` (`CIType` in 1.0) and
source governance are optional domain metadata, useful for ingestion and
reconciliation.

## Tenant

Tenant id is supplied by `X-Tenant-ID` for data APIs. Each tenant has:

- manifest and commit tail.
- optional compacted snapshot.
- entity pages and entity by-id records.
- edge shards.
- persisted secondary indexes.
- source policy, tenant config, saved queries, tasks, dead letters, and reader
  heartbeats.

## Entity Type (`CIType` Compatibility Alias)

`EntityType` is the domain-neutral 1.1 name for optional kind-level metadata.
It is the same persisted object as the 1.0 `CIType`, which remains available as
a compatibility alias. Entity types are especially useful when entities need
field rules and identity reconciliation:

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
  "labels": ["asset", "production"],
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
- `labels`: optional domain-neutral classifications. `labels CONTAINS
  "production"` performs label membership filtering.
- `fields`: schemaless JSON object.
- `source` and `external_id`: upstream identity.
- `confidence`: tie breaker for equal source priority.
- `source_priority`: used only when no tenant source policy overrides it.
- `field_sources`: stored by GGraphDB to record field ownership.
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

## Relation Property Schema

GGraphDB 1.1 can optionally validate and default edge properties for an existing
relation type:

```json
{
  "relation_type": "cites",
  "strict": true,
  "fields": {
    "confidence": {"type": "number", "required": true},
    "source": {"type": "string", "default": "unknown"},
    "status": {"type": "string", "enum": ["draft", "verified"]}
  }
}
```

Create or replace it with `PUT /v1/relation-schemas/cites`. A schema must refer
to an existing relation type. Supported property constraints are `type`,
`required`, `enum`, and `default`; `strict=true` rejects undeclared edge
properties. Existing edges must also satisfy a schema before it can be
published. Delete the property schema before deleting its relation type.

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

If a write is MD5-identical to the current graph, GGraphDB returns
`skipped=true` and does not publish a new commit.

## 1.0 Data Compatibility

Version 1.1 leaves the core manifest, snapshot, commit, entity, edge, and
Parquet layout at object layout version 2:

- `EntityType` is an API/code alias of the existing `CIType` object.
- labels are encoded in the ordinary entity `fields.__graphdb_labels` value and
  exposed as the top-level `labels` convenience field by 1.1 APIs.
- relation property schemas and reverse adjacency artifacts live under
  `tenants/<tenant>/extensions/v1.1/`.
- 1.2 retrieval definitions, immutable catalogs, retrieval segments, and the
  CAS-published retrieval head live under
  `tenants/<tenant>/extensions/v1.2/retrieval/`.

A 1.0 or 1.1 reader can therefore continue reading the core graph and ignore
the reserved field and extension sidecars. A 1.0 writer does not enforce 1.1
relation property schemas, so schema-governed edge writes should stay on 1.1
or later.
