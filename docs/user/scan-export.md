# Scan And Export

[中文](scan-export.zh-CN.md)

Scan/export APIs return current state without the query planner. Use them for
operations, reconciliation, migration, and offline export.

All endpoints require `X-Tenant-ID`.

## List Entities

```sh
curl -sS "$READER/v1/entities?kind=host&source=aws&limit=500" \
  -H 'X-Tenant-ID: demo'
```

Query parameters:

- `kind`: entity kind filter.
- `source`: source filter.
- `shard` or `entity_shard`: shard filter.
- `limit`: page size.
- `cursor`: next page cursor.
- `min_version`, `allow_stale`: read freshness.

Response:

```json
{
  "version": 12,
  "entities": [],
  "next_cursor": ""
}
```

Streaming:

```sh
curl -sS "$READER/v1/entities/stream?kind=host" \
  -H 'X-Tenant-ID: demo'
```

The streaming endpoint returns `application/x-ndjson`.

## List Edges

```sh
curl -sS "$READER/v1/edges?type=runs_on&from_shard=ab&limit=500" \
  -H 'X-Tenant-ID: demo'
```

Query parameters:

- `type` or `relation_type`: edge relation type.
- `from`: exact source entity id.
- `from_shard` or `shard`: source shard.
- `source`: source filter.
- `limit`
- `cursor`
- `min_version`, `allow_stale`

Streaming:

```sh
curl -sS "$READER/v1/edges/stream?type=runs_on" \
  -H 'X-Tenant-ID: demo'
```

## Export Snapshot

Small tenants can use inline JSON:

```sh
curl -sS "$READER/v1/export/snapshot" -H 'X-Tenant-ID: demo'
```

Large tenants should use NDJSON streaming:

```sh
curl -sS "$READER/v1/export/snapshot/stream" \
  -H 'X-Tenant-ID: demo' \
  > snapshot.ndjson
```

By default, stream export can return references to sharded snapshot pages when
available. Add `inline=true` to force inline entity and edge rows:

```sh
curl -sS "$READER/v1/export/snapshot/stream?inline=true" \
  -H 'X-Tenant-ID: demo'
```

## Consistency

Use `min_version` when exporting after a specific write:

```sh
curl -sS "$READER/v1/export/snapshot/stream?min_version=123" \
  -H 'X-Tenant-ID: demo'
```

If the reader cannot reach the version within `GRAPHDB_READER_CATCHUP_TIMEOUT`,
the response is `reader_not_fresh` with a retry hint.
