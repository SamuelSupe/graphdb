# 扫描与导出

[English](scan-export.md)

Scan/export API 在不经过查询 planner 的情况下返回当前状态，适合运维、
对账、迁移和离线导出。所有 endpoint 都需要 `X-Tenant-ID`。

## 列出实体

```sh
curl -sS "$READER/v1/entities?kind=host&source=aws&limit=500" \
  -H 'X-Tenant-ID: demo'
```

参数：

- `kind`：实体 kind；
- `source`：来源过滤；
- `shard` 或 `entity_shard`：分片过滤；
- `limit`：页大小；
- `cursor`：下一页 cursor；
- `min_version`、`allow_stale`：读取新鲜度。

响应：

```json
{
  "version": 12,
  "entities": [],
  "next_cursor": ""
}
```

流式读取：

```sh
curl -sS "$READER/v1/entities/stream?kind=host" \
  -H 'X-Tenant-ID: demo'
```

流式 endpoint 返回 `application/x-ndjson`。

## 列出边

```sh
curl -sS "$READER/v1/edges?type=runs_on&from_shard=ab&limit=500" \
  -H 'X-Tenant-ID: demo'
```

参数：

- `type` 或 `relation_type`：边关系类型；
- `from`：精确的源实体 ID；
- `from_shard` 或 `shard`：源分片；
- `source`：来源过滤；
- `limit`、`cursor`；
- `min_version`、`allow_stale`。

流式读取：

```sh
curl -sS "$READER/v1/edges/stream?type=runs_on" \
  -H 'X-Tenant-ID: demo'
```

## 导出 Snapshot

小租户可以使用内联 JSON：

```sh
curl -sS "$READER/v1/export/snapshot" -H 'X-Tenant-ID: demo'
```

大租户应使用 NDJSON 流：

```sh
curl -sS "$READER/v1/export/snapshot/stream" \
  -H 'X-Tenant-ID: demo' \
  > snapshot.ndjson
```

默认情况下，流式导出在可用时会返回分片 snapshot page 的引用。使用
`inline=true` 强制返回内联实体和边：

```sh
curl -sS "$READER/v1/export/snapshot/stream?inline=true" \
  -H 'X-Tenant-ID: demo'
```

## 一致性

在特定写入后导出时使用 `min_version`：

```sh
curl -sS "$READER/v1/export/snapshot/stream?min_version=123" \
  -H 'X-Tenant-ID: demo'
```

如果 reader 在 `GRAPHDB_READER_CATCHUP_TIMEOUT` 内无法追上该版本，会返回
带重试提示的 `reader_not_fresh`。
