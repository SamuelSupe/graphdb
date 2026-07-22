# 数据模型

[English](data-model.md)

GraphDB 为每个租户保存一张当前图，不提供历史版本查询；每次读取观察一个
manifest snapshot 版本。

## Tenant

数据 API 通过 `X-Tenant-ID` 提供租户 ID。每个租户包含：

- manifest 和 commit tail；
- 可选的 compact snapshot；
- entity page 和按 ID 的实体记录；
- edge shard；
- 持久化二级索引；
- source policy、tenant config、saved query、task、dead letter 和 reader
  heartbeat。

## CI Type

`CIType` 为 CMDB 实体提供可选的 kind 级元数据：

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

校验保持有意轻量，因为上游系统应负责 payload 校验。CI type 主要用于默认
值、索引、身份合并和运维理解。

数组字段可以使用 `merge_strategy: "append_unique"`。现有数组顺序保持不变，
新来的不重复值追加到末尾。写入时可以在字段名后加 `!` 强制替换，例如
`"tags!": ["blue"]`。

## Entity

实体结构：

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

重要字段：

- `id`：稳定的内部 ID；
- `kind`：实体类型；
- `fields`：无模式 JSON 对象；
- `source`、`external_id`：上游身份；
- `confidence`：source priority 相同的平局决胜值；
- `source_priority`：没有租户 source policy 覆盖时使用；
- `field_sources`：GraphDB 记录的字段归属；
- `sources`：累积的来源观察记录。

## Relation Type

关系类型：

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

支持的 cardinality：

- `many_to_many`
- `one_to_many`
- `many_to_one`
- `one_to_one`

`impact_direction` 控制该关系类型在影响查询中的传播。

## Edge

输入边：

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

保存的边身份由 `(type, from, to)` 规范化：

```text
edge:<sha256(type + "\x00" + from + "\x00" + to) first 32 hex chars>
```

输入 `id` 会作为 source metadata 中的别名保留。相同 triple 的重复 upsert
会合并为同一条边，即使不同采集器使用不同 ID。

## Source Governance

租户 source policy 定义有效优先级：

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

`field_priorities` 只作用于顶层实体字段，并在写入别名处理后使用规范字段名。
它改变字段归属优先级，不改变实体级 `source_priority`。

实体字段、边字段和边存在性的合并顺序：

1. 高优先级胜出；
2. 优先级相同使用更高 `confidence`；
3. 优先级和 confidence 都相同使用最后写入；
4. 低优先级写入/删除被抑制并返回。

管理员强制删除数组绕过 source governance；采集器应使用来源感知的删除请求。

## Snapshot Version

每个可见 commit 都会增加租户 manifest 的 `version`。读响应包含其观察到的
版本；读后写场景使用 `min_version`。

如果写入后的 MD5 与当前图一致，GraphDB 返回 `skipped=true`，不会发布
新 commit。
