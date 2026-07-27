# 数据模型

[English](data-model.md)

GGraphDB 为每个租户保存一张当前态属性知识图谱，不提供历史版本查询；每次
读取观察一个 manifest snapshot 版本。GGraphDB 1.1 不提供 RDF/OWL 存储、
SPARQL、本体推理或向量检索。

核心模型与具体领域无关：应用可以只使用无模式实体和类型化边，不必定义实体
类型。`EntityType`（1.0 名称为 `CIType`）和 source governance 都是可选
领域元数据，用于需要采集合并和身份治理的场景。

## Tenant

数据 API 通过 `X-Tenant-ID` 提供租户 ID。每个租户包含：

- manifest 和 commit tail；
- 可选的 compact snapshot；
- entity page 和按 ID 的实体记录；
- edge shard；
- 持久化二级索引；
- source policy、tenant config、saved query、task、dead letter 和 reader
  heartbeat。

## 实体类型（`CIType` 兼容别名）

`EntityType` 是 1.1 对可选 kind 级元数据的领域中立命名；它与 1.0 的
`CIType` 是同一个持久化对象，`CIType` 继续作为兼容别名。实体需要字段规则
和身份合并时可以定义实体类型：

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

重要字段：

- `id`：稳定的内部 ID；
- `kind`：实体类型；
- `labels`：可选的领域中立分类；`labels CONTAINS "production"` 执行标签
  成员过滤；
- `fields`：无模式 JSON 对象；
- `source`、`external_id`：上游身份；
- `confidence`：source priority 相同的平局决胜值；
- `source_priority`：没有租户 source policy 覆盖时使用；
- `field_sources`：GGraphDB 记录的字段归属；
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

## 关系属性 Schema

GGraphDB 1.1 可以为已有关系类型选择性定义边属性校验和默认值：

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

通过 `PUT /v1/relation-schemas/cites` 创建或替换。Schema 必须引用已存在的
关系类型，支持 `type`、`required`、`enum`、`default`；`strict=true` 会拒绝
未声明的边属性。发布 schema 前，已有边也必须满足它。
删除关系类型前应先删除对应的属性 schema。

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

如果写入后的 MD5 与当前图一致，GGraphDB 返回 `skipped=true`，不会发布
新 commit。

## 1.0 数据兼容性

1.1 保持核心 manifest、snapshot、commit、entity、edge 和 Parquet 的对象
布局版本为 2：

- `EntityType` 只是已有 `CIType` 对象的 API/代码别名；
- 标签编码在普通实体字段 `fields.__graphdb_labels` 中，1.1 API 同时以顶层
  `labels` 便捷字段暴露；
- 关系属性 schema 和反向邻接产物放在
  `tenants/<tenant>/extensions/v1.1/` 下。

因此 1.0 reader 可以继续读取核心图，并忽略保留字段和扩展 sidecar。1.0
writer 不会执行 1.1 关系属性校验，所以受 schema 管理的边应继续由 1.1
writer 写入。
