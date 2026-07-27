# 写入与采集

[English](write-ingest.md)

GGraphDB 有三条写入路径：

- `POST /v1/commits`：直接原子图变更。
- `POST /v1/ingest/batches`：面向采集器的批量写入，支持 source、cursor、
  幂等、部分失败、死信和采集器状态。
- `POST /v1/imports`：基于 ingest batch 的异步 CSV/JSONL 批量导入，支持
  task checkpoint 和恢复。

三者都需要 `X-Tenant-ID`；在 reader 模式都返回 `405`。

这些都是通用图数据写入接口。下面的请求和文件示例在需要具体说明采集与
身份合并时使用 CMDB 风格数据；其他领域可以不使用 CI type 和 source governance。

## 直接提交

请求结构：

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

`upsert_entity_types`、`delete_entity_types` 是领域中立的 1.1 名称；1.0 的
`upsert_ci_types`、`delete_ci_types` 仍然可用，并操作相同持久化结构。同一
mutation 不要同时发送两种名称。

`expected_version` 可选；设置后只有租户 manifest 仍处于该版本时才接受
提交。`idempotency_key` 可选但建议使用。相同 key 和相同 payload 会返回
已保存结果；相同 key 配合不同 payload 会返回 `idempotency_conflict`。

示例：

```sh
curl -sS -X POST "$WRITER/v1/commits" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/commit-cmdb.json
```

## 实体

实体字段：

- `id`：稳定实体 ID。
- `kind`：由应用定义的实体类型；下方 CMDB 示例使用 `host`、`service`、`database`。
- `fields`：无模式 JSON 对象。
- `source`、`external_id`、`confidence`、`source_priority`：可选的来源元数据。
- `identity_keys`：由 CI type 身份规则使用的可选身份信息。

数组字段合并：

- 在 CI type 中定义 `{"type":"array","merge_strategy":"append_unique"}`，
  默认追加不重复值。
- 在字段名后加 `!` 强制本次写入替换，例如 `"tags!": ["blue"]`。
- `!` 只改变数组合并/替换行为，不绕过 source priority。

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

`labels` 是 1.1 的顶层便捷字段。GGraphDB 会规范化标签并保存在兼容 1.0 的
fields map 内，因此不改变持久化实体布局，也可以使用 `labels CONTAINS
"production"` 查询。

## 关系类型和边

关系类型字段：

- `name`：关系类型名。
- `from_kind` / `to_kind`，或 `from_kinds` / `to_kinds`。
- `directed`：方向是否有语义。
- `cardinality`：`many_to_many`、`one_to_many`、`many_to_one`、
  `one_to_one`。
- `impact_direction`：影响分析的传播方向。

边以 `(type, from, to)` 作为规范身份。输入的 `edge.id` 作为来源别名
保留；GGraphDB 会将保存的边 ID 改写为稳定的规范 ID。

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

移动边端点时，应删除旧的 `(type, from, to)`，再创建新边。端点是身份，
不是可变字段。

关系属性 schema 与关系类型分开管理：

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

被引用的关系类型必须已存在。默认值和校验同时作用于 direct commit、ingest
batch 和文件导入批次；删除关系类型前先删除对应属性 schema。

## Source Policy

Source policy 按租户生效：

```sh
curl -sS -X PUT "$WRITER/v1/source-policy" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/source-policy.json
```

优先级规则：

- policy 中存在 `source` 时，policy priority 覆盖请求中的
  `source_priority`。
- policy 存在但 source 未知时，使用 `default_priority`。
- 没有 policy 时，使用请求的 `source_priority`。

字段优先级：

- `field_priorities` 可以为指定规范实体字段设置 source 的绝对有效优先级，
  不改变实体级 `source_priority`。
- `source + kind + field` 规则覆盖 source 全局字段规则。
- 字段别名处理后才计算字段优先级，因此配置规范字段名。
- 没有 `source` 的直接提交实体不使用字段优先级；ingest 实体先继承批次
  source，再应用字段优先级。

字段别名：

- `field_aliases` 在合并、索引、MD5 skip、查询、scan 和 export 前，把
  输入的顶层 `entity.fields` 名称映射为规范字段名。
- `source + kind` 规则优先于 source 全局回退规则。
- 没有 `source` 的直接提交实体不使用别名；ingest 实体先继承批次 source。
- 同一 payload 同时提供规范名和别名时，规范名优先；不同的别名值作为
  带有 `alias_field` 的 suppressed conflict 返回。
- 同一规范字段存在多个别名时，按别名字段名排序解决，结果确定。
- 别名不支持嵌套路径、通配、正则或值/类型转换；Query DSL 和 scan/export
  始终使用规范字段名。

字段合并：

- 高优先级覆盖低优先级；
- 优先级相同则比较更高的 `confidence`；
- 优先级和 confidence 都相同则使用最后写入；
- 低优先级写入被 suppressed，不会失败。

suppressed conflict 会出现在 commit 和 ingest 响应中，不会进入死信。

## 删除

管理员强制删除：

- `delete_entities`：实体 ID 列表；
- `delete_edges`：规范边 ID 或已知别名。

来源感知删除：

- `delete_entity_requests`
- `delete_edge_requests`

采集器应使用来源感知的边删除：

```json
{
  "type": "runs_on",
  "from": "service:api",
  "to": "host:aws:i-001",
  "source": "aws",
  "reason": "collector no longer observes relation"
}
```

低优先级删除不能移除高优先级边存在性，会返回 suppressed conflict。

## Ingestion Batch

请求结构：

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

支持的 item：

- `entity`
- `edge`
- `delete_entity`
- `delete_edge`
- `relation_type`
- `ci_type`
- `entity_type`（`ci_type` 的 1.1 别名）

响应字段：

- `applied`：已纳入 commit 的 item；
- `failed`：无效 item 或提交失败；
- `suppressed`：低优先级字段/删除冲突；
- `skipped`：幂等重放或 MD5 相同的图写入；
- `cursor`：返回的采集器 cursor；
- `failures`：item 级错误；
- `conflicts`：被抑制冲突和提交失败原因。

CMDB 采集场景的批次建议：

- 从每批 200 个逻辑 CMDB 组开始，对象存储和 writer 超时稳定后再接近 500；
- 优先扩大批次，而不是并发大量小批次；每批都有固定的 commit、manifest、
  幂等和采集器元数据开销；
- 如果 `batch_id` 已代表采集器 checkpoint，复用它作为 `idempotency_key`，
  writer 可以合并为一个 ingest 元数据对象；
- 429 后复用相同 `batch_id` 和 `idempotency_key`，指数退避加抖动重试；
- 从 200 提升到 500 组时，应同步增加 HTTP 超时，因为每组通常会展开为
  多个实体和边。

采集器状态：

```sh
curl -sS "$WRITER/v1/ingest/collectors/aws/collector-a" \
  -H 'X-Tenant-ID: demo'
```

死信：

```sh
curl -sS "$WRITER/v1/ingest/deadletters/aws" -H 'X-Tenant-ID: demo'
curl -sS -X POST "$WRITER/v1/ingest/deadletters/aws/replay?limit=10" -H 'X-Tenant-ID: demo'
```

## CSV 与 JSONL 导入

JSONL 的每个非空行都是一个已有的 ingestion item：

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

CSV 必须包含 `record_type`。entity/node 和 edge/relationship 行使用下方保留
列，其他非空列会成为带类型的属性；`labels` 支持 JSON 字符串数组或 `|`
分隔文本。

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

CSV `record_type` 支持 `entity`/`node`、`edge`/`relationship`、
`delete_entity`/`delete_node`、`delete_edge`/`delete_relationship`、
`entity_type`、`relation_type`。类型定义行把 JSON 放在 `payload` 或
`definition` 列。

接口返回 `202`、普通 `bulk_import` task 和 `Location` header。轮询
`/v1/tasks/{id}` 获取 checkpoint、进度、问题样本和最终计数。`format` 可由
JSONL/CSV Content-Type 推断；`batch_size` 默认 500、最大 5000；`on_error`
为 `abort` 或 `continue`。当前上传上限是 32 MiB，每个租户同时只运行一个
bulk import。

## MD5 Skip

commit 和 ingest 作用于当前图。如果结果 MD5 与当前已存图相同，GGraphDB
会跳过新 commit 并返回 `skipped=true`，避免重复采集导致 commit tail
增长。

## 写入背压

写入准入可能以 `429` 返回结构化原因：

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

采集器应遵守 `Retry-After`，使用相同 `idempotency_key` 重试；同一原因
反复出现时降低并发。

常见原因：

- 对象存储延迟或错误；
- manifest CAS 冲突；
- 索引重建或维护任务运行中；
- commit tail 过长；
- 租户实体/边/对象/字节配额超限。
