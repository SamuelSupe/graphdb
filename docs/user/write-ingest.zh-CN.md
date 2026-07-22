# 写入与采集

[English](write-ingest.md)

GraphDB 有两条写入路径：

- `POST /v1/commits`：直接原子图变更。
- `POST /v1/ingest/batches`：面向采集器的批量写入，支持 source、cursor、
  幂等、部分失败、死信和采集器状态。

两者都需要 `X-Tenant-ID`；在 reader 模式都返回 `405`。

## 直接提交

请求结构：

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
- `kind`：CI 类型，例如 `host`、`service`、`database`。
- `fields`：无模式 JSON 对象。
- `source`、`external_id`、`confidence`、`source_priority`：来源元数据。
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

## 关系类型和边

关系类型字段：

- `name`：关系类型名。
- `from_kind` / `to_kind`，或 `from_kinds` / `to_kinds`。
- `directed`：方向是否有语义。
- `cardinality`：`many_to_many`、`one_to_many`、`many_to_one`、
  `one_to_one`。
- `impact_direction`：影响分析的传播方向。

边以 `(type, from, to)` 作为规范身份。输入的 `edge.id` 作为来源别名
保留；GraphDB 会将保存的边 ID 改写为稳定的规范 ID。

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

响应字段：

- `applied`：已纳入 commit 的 item；
- `failed`：无效 item 或提交失败；
- `suppressed`：低优先级字段/删除冲突；
- `skipped`：幂等重放或 MD5 相同的图写入；
- `cursor`：返回的采集器 cursor；
- `failures`：item 级错误；
- `conflicts`：被抑制冲突和提交失败原因。

采集批次建议：

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

## MD5 Skip

commit 和 ingest 作用于当前图。如果结果 MD5 与当前已存图相同，GraphDB
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
      "threshold": 300,
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
