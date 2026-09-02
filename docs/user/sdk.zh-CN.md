# Go 与 Python SDK

[English](sdk.md)

GGraphDB 提供基于 HTTP API 的轻量 Go 和 Python SDK。SDK 不导入服务端
`internal` 包，可以安全地 vendoring 到采集器、内部服务和运维工具。

1.3 SDK 已对齐当前 ingest 合同。两套 SDK 都保留 direct 模式的终态
`200/207` 结果，并提供 WAL 的 `202` acceptance、`Location`/owner status
资源、轮询/等待，以及 ingest CAS、条件和 atomic 选项。Go 和 Python SDK
包版本均为 `1.3.1`。

SDK 覆盖：

- 租户生命周期基础操作；
- 直接 commit、批量 ingest 和 CSV/JSONL 导入；
- 实体类型别名和关系属性 schema；
- source policy 和 tenant config；
- 实体查询、实体/边列表和导出流；
- GraphQL 和 JSON Query DSL；
- saved query 和运行中查询控制；
- task、索引健康/重建、reader freshness、writer lease、审计/修复；
- 带错误码和重试提示的结构化 API 错误。

## Go SDK

包：

```go
import graphdb "gitlab.jiagouyun.com/guance/graphdb/sdk/go/graphdb"
```

创建客户端：

```go
client, err := graphdb.NewClient(
    "https://graphdb.example.com",
    graphdb.WithTenant("demo"),
    graphdb.WithBearerToken(token),
)
if err != nil {
    return err
}
```

读写分离时使用不同客户端：

```go
writer, _ := graphdb.NewClient("http://127.0.0.1:38080", graphdb.WithTenant("demo"))
reader, _ := graphdb.NewClient("http://127.0.0.1:38081", graphdb.WithTenant("demo"))
```

### Go：直接 Commit

```go
result, err := writer.Commit(ctx, graphdb.Mutations{
    UpsertEntityTypes: []graphdb.EntityType{{
        Name: "host",
        Fields: map[string]graphdb.FieldSpec{
            "tags": {Type: "array", MergeStrategy: "append_unique"},
        },
    }},
    UpsertRelationTypes: []graphdb.RelationType{{
        Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
    }},
    UpsertEntities: []graphdb.Entity{
        {ID: "host:1", Kind: "host", Labels: []string{"asset"}, Source: "agent", Fields: graphdb.Fields{"hostname": "app-01", "tags": []any{"agent"}}},
        {ID: "service:api", Kind: "service", Source: "manual", Fields: graphdb.Fields{"name": "api"}},
    },
    UpsertEdges: []graphdb.Edge{{
        ID: "collector-edge-1", Type: "runs_on", From: "service:api", To: "host:1",
    }},
}, &graphdb.CommitOptions{IdempotencyKey: "batch-001"})
if err != nil {
    return err
}
fmt.Println(result.Version, result.Skipped, result.Suppressed)
```

使用 `MergeStrategy: "append_unique"` 的数组字段会追加不重复值；加 `!`
强制某次写入替换：

```go
_, err = writer.Commit(ctx, graphdb.Mutations{
    UpsertEntities: []graphdb.Entity{{
        ID: "host:1", Kind: "host",
        Fields: graphdb.Fields{"tags!": []any{"manual"}},
    }},
}, nil)
```

### Go：Ingestion

`Ingest` 是兼容性便利调用：它发送 `Prefer: wait=committed`，无论服务端直接
完成还是先返回 WAL `202`，都等待并返回终态 `IngestResult`。需要版本保护、
条件 mutation 或 all-or-nothing 行为时，在请求中加入事务选项：

```go
expectedVersion := int64(42)
request := graphdb.IngestRequest{
    Source: "aws", CollectorID: "collector-a",
    BatchID: "aws-001", IdempotencyKey: "aws-001", Cursor: "cursor-002",
    ExpectedVersion: &expectedVersion,
    FailureMode: "best_effort",
    Preconditions: []graphdb.IngestPrecondition{
        {ResourceType: "entity", ID: "host:aws:i-001", Op: "exists"},
        {ResourceType: "entity", ID: "host:aws:i-001", Field: "state", Op: "eq", Value: "ready"},
    },
    Items: []graphdb.IngestItem{{
        ExternalID: "i-001",
        Entity: &graphdb.Entity{
            ID: "host:aws:i-001", Kind: "host",
            Fields: graphdb.Fields{"hostname": "app-01"},
        },
    }},
}
result, err := writer.Ingest(ctx, request)
if err != nil {
    return err
}
fmt.Println(result.Version, result.ErrorCode, result.Applied, result.Failed)
```

需要非阻塞地接收 WAL 时使用 `SubmitIngest`，并保存返回的 owner URL。direct
模式会在 `Result` 中返回终态结果（状态为 `200` 或 `207`）；WAL 模式会在
`Accepted` 中返回 durable acceptance（状态为 `202`）。`SubmitIngest` 优先
读取 `status_url`，没有时使用 HTTP `Location` header，因此可以把状态请求
路由给 owner writer：

```go
submission, err := writer.SubmitIngest(ctx, request)
if err != nil {
    return err
}
if submission.StatusCode == 202 {
    accepted := submission.Accepted
    status, err := writer.WaitIngest(ctx, accepted.StatusURL, &graphdb.IngestWaitOptions{
        PollInterval: 250 * time.Millisecond,
    })
    if err != nil {
        return err
    }
    fmt.Println(status.State, status.Result.Version)
} else {
    fmt.Println(submission.StatusCode, submission.Result.Version)
}
```

需要自行控制轮询时，`GetIngestStatus` 只执行一次状态读取。终态为
`committed` 和 `failed`；中间状态包括 `accepted`、`prepared`、`published`、
`retrying`。`202` 只表示 writer 已持久接管，不表示图版本已经提交。终态条件
失败会在 `IngestResult.ErrorCode` 中返回：`version_conflict`、
`precondition_failed`、`atomic_validation_failed` 或 `atomic_suppressed`。

### Go：Schema 与文件导入（兼容 1.1）

```go
catalog, err := writer.PutRelationSchema(ctx, graphdb.RelationSchema{
    RelationType: "cites",
    Strict: true,
    Fields: map[string]graphdb.FieldSpec{
        "confidence": {Type: "number", Required: true},
    },
})
if err != nil {
    return err
}
fmt.Println(catalog.Revision)

task, err := writer.StartImport(ctx, strings.NewReader(jsonl), graphdb.ImportOptions{
    Format: "jsonl", Source: "knowledge-base", CollectorID: "files",
    BatchSize: 500, OnError: "continue",
})
```

使用 `ListEntityTypes`、`ListRelationSchemas` 和普通 task 方法查看类型元数据、
schema 和导入进度。

### Go：查询

```go
response, err := reader.Query(ctx, graphdb.QueryRequest{
    Op: "match", Kind: "host",
    Where: []graphdb.Filter{{Field: "hostname", Op: "prefix", Value: "app-"}},
    Project: []string{"id", "hostname"},
    Limit: 100,
    MinVersion: result.Version,
})
```

GraphQL：

```go
response, err := reader.GraphQL(ctx, graphdb.GraphQLRequest{
    Query: `query Find($request: QueryRequest!) {
        graph(request: $request) { version results stats }
    }`,
    OperationName: "Find",
    Variables: map[string]any{
        "request": map[string]any{"op": "match", "kind": "host", "limit": 100},
    },
})
```

已弃用的 `GQL` 和 `GQLStream` 方法保留 1.0 `FIND`/`MATCH` 文本 DSL，
它们不是 GraphQL。旧 NDJSON 示例：

```go
stream, err := reader.GQLStream(ctx, `FIND host LIMIT 1000`)
if err != nil {
    return err
}
defer stream.Close()
for {
    var item map[string]any
    if !stream.Next(&item) {
        break
    }
    fmt.Println(item)
}
if err := stream.Err(); err != nil {
    return err
}
```

### Go：错误处理

```go
_, err = writer.Commit(ctx, mutations, &graphdb.CommitOptions{IdempotencyKey: key})
var apiErr *graphdb.APIError
if errors.As(err, &apiErr) {
    if apiErr.StatusCode == 429 && apiErr.RetryAfter > 0 {
        time.Sleep(apiErr.RetryAfter)
        // 使用相同幂等键重试
    }
}
```

## Python SDK

从仓库安装：

```sh
python3 -m pip install -e sdk/python
```

创建客户端：

```python
from graphdb_sdk import GraphDBClient

writer = GraphDBClient("https://graphdb.example.com", tenant_id="demo", bearer_token=token)
reader = GraphDBClient("https://graphdb.example.com", tenant_id="demo", bearer_token=token)
```

### Python：直接 Commit

```python
result = writer.commit(
    {
        "upsert_entity_types": [{"name": "host"}],
        "upsert_entities": [
            {
                "id": "host:1",
                "kind": "host",
                "labels": ["asset"],
                "source": "agent",
                "fields": {"hostname": "app-01"},
            }
        ]
    },
    idempotency_key="batch-001",
)
print(result["version"], result.get("skipped"))
```

### Python：Ingestion

```python
batch = {
    "source": "aws",
    "collector_id": "collector-a",
    "batch_id": "aws-001",
    "idempotency_key": "aws-001",
    "cursor": "cursor-002",
    "expected_version": 42,
    "failure_mode": "best_effort",
    "preconditions": [
        {"resource_type": "entity", "id": "host:aws:i-001", "op": "exists"},
        {"resource_type": "entity", "id": "host:aws:i-001", "field": "state", "op": "eq", "value": "ready"},
    ],
    "items": [
        {
            "external_id": "i-001",
            "entity": {
                "id": "host:aws:i-001",
                "kind": "host",
                "fields": {"hostname": "app-01"},
            },
        }
    ],
}
result = writer.ingest(batch)
print(result["version"], result.get("error_code"), result["applied"], result["failed"])
```

`ingest` 是阻塞式兼容便利调用：服务端先以 `202` 确认 WAL 请求时，它会等待
终态结果。需要显式接收并轮询时，使用 `submit_ingest`，保存返回的
`status_url`/owner 信息，再调用 `get_ingest_status` 或 `wait_ingest`。direct
模式返回 HTTP `200` 或 `207` 的终态结果；WAL 模式返回 HTTP `202` acceptance。
终态响应可能包含 `error_code`：`version_conflict`、`precondition_failed`、
`atomic_validation_failed` 或 `atomic_suppressed`。

### Python：Schema 与文件导入（兼容 1.1）

```python
catalog = writer.put_relation_schema("cites", {
    "strict": True,
    "fields": {"confidence": {"type": "number", "required": True}},
})

task = writer.start_import(
    jsonl_data,
    "jsonl",
    source="knowledge-base",
    collector_id="files",
    batch_size=500,
    on_error="continue",
)
```

使用 `list_entity_types()`、`list_relation_schemas()` 和
`get_task(task["id"])` 查看元数据和导入进度。

### Python：查询

```python
response = reader.query({
    "op": "match",
    "kind": "host",
    "where": [{"field": "hostname", "op": "prefix", "value": "app-"}],
    "project": ["id", "hostname"],
    "limit": 100,
    "min_version": result["version"],
})
```

GraphQL：

```python
response = reader.graphql(
    "query Find($request: QueryRequest!) { graph(request: $request) { version results stats } }",
    {"request": {"op": "match", "kind": "host", "limit": 100}},
    "Find",
)
```

已弃用的 `gql` 和 `stream_gql` 方法保留 1.0 文本 DSL。旧 NDJSON 示例：

```python
with reader.stream_gql("FIND host LIMIT 1000") as stream:
    for item in stream:
        print(item)
```

### Python：错误处理

```python
from graphdb_sdk import GraphDBAPIError

try:
    writer.commit(mutations, idempotency_key="batch-001")
except GraphDBAPIError as err:
    if err.status_code == 429 and err.retry_after_ms:
        # 休眠后使用相同幂等键重试
        pass
```

## Source Policy 示例

Go：

```go
_, err := writer.PutSourcePolicy(ctx, graphdb.SourcePolicy{
    DefaultPriority: 0,
    Sources: []graphdb.SourcePolicyItem{
        {Name: "manual", Priority: 1000},
        {Name: "agent", Priority: 100},
        {Name: "aws", Priority: 50},
    },
    FieldAliases: []graphdb.FieldAliasRule{
        {
            Source: "aws",
            Kind: "host",
            Aliases: map[string]string{
                "privateIpAddress": "private_ip",
                "instanceName": "hostname",
            },
        },
    },
    FieldPriorities: []graphdb.FieldPriorityRule{
        {
            Source: "aws",
            Kind: "host",
            Fields: map[string]int{"hostname": 1200, "private_ip": 900},
        },
    },
})
```

Python：

```python
writer.put_source_policy({
    "default_priority": 0,
    "sources": [
        {"name": "manual", "priority": 1000},
        {"name": "agent", "priority": 100},
        {"name": "aws", "priority": 50},
    ],
    "field_aliases": [
        {
            "source": "aws",
            "kind": "host",
            "aliases": {
                "privateIpAddress": "private_ip",
                "instanceName": "hostname",
            },
        },
    ],
    "field_priorities": [
        {
            "source": "aws",
            "kind": "host",
            "fields": {"hostname": 1200, "private_ip": 900},
        },
    ],
})
```

## 运维调用

Go：

```go
health, _ := reader.IndexHealth(ctx, false)
task, _ := writer.StartTask(ctx, "compact", nil)
freshness, _ := reader.ReaderFreshness(ctx)
```

Python：

```python
health = reader.index_health()
task = writer.start_task("compact")
freshness = reader.reader_freshness()
```

## 重试建议

写入和采集：

- 始终设置 `idempotency_key`；
- 429 时遵守 SDK 重试提示，用相同 payload 和相同 key 重试；
- 将 WAL `202` 视为 durable takeover，而不是已提交图版本；在将 mutation
  视为 committed 前轮询 owner 路由的 status URL（或使用阻塞式 ingest helper）；
- `idempotency_conflict` 表示 payload 不同，除非 payload 与原请求完全一致，
  不要继续使用同一个 key；
- 被抑制的 source-priority 冲突会在成功响应中返回，不会成为 SDK 异常。
