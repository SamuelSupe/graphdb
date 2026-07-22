# Go And Python SDK

[中文](sdk.zh-CN.md)

GraphDB provides lightweight Go and Python SDKs over the HTTP API. They do not
import service `internal` packages and are safe to vendor into collectors,
internal services, and operations tools.

SDK scope:

- tenant lifecycle basics.
- direct commits and ingestion batches.
- source policy and tenant config.
- entity lookup, list entities/edges, export streams.
- JSON Query DSL and GQL.
- saved query and running query control.
- task, index health/rebuild, reader freshness, writer lease, audit/repair.
- structured API errors with code and retry hints.

## Go SDK

Package:

```go
import graphdb "gitlab.jiagouyun.com/guance/graphdb/sdk/go/graphdb"
```

Create a client:

```go
client, err := graphdb.NewClient(
    "http://127.0.0.1:38080",
    graphdb.WithTenant("demo"),
)
if err != nil {
    return err
}
```

Use separate writer and reader clients when deployed separately:

```go
writer, _ := graphdb.NewClient("http://127.0.0.1:38080", graphdb.WithTenant("demo"))
reader, _ := graphdb.NewClient("http://127.0.0.1:38081", graphdb.WithTenant("demo"))
```

### Go: Direct Commit

```go
result, err := writer.Commit(ctx, graphdb.Mutations{
    UpsertCITypes: []graphdb.CIType{{
        Name: "host",
        Fields: map[string]graphdb.FieldSpec{
            "tags": {Type: "array", MergeStrategy: "append_unique"},
        },
    }},
    UpsertRelationTypes: []graphdb.RelationType{{
        Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
    }},
    UpsertEntities: []graphdb.Entity{
        {ID: "host:1", Kind: "host", Source: "agent", Fields: graphdb.Fields{"hostname": "app-01", "tags": []any{"agent"}}},
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

For array fields declared with `MergeStrategy: "append_unique"`, repeated
writes append unique elements. Use a `!` suffix to force replace in one write:

```go
_, err = writer.Commit(ctx, graphdb.Mutations{
    UpsertEntities: []graphdb.Entity{{
        ID: "host:1", Kind: "host",
        Fields: graphdb.Fields{"tags!": []any{"manual"}},
    }},
}, nil)
```

### Go: Ingestion

```go
result, err := writer.Ingest(ctx, graphdb.IngestRequest{
    Source: "aws", CollectorID: "collector-a",
    BatchID: "aws-001", IdempotencyKey: "aws-001", Cursor: "cursor-002",
    Items: []graphdb.IngestItem{{
        ExternalID: "i-001",
        Entity: &graphdb.Entity{
            ID: "host:aws:i-001", Kind: "host",
            Fields: graphdb.Fields{"hostname": "app-01"},
        },
    }},
})
```

### Go: Query

```go
response, err := reader.Query(ctx, graphdb.QueryRequest{
    Op: "match", Kind: "host",
    Where: []graphdb.Filter{{Field: "hostname", Op: "prefix", Value: "app-"}},
    Project: []string{"id", "hostname"},
    Limit: 100,
    MinVersion: result.Version,
})
```

GQL:

```go
response, err := reader.GQL(ctx, `FIND host WHERE hostname PREFIX "app-" LIMIT 100`)
```

Streaming:

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

### Go: Error Handling

```go
_, err = writer.Commit(ctx, mutations, &graphdb.CommitOptions{IdempotencyKey: key})
var apiErr *graphdb.APIError
if errors.As(err, &apiErr) {
    if apiErr.StatusCode == 429 && apiErr.RetryAfter > 0 {
        time.Sleep(apiErr.RetryAfter)
        // retry with the same idempotency key
    }
}
```

## Python SDK

Install from the repo:

```sh
python3 -m pip install -e sdk/python
```

Create a client:

```python
from graphdb_sdk import GraphDBClient

writer = GraphDBClient("http://127.0.0.1:38080", tenant_id="demo")
reader = GraphDBClient("http://127.0.0.1:38081", tenant_id="demo")
```

### Python: Direct Commit

```python
result = writer.commit(
    {
        "upsert_entities": [
            {
                "id": "host:1",
                "kind": "host",
                "source": "agent",
                "fields": {"hostname": "app-01"},
            }
        ]
    },
    idempotency_key="batch-001",
)
print(result["version"], result.get("skipped"))
```

### Python: Ingestion

```python
result = writer.ingest({
    "source": "aws",
    "collector_id": "collector-a",
    "batch_id": "aws-001",
    "idempotency_key": "aws-001",
    "cursor": "cursor-002",
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
})
```

### Python: Query

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

GQL:

```python
response = reader.gql('FIND host WHERE hostname PREFIX "app-" LIMIT 100')
```

Streaming:

```python
with reader.stream_gql("FIND host LIMIT 1000") as stream:
    for item in stream:
        print(item)
```

### Python: Error Handling

```python
from graphdb_sdk import GraphDBAPIError

try:
    writer.commit(mutations, idempotency_key="batch-001")
except GraphDBAPIError as err:
    if err.status_code == 429 and err.retry_after_ms:
        # sleep and retry with the same idempotency key
        pass
```

## Source Policy Example

Go:

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

Python:

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

## Operational Calls

Go:

```go
health, _ := reader.IndexHealth(ctx, false)
task, _ := writer.StartTask(ctx, "compact", nil)
freshness, _ := reader.ReaderFreshness(ctx)
```

Python:

```python
health = reader.index_health()
task = writer.start_task("compact")
freshness = reader.reader_freshness()
```

## Retry Guidance

For writes and ingestion:

- Always set `idempotency_key`.
- On `429`, honor SDK retry hints and retry the same payload with the same key.
- On `idempotency_conflict`, do not retry with the same key unless the payload
  is exactly the original payload.
- Suppressed source-priority conflicts are returned in the successful response;
  they are not SDK exceptions.
