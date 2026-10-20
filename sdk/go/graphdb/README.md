# GGraphDB Go SDK

Version **2.3.8**, matching the [server release](../../../docs/validation-v2.3.8.md).

Import path inside this repository:

```go
import graphdb "github.com/SamuelSupe/graphdb/v2/sdk/go/graphdb"
```

Example:

```go
client, err := graphdb.NewClient("http://127.0.0.1:38080", graphdb.WithTenant("demo"))
if err != nil {
    panic(err)
}

result, err := client.Commit(ctx, graphdb.Mutations{
    UpsertEntities: []graphdb.Entity{{
        ID: "host:1", Kind: "host", Fields: graphdb.Fields{"hostname": "app-01"},
    }},
}, &graphdb.CommitOptions{IdempotencyKey: "batch-001"})
```

See [SDK user guide](../../../docs/user/sdk.md) for the full user
guide.

Since 2.3.4: consume snapshot streams through EOF and check `Stream.Err()`; a
missing final `done:true` is reported as `io.ErrUnexpectedEOF`. Early `Close()`
remains a caller cancellation, not a successful export.
