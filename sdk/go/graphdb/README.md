# GGraphDB Go SDK

Version **2.6.5**, matching the [server release](../../../release/local-disk.md).

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

Consume query, GQL, entity, edge and snapshot streams through EOF and check `Stream.Err()`. A missing final completion marker returns `io.ErrUnexpectedEOF`. A terminal error frame is available to the caller and sets an `APIError`. Closing early cancels consumption.
