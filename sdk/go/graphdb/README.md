# GGraphDB Go SDK

Import path inside this repository:

```go
import graphdb "gitlab.jiagouyun.com/guance/graphdb/sdk/go/graphdb"
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

See [../../docs/user/sdk.md](../../../docs/user/sdk.md) for the full user
guide.
