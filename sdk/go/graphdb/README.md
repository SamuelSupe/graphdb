# GGraphDB Go SDK

Version **2.5.1**, matching the [server release](../../../docs/validation-v2.5.1.md).

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

The 3.0 development branch additionally provides `Client.Cypher(ctx,
CypherRequest{Query: "MATCH (n:host) RETURN n.hostname AS hostname LIMIT 10"})`
and `Client.CypherStream`. Dynamic numeric cells are `json.Number`; consume a
stream through EOF and check `Stream.Err()` for failed or missing terminal
frames. Each `Next` replaces the destination with the current Cypher frame.
See the [read-only profile](../../../docs/opencypher.md). These methods
require the development server and are not a 2.5.1 release capability.
