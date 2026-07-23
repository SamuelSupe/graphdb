# GGraphDB naming and 1.0 compatibility

The public product name is **GGraphDB**.

The canonical text protocol name is **GraphQL**, served by
`POST /v1/query/graphql`. It accepts GraphQL documents and variables and
returns a GraphQL `data`/`errors` envelope. The GraphQL schema and 1.1 limits
are documented in [graphql.md](graphql.md).

The following technical identifiers remain unchanged in 1.1 to avoid breaking
1.0 deployments and clients:

| Compatibility identifier | 1.1 policy |
| --- | --- |
| `graphdb` binary and repository/module paths | Retained. |
| `GRAPHDB_*` environment variables | Retained. |
| `X-GraphDB-*` response and control headers | Retained. |
| `graphdb` object prefixes and layout keys | Retained. |
| Go package `graphdb` and Python `GraphDBClient` | Retained. |
| `/v1/query/gql`, `graphdb gql`, SDK `GQL`/`gql` | Deprecated 1.0 text-DSL aliases; not GraphQL. |

New public documentation must not call the `FIND`/`MATCH` compatibility DSL
“GQL”. That name is reserved neither as a GGraphDB language nor as an
abbreviation for the legacy syntax.
