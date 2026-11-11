# Read-only openCypher in the 3.0 development branch

This is a bounded openCypher 9 query profile, based on grammar/TCK **1.0.0-M23**,
not a 3.0 release qualification or a claim of full Neo4j/Bolt compatibility.
HTTP `/v1` and Go module `/v2` retain their independent version contracts.

Use `POST /v1/query/cypher` or `POST /v1/query/cypher/stream`, with existing
authentication and `X-Tenant-ID`. Send `query`, `parameters`, and optionally
`min_version`, `allow_stale`, `timeout_ms`, `cost_limit`, `profile`. Parameters
are bound values; signed int64 JSON parameters retain their precision. The
response contains `version`, ordered `columns`, `rows`, and `stats`.

```sh
curl -sS "$READER/v1/query/cypher" \
  -H 'Content-Type: application/json' -H 'X-Tenant-ID: demo' \
  --data-binary @examples/query-cypher.json
```

Supported: multiple MATCH clauses, shared bindings, OPTIONAL MATCH, fixed and
explicitly bounded variable-length patterns, WITH, UNWIND, RETURN, DISTINCT,
grouping/aggregation, ORDER BY, SKIP/LIMIT, UNION/UNION ALL, CASE, lists/maps,
comprehensions, quantifiers, pattern predicates/comprehensions and pattern-form
EXISTS. A statement uses one UNION mode. After a DISTINCT or grouped projection,
ORDER BY and WITH's WHERE can use projected values. Local comprehension and
pattern variables remain visible inside their expressions. Sort aggregates
containing pattern expressions by their projected aliases. Shortest-path functions require pre-bound endpoints and a single
explicitly bounded variable-length relationship in a separate MATCH clause.
EXPLAIN and PROFILE are available. All updates and procedure calls are rejected
before graph reads. Temporal/spatial types, non-finite numbers, general
subqueries and unlisted functions are outside this profile.

Nodes map to Entity; labels are the union of kind and attached labels. Properties
map to business Fields. `graphdb.id(n)` exposes the existing string identity;
`n.id` reads a business property, and standard integer `id()` is unsupported.
Edges retain their canonical `(type, from, to)` identity. Queries observe one
snapshot; min_version is a freshness floor. Missing properties read as null;
predicates use three-valued logic, string matching is case-sensitive, duplicate
rows are retained unless DISTINCT is requested, and relationship uniqueness is
scoped to each MATCH. `keys()` and `properties()` preserve ordinary map keys.
`toInteger()` returns null for an integer string outside the signed 64-bit range.
A missing candidate index page fails a point lookup with `index_stale`.

Limits: 64 KiB query text, 8192 tokens, structural nesting 32, 1024 variable slots,
32 UNION branches, 64 clauses per branch, path length 16, default/max cost
100000/1000000, conservative cumulative executor allocation budget 64 MiB, 100000 buffered
rows/list elements/streamed rows, 10000 aggregation groups, 1000 JSON output rows.
The list limit also covers list parameters, concatenation, and results from
`split()` and `keys()`. Exceeding a limit fails the query. Blocking
operators buffer within these limits; sorting currently keeps all candidates,
without Top-K or disk spill. Existing persisted floats retain their original
precision; lost integer precision cannot be reconstructed.

Streaming emits a meta object, `{ "row": [...] }` objects, then a terminal object
with `done:true` and `success:true`. A failed terminal uses `success:false` and an
error envelope. A disconnected or timed-out connection may omit the terminal.
HTTP 200 alone never establishes completion. Go `CypherStream` and Python
`stream_cypher` detect failures and missing completion when consumed through EOF.
Go dynamic numeric cells use json.Number. Each successful Cypher stream
`Next` replaces the destination with the current frame. Ordinary tenant queries participate
in the local running-query registry; distributed graphs retain their existing
request/connection cancellation rather than adding a cross-process registry.

The pinned TCK extraction covers empty/any-graph expressions and selected read
clauses, not the full TCK. Passing, unsupported, adapter-excluded and
resource-limited scenarios are reported separately. The full graph-fixture and
error-category adapters remain future compatibility work. See the
[full Chinese contract](opencypher.zh-CN.md), [validation record](opencypher-validation.zh-CN.md),
and [parser provenance](../internal/query/cypherparser/README.md).
