# GGraphDB Python SDK

Version **2.5.1**, matching the [server release](../../docs/validation-v2.5.1.md).

Install locally:

```sh
python3 -m pip install -e sdk/python
```

Example:

```python
from graphdb_sdk import GraphDBClient

client = GraphDBClient("http://127.0.0.1:38080", tenant_id="demo")
result = client.commit({
    "upsert_entities": [
        {"id": "host:1", "kind": "host", "fields": {"hostname": "app-01"}}
    ]
}, idempotency_key="batch-001")
```

See [SDK user guide](../../docs/user/sdk.md) for the full user guide.

Since 2.3.4: consuming `stream_snapshot()` to EOF raises `EOFError` if the final
`done:true` marker is missing. Closing a stream early cancels consumption and does
not qualify the export as complete.

The 3.0 development branch adds `client.cypher(text, parameters, **options)` and
`client.stream_cypher(text, parameters, **options)`. Consume the stream through
EOF to detect failed or missing terminal frames. Example:

```python
result = client.cypher("MATCH (n:host) WHERE n.region=$region RETURN n.hostname AS hostname LIMIT 10", {"region": "cn-east"})
```

See the [read-only profile](../../docs/opencypher.md). These methods require the
development server and are not part of the 2.5.1 release contract.
