# GGraphDB Python SDK

Version **2.6.5**, matching the [server release](../../release/local-disk.md).

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

Consume query, GQL, entity, edge and snapshot streams to the end. A missing final completion marker raises `EOFError`. Iteration yields a terminal error frame and then raises `GraphDBAPIError`. Closing early cancels consumption.
