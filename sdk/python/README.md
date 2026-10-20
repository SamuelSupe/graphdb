# GGraphDB Python SDK

Version **2.3.7**, matching the [server release](../../docs/validation-v2.3.7.md).

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
