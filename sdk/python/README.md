# GGraphDB Python SDK

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

`client.ingest()` waits for `committed`; use `client.accept_ingest()` plus
`client.get_ingest_batch_status()` for the default asynchronous WAL path.

See [../../docs/user/sdk.md](../../docs/user/sdk.md) for the full user guide.
