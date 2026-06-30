# Tenant And Config

Tenant lifecycle APIs are not tenant-header scoped. Data APIs remain scoped by
`X-Tenant-ID`.

## Lifecycle

Create:

```sh
curl -sS -X POST "$WRITER/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "demo",
    "name": "Demo",
    "labels": {"env": "dev"}
  }'
```

List:

```sh
curl -sS "$WRITER/v1/tenants"
curl -sS "$WRITER/v1/tenants?include_legacy=true"
```

Get:

```sh
curl -sS "$WRITER/v1/tenants/demo"
```

Update metadata:

```sh
curl -sS -X PUT "$WRITER/v1/tenants/demo" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Demo CMDB","labels":{"env":"prod"}}'
```

Disable and enable:

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/disable"
curl -sS -X POST "$WRITER/v1/tenants/demo/enable"
```

Soft delete:

```sh
curl -sS -X DELETE "$WRITER/v1/tenants/demo"
```

Purge object data after soft delete:

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/purge?force=true"
```

Clone:

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/clone" \
  -H 'Content-Type: application/json' \
  -d '{"target_tenant_id":"demo-repro","name":"Demo Repro"}'
```

## Source Policy

Get:

```sh
curl -sS "$WRITER/v1/source-policy" -H 'X-Tenant-ID: demo'
```

Put:

```sh
curl -sS -X PUT "$WRITER/v1/source-policy" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/source-policy.json
```

Recommended baseline:

```json
{
  "default_priority": 0,
  "sources": [
    {"name": "manual", "priority": 1000},
    {"name": "agent", "priority": 100},
    {"name": "cloud", "priority": 50},
    {"name": "aws", "priority": 50}
  ]
}
```

## Tenant Config

Get:

```sh
curl -sS "$WRITER/v1/tenant-config" -H 'X-Tenant-ID: demo'
```

Put:

```sh
curl -sS -X PUT "$WRITER/v1/tenant-config" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/tenant-config.json
```

Config sections:

- `backpressure`: object latency threshold, CAS conflict window, commit tail
  threshold, retry hint.
- `quota`: max entities and max edges for the tenant.
- `maintenance`: auto compact, GC interval, retention, small-file thresholds,
  index orphan cleanup.
- `indexes`: auto rebuild behavior.

`0` usually means no quota or disabled threshold, depending on the field.

## Tenant Usage

```sh
curl -sS "$WRITER/v1/tenant-usage" -H 'X-Tenant-ID: demo'
```

Use tenant usage to watch object count, byte count, and category growth before
enabling hard quota.

## CLI

```sh
go run ./cmd/graphdb list-tenants
go run ./cmd/graphdb tenant demo
go run ./cmd/graphdb create-tenant demo
go run ./cmd/graphdb set-tenant-metadata demo metadata.json
go run ./cmd/graphdb disable-tenant demo
go run ./cmd/graphdb enable-tenant demo
go run ./cmd/graphdb delete-tenant demo
go run ./cmd/graphdb purge-tenant demo --force
go run ./cmd/graphdb clone-tenant demo demo-repro
go run ./cmd/graphdb source-policy demo
go run ./cmd/graphdb set-source-policy demo examples/source-policy.json
go run ./cmd/graphdb tenant-config demo
go run ./cmd/graphdb set-tenant-config demo examples/tenant-config.json
go run ./cmd/graphdb tenant-usage demo
```
