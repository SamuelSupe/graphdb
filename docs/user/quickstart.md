# Quick Start

[中文](quickstart.zh-CN.md)

## Local File Storage

Start one local process:

```sh
go run ./cmd/graphdb serve
```

The default API address is `http://127.0.0.1:8080`.

Check health:

```sh
curl -sS http://127.0.0.1:8080/v1/health
```

## Docker Compose With MinIO

```sh
docker compose up --build
```

Override ports if needed:

```sh
MINIO_API_PORT=29000 MINIO_CONSOLE_PORT=29001 GRAPHDB_PORT=28080 docker compose up --build
```

## Docker Compose With RustFS

```sh
docker compose -f docker-compose.rustfs.yml up --build
```

Default ports:

- Writer: `http://127.0.0.1:38080`
- Reader: `http://127.0.0.1:38081`
- RustFS S3 API: `http://127.0.0.1:39000`

## Create A Tenant

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

CLI equivalent:

```sh
go run ./cmd/graphdb create-tenant demo
```

## Write Data

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/commit.json
```

CLI equivalent:

```sh
go run ./cmd/graphdb commit demo examples/commit.json
```

The response contains `version`, `readable_version`, `skipped`,
`canonical_edges`, and optional `suppressed` conflicts.

## Query Data

JSON DSL:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/query-match.json
```

GQL:

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/query/gql \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: text/plain' \
  --data-binary 'FIND person WHERE name = "Alice" LIMIT 10'
```

## Read After Write

Use the committed version as `min_version` when a reader must catch up:

```sh
curl -sS 'http://127.0.0.1:8080/v1/entities/person:alice?min_version=1' \
  -H 'X-Tenant-ID: demo'
```

If stale reads are acceptable:

```sh
curl -sS 'http://127.0.0.1:8080/v1/entities/person:alice?allow_stale=true' \
  -H 'X-Tenant-ID: demo'
```
