# 快速开始

[English](quickstart.md)

## 本地文件存储

启动单个本地进程：

```sh
go run ./cmd/graphdb serve
```

默认 API 地址为 `http://127.0.0.1:8080`。检查健康状态：

```sh
curl -sS http://127.0.0.1:8080/v1/health
```

## Docker Compose + MinIO

```sh
docker compose up --build
```

需要覆盖端口时：

```sh
MINIO_API_PORT=29000 MINIO_CONSOLE_PORT=29001 GRAPHDB_PORT=28080 docker compose up --build
```

## Docker Compose + RustFS

```sh
docker compose -f docker-compose.rustfs.yml up --build
```

默认端口：

- Writer：`http://127.0.0.1:38080`
- Reader：`http://127.0.0.1:38081`
- RustFS S3 API：`http://127.0.0.1:39000`

## 创建租户

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

CLI 等价命令：

```sh
go run ./cmd/graphdb create-tenant demo
```

## 写入数据

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/commits \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/commit.json
```

CLI 等价命令：

```sh
go run ./cmd/graphdb commit demo examples/commit.json
```

响应包含 `version`、`readable_version`、`skipped`、
`canonical_edges` 和可选的 `suppressed` 冲突信息。

## 查询数据

JSON DSL：

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/query \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/query-match.json
```

GQL：

```sh
curl -sS -X POST http://127.0.0.1:8080/v1/query/gql \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: text/plain' \
  --data-binary 'FIND person WHERE name = "Alice" LIMIT 10'
```

## 写入后读取

当 reader 必须追上某次写入时，把提交版本作为 `min_version`：

```sh
curl -sS 'http://127.0.0.1:8080/v1/entities/person:alice?min_version=1' \
  -H 'X-Tenant-ID: demo'
```

如果允许读取旧版本：

```sh
curl -sS 'http://127.0.0.1:8080/v1/entities/person:alice?allow_stale=true' \
  -H 'X-Tenant-ID: demo'
```
