# 部署与运维

[English](deploy-ops.md)

## 运行模式

```sh
GRAPHDB_MODE=all|writer|reader
```

- `all`：开发或小规模部署的单进程模式。
- `writer`：生产写入和控制进程。
- `reader`：读取和查询进程。

默认 `GRAPHDB_COORDINATION=local` 时，生产环境每个租户只能有一个活跃
writer，writer lease 和 manifest CAS 用于防止重复或陈旧 writer。
`GRAPHDB_COORDINATION=postgres` 改用 PostgreSQL head CAS，支持每租户
2–8 个乐观并发 writer。reader 相互独立；本地模式以对象存储为权威，
PostgreSQL 模式以 PG head 为权威。

## 对象存储

本地文件：

```sh
GRAPHDB_STORAGE=local
GRAPHDB_DATA_DIR=.graphdb
```

S3 兼容存储：

```sh
GRAPHDB_STORAGE=s3
S3_ENDPOINT=http://127.0.0.1:39000
S3_BUCKET=graphdb
S3_PATH_STYLE=true
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=graphdbadmin
S3_SECRET_ACCESS_KEY=graphdbsecret
GRAPHDB_PREFIX=graphdb
```

每个租户位于：

```text
<GRAPHDB_PREFIX>/tenants/<tenant-id>/
```

## 重要环境变量

服务：

- `GRAPHDB_ADDR=:8080`
- `GRAPHDB_ADMIN_ADDR=`（空值保持 1.0 兼容的合并 listener）
- `GRAPHDB_PPROF_ENABLED=false`（启用时必须使用独立管理 listener）
- `GRAPHDB_PREFIX=graphdb`
- `GRAPHDB_POLL_INTERVAL=2s`
- `GRAPHDB_INSTANCE_ID=<stable-instance-name>`

查询准入：

- `GRAPHDB_QUERY_MAX_CONCURRENT=64`
- `GRAPHDB_QUERY_MAX_PER_TENANT=32`
- `GRAPHDB_QUERY_QUEUE_TIMEOUT=5s`

读取路径：

- `GRAPHDB_READ_MAX_CONCURRENT=128`
- `GRAPHDB_READ_MAX_PER_TENANT=64`
- `GRAPHDB_READ_QUEUE_TIMEOUT=500ms`
- `GRAPHDB_READ_OBJECT_MAX_CONCURRENT=128`
- `GRAPHDB_READ_OBJECT_SINGLEFLIGHT=true`
- `GRAPHDB_PARQUET_DECODE_MAX_CONCURRENT=2`
- `GRAPHDB_READER_INDEX_CACHE_MAX_BYTES=256MiB`
- `GRAPHDB_READER_CATCHUP_TIMEOUT=2s`

写入路径：

- `GRAPHDB_WRITE_MAX_CONCURRENT=32`
- `GRAPHDB_WRITE_MAX_PER_TENANT=1`
- `GRAPHDB_WRITE_QUEUE_TIMEOUT=2s`
- `GRAPHDB_WRITE_EXECUTION_TIMEOUT=90s`
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=20000`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=4GiB`
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB`
- `GRAPHDB_INDEX_ENTITY_RECORDS=false`
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true`
- `GRAPHDB_INGEST_MODE=wal|direct`（本地 writer 默认 `wal`；PostgreSQL 必须显式 `direct`）
- `GRAPHDB_INGEST_WAL_DURABILITY=sync`
- `GRAPHDB_INGEST_METADATA_MODE=segment|legacy`（WAL 默认 `segment`）
- `GRAPHDB_INGEST_QUEUE_HIGH_WATERMARK=80`
- `GRAPHDB_INGEST_WAL_HIGH_WATERMARK=70`
- `GRAPHDB_INGEST_WAL_STOP_WATERMARK=85`
- `GRAPHDB_INGEST_MAX_PENDING_AGE=2m`
- `GRAPHDB_INGEST_FLUSH_INTERVAL=250ms`
- `GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=8`
- `GRAPHDB_INGEST_FLUSH_MAX_BYTES=2MiB`
- `GRAPHDB_INGEST_FLUSH_WORKERS=2`
- `GRAPHDB_INGEST_METADATA_FLUSH_INTERVAL=500ms`
- `GRAPHDB_INGEST_METADATA_MAX_REQUESTS=256`
- `GRAPHDB_INGEST_METADATA_MAX_BYTES=8MiB`
- `GRAPHDB_INGEST_METADATA_FLUSH_WORKERS=2`
- `GRAPHDB_COORDINATION=local|postgres`
- `GRAPHDB_POSTGRES_DSN=<dsn>`
- `GRAPHDB_POSTGRES_SCHEMA=graphdb_coordination`
- `GRAPHDB_COORDINATOR_NAMESPACE=<stable-cluster-id>`
- `GRAPHDB_WRITE_CAS_MAX_RETRIES=8`
- `GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION=24h`
- `GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL=3m`（必须大于 `GRAPHDB_WRITE_EXECUTION_TIMEOUT`）
- `GRAPHDB_COORDINATOR_OUTBOX_RETENTION=1h`
- `GRAPHDB_COORDINATOR_CLEANUP_INTERVAL=1m`
- `GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE=5000`
- `GRAPHDB_READINESS_TIMEOUT=2s`

严格串行写入时保持 `GRAPHDB_WRITE_MAX_PER_TENANT=1`。设为 `2`-`4`
可以有限流水化准入检查和提交后的元数据收尾，但 manifest 发布仍受每租户
本地单 writer 锁或 PostgreSQL head CAS 保护。`0` 会关闭这一准入维度，
只适合受控测试。

自动 compact、GC 和索引追赶会等待每个租户 ingest 空闲满 1 分钟。后台重型
任务默认单并发；只有经过明确的工作负载评审后才应提高并发。

PostgreSQL 协调还要求 `GRAPHDB_INGEST_MODE=direct`、`GRAPHDB_STORAGE=s3`、
`S3_PROVIDER=generic-s3` 和 `GRAPHDB_WRITER_TOPOLOGY=cas`。启动 writer
前先执行 `graphdb coordinator migrate` 和
`graphdb coordinator bootstrap --apply`；上线与回滚顺序见发行版部署文档。

已提交幂等记录和被遗弃的 pending reservation 只保留到配置的幂等窗口，
重放去重也只在该窗口内保证。`GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL`
必须大于 `GRAPHDB_WRITE_EXECUTION_TIMEOUT`，pending 记录在 ownership TTL
以内绝不会被清理。已完成的 legacy manifest 任务只有在超过保留时间、且
租户镜像水位已经到达该 revision 后才会删除。对应 retention 设为 `0`
会关闭自动清理，只应在另有归档和清理流程时使用。

`/v1/readiness` 会执行有界 bucket list（`max-keys=1`），因此 reader 和
writer 使用的对象存储身份都必须具备 bucket list 权限。`/v1/health` 和
`/metrics` 只读取后台采样的 coordinator 状态，不在请求路径同步访问
PostgreSQL；readiness 仍然是主动依赖探测。

可观测性：

- `GRAPHDB_SLOW_QUERY_THRESHOLD=500ms`
- `GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces`
- `GRAPHDB_OTLP_INSECURE=true`
- `GRAPHDB_SERVICE_NAME=graphdb`

## MinIO 本地栈

```sh
docker compose up --build
```

这会启动 MinIO、创建 `graphdb` bucket，并以 `all` 模式启动 GGraphDB。

## RustFS Writer/Reader 栈

```sh
docker compose -f docker-compose.rustfs.yml up --build
```

默认服务：

- `graphdb`：`:38080` 上的 writer
- `graphdb-reader`：`:38081` 上的 reader
- `rustfs`：`:39000` 上的对象存储

可选 reader 扩展：

```sh
docker compose -f docker-compose.rustfs.yml --profile scale-readers up --build
```

## Reader 新鲜度

单个 reader：

```sh
curl -sS "$READER/v1/control/reader-freshness" -H 'X-Tenant-ID: demo'
```

集群就绪：

```sh
curl -sS "$READER/v1/control/reader-fleet-readiness?min_ready=1" \
  -H 'X-Tenant-ID: demo'
```

流量闸门：

```sh
curl -sS "$READER/v1/control/reader-traffic-gate?min_ready=1" \
  -H 'X-Tenant-ID: demo'
```

只有 reader 能报告所需版本或 fleet gate 返回 ready 时，才应接入流量。没有
外部路由器时，可把该接口交给部署系统或负载均衡器做健康检查。

## 指标、日志和链路

指标：

```sh
curl -sS "$ADMIN/metrics"
```

重点指标包括 HTTP/查询延迟、写入背压、对象存储延迟、CAS 冲突、commit
tail 长度、reader 可见版本、coordinator 可用性/head revision/mirror lag
和索引健康。日志以 JSON 行输出，覆盖 HTTP
访问、写入/控制审计、采集、索引重建、慢查询和背压事件。设置
`GRAPHDB_OTLP_ENDPOINT` 后通过 OTLP/HTTP 导出 trace。
listener 分离、网关租户绑定、RBAC 与 TLS 见
[生产安全边界](../security-deployment.zh-CN.md)。

## 生产运行规则

- 本地协调每租户保持恰好一个活跃 writer；只有完成 PostgreSQL bootstrap
  后才扩到 2–8 个，并且绝不能混入 1.0 writer。
- 多个 reader 从同一对象存储前缀独立运行。
- 需要新鲜度时使用 `min_version`。
- 关注 commit tail 并保持自动 compact。
- 定期运行索引健康检查；`?deep=true` 只用于明确的深度校验。
- 按计划执行恢复演练、完整性审计和 GC。
- 把对象存储延迟和 429 背压视为采集器降速信号，而不是数据丢失。
- 对于 CMDB 采集场景，每批先按 200-500 个逻辑组规划，再逐步提高并发；
  小批次会放大 commit、manifest、幂等和采集元数据对象写入。
- 429 重试必须复用相同 `batch_id` 和 `idempotency_key`，使用指数退避
  和抖动。同一源页面的重试不能生成新幂等键。
