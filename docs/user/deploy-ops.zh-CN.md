# 部署与运维

[English](deploy-ops.md)

## 运行模式

```sh
GRAPHDB_MODE=all|writer|reader
```

- `all`：开发或小规模部署的单进程模式。
- `writer`：生产写入和控制进程。
- `reader`：读取和查询进程。

生产环境每个租户只能有一个活跃 writer。writer lease 和 manifest CAS
用于防止重复或陈旧 writer，不是多 writer 调度器。reader 相互独立，维护
本地缓存，但对象存储仍是事实来源。

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
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=300`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=512MiB`
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB`
- `GRAPHDB_INDEX_ENTITY_RECORDS=false`
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true`

严格串行写入时保持 `GRAPHDB_WRITE_MAX_PER_TENANT=1`。设为 `2`-`4`
可以有限流水化准入检查和提交后的元数据收尾，但 manifest 发布仍受每租户
单 writer 锁保护。`0` 会关闭这一准入维度，只适合受控测试。

可观测性：

- `GRAPHDB_SLOW_QUERY_THRESHOLD=500ms`
- `GRAPHDB_OTLP_ENDPOINT=http://otel-collector:4318/v1/traces`
- `GRAPHDB_OTLP_INSECURE=true`
- `GRAPHDB_SERVICE_NAME=graphdb`

## MinIO 本地栈

```sh
docker compose up --build
```

这会启动 MinIO、创建 `graphdb` bucket，并以 `all` 模式启动 GraphDB。

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
curl -sS "$BASE/metrics"
```

重点指标包括 HTTP/查询延迟、写入背压、对象存储延迟、CAS 冲突、commit
tail 长度、reader 可见版本和索引健康。日志以 JSON 行输出，覆盖 HTTP
访问、写入/控制审计、采集、索引重建、慢查询和背压事件。设置
`GRAPHDB_OTLP_ENDPOINT` 后通过 OTLP/HTTP 导出 trace。

## 生产运行规则

- 每个租户保持恰好一个活跃 writer。
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
