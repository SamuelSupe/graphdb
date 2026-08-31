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
`GRAPHDB_COORDINATION=postgres` 使用 PostgreSQL head CAS 协调租户 head，支持
每租户 2–8 个乐观并发 writer。PostgreSQL 只保存协调元数据；不可变图对象和
对象存储中已发布的 manifest 仍是图数据权威。reader 相互独立，从对象存储
加载不可变图对象。

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
- `GRAPHDB_READER_CACHE_IDLE_TTL=15m`
- `GRAPHDB_READER_CACHE_LOAD_TIMEOUT=1m`
- `GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT=4`
- `GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT=2s`
- `GRAPHDB_READER_CATCHUP_TIMEOUT=2s`

写入路径：

- `GRAPHDB_WRITE_MAX_CONCURRENT=32`
- `GRAPHDB_WRITE_MAX_PER_TENANT=1`
- `GRAPHDB_WRITE_QUEUE_TIMEOUT=2s`
- `GRAPHDB_WRITE_EXECUTION_TIMEOUT=90s`
- `GRAPHDB_WRITE_MAX_COMMIT_TAIL=1500`
- `GRAPHDB_WRITE_CACHE_MAX_BYTES=512MiB`
- `GRAPHDB_ENTITY_PAGE_PACK_MAX_BYTES=32MiB`
- `GRAPHDB_INDEX_ENTITY_RECORDS=false`
- `GRAPHDB_INGEST_COLLECTOR_STATUS_MATERIALIZED=true`
- `GRAPHDB_COORDINATION=local|postgres`
- `GRAPHDB_INGEST_MODE=direct|wal`
- `GRAPHDB_INGEST_WAL_DIR=${GRAPHDB_DATA_DIR}/wal/ingest`
- `GRAPHDB_INGEST_WAL_DURABILITY=sync|os`（durable 接收应使用 `sync`）
- `GRAPHDB_INGEST_WAL_BUFFER_BYTES=4MiB`
- `GRAPHDB_INGEST_WAL_FSYNC_INTERVAL=5ms`
- `GRAPHDB_INGEST_WAL_MAX_BYTES=10GiB`
- `GRAPHDB_INGEST_QUEUE_MEMORY_MAX_BYTES=256MiB`
- `GRAPHDB_INGEST_FLUSH_INTERVAL=10s`
- `GRAPHDB_INGEST_FLUSH_MAX_REQUESTS=256`
- `GRAPHDB_INGEST_FLUSH_MAX_BYTES=8MiB`
- `GRAPHDB_INGEST_FLUSH_WORKERS=1`
- `GRAPHDB_INGEST_SHUTDOWN_TIMEOUT=30s`
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

PostgreSQL 协调还要求 `GRAPHDB_STORAGE=s3`、
`S3_PROVIDER=generic-s3` 和 `GRAPHDB_WRITER_TOPOLOGY=cas`。启动 writer
前先执行 `graphdb coordinator migrate` 和
`graphdb coordinator bootstrap --apply`；上线与回滚顺序见发行版部署文档。

### 1.3 PostgreSQL-CAS WAL profile

1.3 WAL profile 只能用于协调的 generic-S3 writer 拓扑。每个 writer 必须有
唯一且稳定的 `GRAPHDB_INSTANCE_ID`，并使用自己的持久读写 WAL 卷：

```sh
GRAPHDB_MODE=writer
GRAPHDB_STORAGE=s3
S3_PROVIDER=generic-s3
GRAPHDB_COORDINATION=postgres
GRAPHDB_WRITER_TOPOLOGY=cas
GRAPHDB_COORDINATOR_NAMESPACE=production-a
GRAPHDB_INSTANCE_ID=writer-a
GRAPHDB_INGEST_MODE=wal
GRAPHDB_INGEST_WAL_DIR=/var/lib/graphdb/wal/ingest
GRAPHDB_INGEST_WAL_DURABILITY=sync
```

不同 writer 不能共享 WAL 目录或卷。durable `202` 表示该 writer 在本地 WAL
`fsync` 后已经接管请求；它不等待 PostgreSQL，也不代表图版本已经提交。响应
会返回 owner-routed `status_url` 和 `writer_id`。状态查询必须路由回该实例，
包括它在 WAL 恢复期间报告 `recovery_pending` 时。

启动时，如果 PostgreSQL 或对象存储 coordination marker 探测因暂时不可用或
超时失败，WAL writer 可以以 degraded 模式启动。恢复本地 owner 身份和 WAL 后，
在这些依赖暂时不可达期间仍可提供 owner status 并接收 durable `202`。但 required
schema 缺失/不匹配或 coordination marker 缺失/不匹配都不是暂时错误；启动必须
fail closed，不能在未经验证的协调平面上接受写入。

WAL 按租户组成有界批次。单 writer 保持自己的 WAL FIFO，跨 writer 则按成功的
PostgreSQL head CAS 排序。CAS、PostgreSQL 暂时故障和对象存储暂时故障都通过
重新读取最新 head、重基和持续冲突后的缩批重试；已接收请求不能仅因为重试预算
到达而进入终态失败。确定性的语义错误和生命周期 fencing 可以最终 `failed`。
freeze/delete/restore fencing 优先于未发布 WAL，但不会回滚已经 CAS 发布的版本。

PostgreSQL 故障期间可以继续本地 durable 接收，直到 WAL 高水位；达到高水位后，
在写入新 payload 前拒绝准入，同时继续提供 owner status 和恢复能力。该 profile
只保护原 WAL 卷仍可恢复时的进程故障，不保护 WAL 卷永久丢失。完整合同见
[PostgreSQL-CAS 多 writer Ingest WAL](../ingest-wal-multiwriter-design.zh-CN.md)。

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

- 本地协调每租户保持恰好一个活跃 writer。1.3 PostgreSQL-CAS profile 允许
  2–8 个 writer 接收同一租户；扩展目标是跨租户，不宣称单租户线性吞吐。
  共享 prefix 中不能混入未接入协调的 1.0 writer。
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
- 进程重启时保持 `GRAPHDB_INSTANCE_ID` 不变并重新挂载原 WAL 卷。WAL writer
  降级为 direct 前，先停止新 WAL 准入并等待 pending WAL 全部 finalized；存在
  pending WAL 时禁止降级。
