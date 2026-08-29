# GGraphDB 发行版部署文档

[English](release-deployment.md)

本文档面向需要下载、部署和升级 GGraphDB 1.2 的服务负责人。示例使用已发布
的 `v1.2.2`，发行页位于
<https://github.com/SamuelSupe/graphdb/releases/tag/v1.2.2>。

## 1. 下载与校验

发行页提供以下资产：

- `graphdb-v1.2.2.tar.gz`：二进制、SDK、文档、示例和 Compose 文件。
- `graphdb-v1.2.2.tar.gz.sha256`：SHA-256 校验文件。

下载后校验并解包：

```sh
sha256sum -c graphdb-v1.2.2.tar.gz.sha256
tar -xzf graphdb-v1.2.2.tar.gz
cd graphdb-v1.2.2
```

压缩包包含：

```text
bin/graphdb-linux-amd64
bin/graphdb-linux-arm64
bin/graphdb-darwin-arm64
Dockerfile
docker-compose.yml
docker-compose.rustfs.yml
docker-compose.postgres.yml
docs/
examples/
sdk/
CHANGELOG.md
LICENSE
SECURITY.md
BUILD-METADATA.json
VERSION
```

二进制是静态构建，不需要在目标主机安装 Go。运行时仍需要可访问的本地
磁盘或 S3 兼容对象存储；生产环境建议使用外部对象存储，不要把示例凭据
直接用于生产。
上线前执行 `bin/graphdb-linux-amd64 version`，并与
`BUILD-METADATA.json` 中的 commit 对照。

## 2. 单机文件存储

适合开发、演示和小规模单进程部署。以 Linux amd64 为例：

```sh
sudo install -m 0755 bin/graphdb-linux-amd64 /usr/local/bin/graphdb
sudo mkdir -p /var/lib/graphdb
sudo chown "$(id -u):$(id -g)" /var/lib/graphdb

export GRAPHDB_MODE=all
export GRAPHDB_STORAGE=local
export GRAPHDB_DATA_DIR=/var/lib/graphdb
export GRAPHDB_PREFIX=graphdb
export GRAPHDB_ADDR=:8080
graphdb serve
```

验证服务：

```sh
curl -fsS http://127.0.0.1:8080/v1/health
```

首次使用时创建租户：

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","name":"Demo"}'
```

## 3. Docker Compose + MinIO

这是最简单的对象存储部署方式，适合本地集成环境：

```sh
docker compose up -d --build
curl -fsS http://127.0.0.1:8080/v1/health
```

默认端口：

- GGraphDB：`8080`
- MinIO API：`9000`
- MinIO Console：`9001`

端口冲突时覆盖宿主机端口：

```sh
MINIO_API_PORT=29000 \
MINIO_CONSOLE_PORT=29001 \
GRAPHDB_PORT=28080 \
docker compose up -d --build
```

## 4. RustFS Writer/Reader 部署

该拓扑使用同一个 GGraphDB 二进制，靠运行模式和流量路由区分 writer 与
reader。一个租户同时只允许一个活跃 writer；reader 从共享对象存储加载
数据并提供查询服务。

```sh
docker compose -f docker-compose.rustfs.yml up -d --build
curl -fsS http://127.0.0.1:38080/v1/health
curl -fsS http://127.0.0.1:38081/v1/health
```

默认端口：

- writer：`38080`
- reader：`38081`
- RustFS S3 API：`39000`

扩展 reader：

```sh
docker compose -f docker-compose.rustfs.yml \
  --profile scale-readers up -d --build
```

生产环境应把 `S3_ENDPOINT`、`S3_BUCKET`、`S3_ACCESS_KEY_ID` 和
`S3_SECRET_ACCESS_KEY` 替换为真实对象存储配置，并通过 Secret、环境变量
管理系统或密钥服务注入，不要提交到仓库。

推荐的进程配置：

```text
写入和控制流量 -> GRAPHDB_MODE=writer -> 一个 writer/租户
查询流量       -> GRAPHDB_MODE=reader -> 多个 reader
对象数据       -> 共享 S3/RustFS bucket 与 GRAPHDB_PREFIX
```

## 5. 关键配置

最小 S3 配置：

```sh
GRAPHDB_MODE=writer
GRAPHDB_STORAGE=s3
GRAPHDB_ADDR=:8080
GRAPHDB_PREFIX=graphdb
S3_ENDPOINT=https://s3.example.com
S3_BUCKET=graphdb
S3_PATH_STYLE=false
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=<access-key>
S3_SECRET_ACCESS_KEY=<secret-key>
```

生产环境应分离管理面：

```sh
GRAPHDB_ADDR=0.0.0.0:8080
GRAPHDB_ADMIN_ADDR=127.0.0.1:8081
GRAPHDB_PPROF_ENABLED=false
```

常用运行参数：

- `GRAPHDB_POLL_INTERVAL`：reader 检查对象存储新 manifest 的间隔。
- `GRAPHDB_READER_CACHE_IDLE_TTL`：不活跃租户图的独立缓存驻留时间，不再与
  manifest 轮询周期绑定。
- `GRAPHDB_READER_CACHE_LOAD_TIMEOUT`：共享冷加载的内部预算；单个请求取消后，
  加载仍可继续完成缓存预热。
- `GRAPHDB_READER_CACHE_LOAD_MAX_CONCURRENT`：跨租户完整图加载的全局并发上限，
  默认 `4`。
- `GRAPHDB_READER_CACHE_LOAD_QUEUE_TIMEOUT`：完整图加载等待槽位的最长时间，
  默认 `2s`，超时后拒绝请求。
- `GRAPHDB_READER_CATCHUP_TIMEOUT`：reader 等待 `min_version` 的最长时间。
- `GRAPHDB_WRITE_MAX_PER_TENANT`：每租户写入准入上限；生产默认保持 `1`。
- `GRAPHDB_MAINTENANCE_INTERVAL`：compact、GC 和索引维护调度间隔。
- `GRAPHDB_OTLP_ENDPOINT`：可选的 OTLP/HTTP trace 接收地址。

### 可选 PostgreSQL 多 writer 协调

`GRAPHDB_COORDINATION=local` 仍是默认值，完整保留 1.0 的 writer lease 和
对象 manifest CAS 行为。需要让同一租户由 2–8 个 writer 乐观并发写入时，
所有 1.1 writer 和 reader 必须使用同一个 PostgreSQL namespace：

```sh
GRAPHDB_COORDINATION=postgres
GRAPHDB_POSTGRES_DSN='postgres://graphdb:<password>@postgres:5432/graphdb'
GRAPHDB_POSTGRES_SCHEMA=graphdb_coordination
GRAPHDB_COORDINATOR_NAMESPACE=production-a
GRAPHDB_WRITE_CAS_MAX_RETRIES=8
GRAPHDB_COORDINATOR_IDEMPOTENCY_RETENTION=24h
GRAPHDB_COORDINATOR_PENDING_RESERVATION_TTL=3m
GRAPHDB_COORDINATOR_OUTBOX_RETENTION=1h
GRAPHDB_COORDINATOR_CLEANUP_INTERVAL=1m
GRAPHDB_COORDINATOR_CLEANUP_BATCH_SIZE=5000

GRAPHDB_STORAGE=s3
S3_PROVIDER=generic-s3
GRAPHDB_WRITER_TOPOLOGY=cas
```

首版 PostgreSQL 协调只支持正确实现 `If-Match` 的 generic S3 兼容存储，
包括 RustFS；原生 OSS/OBS/COS provider 继续使用本地单 writer 协调。
PostgreSQL 故障不会自动回退到本地模式：写入返回
`503 coordinator_unavailable`；reader 可以服务已有缓存，但无法满足
`min_version` 时返回 `reader_not_fresh`。

初始化和检查协调平面：

```sh
graphdb coordinator migrate
graphdb coordinator bootstrap --dry-run
graphdb coordinator bootstrap --apply
graphdb coordinator status
graphdb coordinator sync-legacy-manifest
graphdb coordinator rollback --dry-run
```

bootstrap 会把每个租户当前 1.0 `manifest.parquet` 和写规则复制为不可变协调
对象，建立 PostgreSQL head，最后写入对象存储 coordination marker。该 marker
会阻止本地 writer 或 1.0 writer 对同一 prefix 启动写入。

完整配置见 [deploy-ops.md](deploy-ops.zh-CN.md) 和根目录 README 的
Configuration 小节。

## 6. 流量接入与健康检查

`GET /v1/health` 只用于进程存活检查，它读取后台最近一次采样的 coordinator
状态，不把 PostgreSQL 放到 liveness 请求路径；`GET /v1/readiness` 用于
进程级流量接入，会主动探测对象存储 bucket 和 coordinator，任一不可用时
返回 `503`。reader 加入负载均衡前，还要使用租户级就绪检查：

```sh
curl -fsS 'http://127.0.0.1:38081/v1/readiness'
curl -fsS \
  'http://127.0.0.1:38081/v1/control/reader-fleet-readiness?min_ready=1' \
  -H 'X-Tenant-ID: demo'
```

需要读到刚提交版本时，把写入响应中的 `version` 作为 `min_version` 传给
reader；允许最终一致读取时才使用 `allow_stale=true`。`X-Tenant-ID` 只是
租户路由标识，不是认证机制，生产环境必须在网关或服务网格中配置认证、
授权、TLS 和限流。

## 7. 升级、回滚与数据安全

### GGraphDB 1.1 兼容性

GGraphDB 1.1 不会重写 1.0 核心图。manifest、snapshot、commit、entity、edge
和 Parquet 对象仍使用对象布局版本 2，因此不需要离线数据迁移。新增数据都是
附加式的：

- 实体标签保存在普通保留字段 `fields.__graphdb_labels` 中；
- 关系属性 schema 和反向邻接索引位于
  `tenants/<tenant>/extensions/v1.1/` 下。

对已有租户，先启动 1.1 writer，再执行一次 `index_rebuild` task，在接入对
延迟敏感的 `in`/`both` 查询前生成反向邻接 shard。在此之前，系统会回退到
图 snapshot 保证结果正确，但反向读取还没有 1.1 lazy read 的性能。

滚动升级期间，1.0 reader 可以继续读取核心图并忽略保留字段和扩展对象；
`pattern` 查询以及依赖反向索引的流量只路由到 1.1 reader。混合版本客户端应
继续使用 `upsert_ci_types`、`delete_ci_types` 和 `ci_type`，领域中立别名只由
1.1 HTTP API 解码。

启用 PostgreSQL 协调时采用更严格的上线顺序：

1. 先迁移 PostgreSQL schema，停止全部 1.0 writer，并校验当前 legacy manifest。
2. 执行 `coordinator bootstrap --dry-run`，确认后再执行 `--apply`。
3. 先启动一个 1.1 PostgreSQL writer，验证真实读写、coordinator health、
   CAS 指标和 mirror 状态。
4. 再扩容 writer；只有在 `max_legacy_mirror_lag=0` 且
   `outbox_backlog=0` 后，才让 1.0 reader 接流量。
5. 永久撤销 1.0 writer 的路由与写凭据。1.0 reader 只看到最终一致镜像，
   PostgreSQL head 始终是唯一权威状态。

回滚必须使用 coordinator 命令，不能手工删除 coordination marker：

```sh
# PostgreSQL writer 仍可用时，先检查所有租户与 mirror。
graphdb coordinator rollback --dry-run

# 摘除写流量并停止全部 PostgreSQL writer 后，显式确认该运维条件。
graphdb coordinator rollback --apply --writers-stopped
```

apply 会先把权威协调模式从 `postgres` 切到 `draining`，从而 fence 所有残留的
PostgreSQL writer；随后排空 mirror outbox，逐租户核对 legacy manifest 与
PostgreSQL head 的 hash、version 和 status，再切到 `local`，最后通过 ETag
条件删除 `<GRAPHDB_PREFIX>/coordination/mode.json`。只有命令报告
`applied=true`、`mode_after=local`、`marker_removed=true` 且 mirror lag/backlog
均为零后，才可启动本地 writer。严禁两种 writer 模式并存。完整备份必须同时
包含对象存储 prefix 和 PostgreSQL coordination schema，只有对象存储的备份
不完整。

除 `GET /v1/health` 和 `graphdb coordinator status` 外，还应监控
`graphdb_coordinator_cas_total`、`graphdb_coordinator_cleanup_runs_total`、
`graphdb_coordinator_cleanup_deleted_total`、`graphdb_coordinator_head_revision`
和 `graphdb_coordinator_status`。

PostgreSQL 与 generic S3 soak 是发布硬依赖。使用 OrbStack/Docker 时，下面
命令会启动隔离的 PostgreSQL 与 RustFS；8 writer 并发正确性由 integration
suite 验证，soak 使用 2 个活跃 writer 执行 30 分钟、20 commit/s 容量门禁：

```sh
scripts/postgres_cas_gate.sh soak
```

测试会使用唯一对象前缀和 PostgreSQL schema；遇到终态 `write_conflict` 时，
使用相同幂等键重试。压测期间同时运行 legacy mirror 和派生索引维护，结束后
等待所有 backlog 清零，并用指定 tag 的真实 1.0 二进制读取获胜镜像。生成的
JSON 证据绑定被测 commit，并由 release job 打包；版本丢失或重复、维护未追平、
1.0 读取失败或吞吐低于目标的 90% 都会使门禁失败。

发行 commit 还必须通过独立的 PostgreSQL 到 local 正式回滚门禁：

```sh
GRAPHDB_TEST_BUILD_COMMIT="$(git rev-parse HEAD)" \
GRAPHDB_TEST_ROLLBACK_REPORT=/path/to/rollback-drill.json \
scripts/postgres_cas_gate.sh rollback
```

该 commit 绑定报告会证明 mirror lag 与 outbox backlog 均已清零、marker
完成条件删除、残留 PostgreSQL writer 被 fence，并且本地 writer 能在同一张
图上继续推进版本而不丢数据。

审批记录应同时使用[发行 checklist](../release-checklist.md)、
[安全边界](../security-deployment.zh-CN.md)和
[容量边界](../capacity.zh-CN.md)。

writer 回滚到 1.0 在布局上安全，但 1.0 writer 不会执行关系属性 schema
校验。回滚前先排空或取消 `bulk_import` task，并暂停受 schema 管理的边写入，
直到恢复 1.1 writer。不要删除 `extensions/v1.1` 对象，重新升级后会继续复用。
需要把关系 schema 恢复到新租户时应使用 1.1 backup/restore；1.0 backup 格式
只认识核心图。

升级前先确认对象存储快照、manifest 和最近备份可读，再执行：

```sh
docker compose -f docker-compose.rustfs.yml pull
docker compose -f docker-compose.rustfs.yml up -d --build
```

二进制部署则下载新 Release，停止旧进程后替换二进制并保留
`GRAPHDB_DATA_DIR` 或 S3 前缀不变。升级后检查 `/v1/health`、reader 就绪、
`/metrics` 和一个真实租户的读写链路。

回滚时固定回滚二进制或镜像版本，不要删除对象存储中的 manifest、commit、
snapshot 或 index 对象。涉及 schema 或存储格式变化时，先在副本 bucket
执行恢复演练，再切换生产流量。

常用停止命令：

```sh
docker compose -f docker-compose.rustfs.yml down
```

`down` 不会删除命名卷；不要使用 `down -v`，除非已经确认可以删除本地
RustFS 数据。
