# GraphDB 发行版部署文档

[English](release-deployment.md)

本文档面向需要下载、部署和升级 GraphDB 的服务负责人。GraphDB 1.0 的
GitHub 发行版标签为 `v1.0.0`，发布页位于：

<https://github.com/SamuelSupe/graphdb/releases/tag/v1.0.0>

## 1. 下载与校验

发行页提供以下资产：

- `graphdb-v1.0.0.tar.gz`：二进制、文档、示例和 Compose 文件。
- `graphdb-v1.0.0.tar.gz.sha256`：SHA-256 校验文件。

下载后校验并解包：

```sh
sha256sum -c graphdb-v1.0.0.tar.gz.sha256
tar -xzf graphdb-v1.0.0.tar.gz
cd graphdb-v1.0.0
```

压缩包包含：

```text
bin/graphdb-linux-amd64
bin/graphdb-linux-arm64
bin/graphdb-darwin-arm64
Dockerfile
docker-compose.yml
docker-compose.rustfs.yml
docs/
examples/
VERSION
```

二进制是静态构建，不需要在目标主机安装 Go。运行时仍需要可访问的本地
磁盘或 S3 兼容对象存储；生产环境建议使用外部对象存储，不要把示例凭据
直接用于生产。

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

- GraphDB：`8080`
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

该拓扑使用同一个 GraphDB 二进制，靠运行模式和流量路由区分 writer 与
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

常用运行参数：

- `GRAPHDB_POLL_INTERVAL`：reader 检查对象存储新 manifest 的间隔。
- `GRAPHDB_READER_CATCHUP_TIMEOUT`：reader 等待 `min_version` 的最长时间。
- `GRAPHDB_WRITE_MAX_PER_TENANT`：每租户写入准入上限；生产默认保持 `1`。
- `GRAPHDB_MAINTENANCE_INTERVAL`：compact、GC 和索引维护调度间隔。
- `GRAPHDB_OTLP_ENDPOINT`：可选的 OTLP/HTTP trace 接收地址。

完整配置见 [deploy-ops.md](deploy-ops.zh-CN.md) 和根目录 README 的
Configuration 小节。

## 6. 流量接入与健康检查

`GET /v1/health` 只用于进程存活检查。reader 加入负载均衡前，使用租户级
就绪检查：

```sh
curl -fsS \
  'http://127.0.0.1:38081/v1/control/reader-fleet-readiness?min_ready=1' \
  -H 'X-Tenant-ID: demo'
```

需要读到刚提交版本时，把写入响应中的 `version` 作为 `min_version` 传给
reader；允许最终一致读取时才使用 `allow_stale=true`。`X-Tenant-ID` 只是
租户路由标识，不是认证机制，生产环境必须在网关或服务网格中配置认证、
授权、TLS 和限流。

## 7. 升级、回滚与数据安全

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
