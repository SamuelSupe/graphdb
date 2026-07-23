# 生产安全边界

[English](security-deployment.md)

GGraphDB 1.1 把认证与授权放在网关或服务网格层。数据库不会把
`X-Tenant-ID` 当成身份凭证；生产部署必须阻止客户端直接访问任一
GGraphDB listener。

## 安全默认值

- 默认 `GRAPHDB_PPROF_ENABLED=false`。
- `GRAPHDB_ADDR` 兼容监听永不暴露 pprof。
- 配置 `GRAPHDB_ADMIN_ADDR` 后，数据面和管理面使用独立 listener。
- 启用 pprof 时必须配置不同于 `GRAPHDB_ADDR` 的
  `GRAPHDB_ADMIN_ADDR`，否则启动失败。
- PostgreSQL coordinator 不可用时写入 fail-closed，不会回退到本地
  writer。

推荐生产配置：

```sh
GRAPHDB_ADDR=0.0.0.0:8080
GRAPHDB_ADMIN_ADDR=127.0.0.1:8081
GRAPHDB_PPROF_ENABLED=false
```

管理 listener 应只绑定 loopback、Pod 私有网卡或独立管理网络。pprof
只在故障诊断期间临时启用。

## Listener 职责

未设置 `GRAPHDB_ADMIN_ADDR` 时，除默认关闭 pprof 外，仍保持 1.0
兼容的合并 listener。设置后：

| Listener | 接口范围 |
| --- | --- |
| 数据面 | commit、ingest/import、图读取、schema 读取、查询、执行 saved query |
| 管理面 | 租户生命周期、policy/config、任务、索引、维护/control、指标、可选 pprof |
| 两者 | health、readiness、OpenAPI |

管理接口在数据 listener 上返回 `404`，数据写入和查询接口在管理
listener 上返回 `404`。

## 网关契约

参考配置为 `deploy/nginx/graphdb.conf.example`。身份服务需要：

1. 验证 bearer token 或 mTLS 身份；
2. 未授权租户必须拒绝请求；仅在鉴权成功时用响应头
   `X-GraphDB-Tenant-ID` 返回已授权租户；
3. 执行请求头 `X-GraphDB-Required-Roles` 指定的角色要求；管理面鉴权子请求
   只允许 `admin` 和 `operator`；
4. 网关删除客户端提供的所有 `X-Tenant-ID`，再写入已验证租户；
5. 身份、租户或角色校验失败时返回 `401` 或 `403`。

示例终止 TLS 1.2/1.3，并只在网关用 `/admin/` 暴露管理路径。请按实际
身份平台细化角色与路径策略。角色判定应留在身份服务中，因为 NGINX
rewrite 条件会早于 `auth_request` 执行；绝不能透传调用方自行提交的租户
header。

参考角色矩阵：

| 路由类型 | 允许角色 |
| --- | --- |
| 图读取、schema 读取和查询 | `reader`、`writer`、`operator`、`admin` |
| commit、ingest 和 import | `writer`、`operator`、`admin` |
| `/admin/` 生命周期、policy、task、index 和 control | `operator`、`admin` |

身份服务必须把逗号分隔角色解释为 any-of，并把原始 method、URI、身份和
租户一起校验。

## 必须落实的网络控制

- 客户端只能访问 TLS 网关；
- 禁止直接访问数据与管理 listener；
- PostgreSQL coordination schema 和对象存储凭据只授予 GGraphDB 服务身份；
- 1.0 reader 只能使用对象存储只读凭据；PG bootstrap 前撤销所有 1.0
  writer 路由和写凭据；
- 指标可能包含租户标签和运行状态，必须保护；
- 访问日志与审计日志发送到追加写或集中控制的日志系统。

GGraphDB 当前不提供租户内部的行级授权。需要子租户可见性时，应在上游
强制执行，或拆成独立租户。
