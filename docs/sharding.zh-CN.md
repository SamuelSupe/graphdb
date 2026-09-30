# 租户分片与扩容

分片模式使用独立的 Raft 数据组承载不同租户，保留默认单机和原有单组 Raft 部署。每个租户的完整图、版本、接入身份和维护记录都归一个组管理。新增组可以增加多个租户的存储和写入容量；单个租户仍由一个 Leader 服务，没有单租户图切分或跨分片关联查询。

## 拓扑与一致性

```mermaid
flowchart TD
  Client[客户端] --> Router[无状态路由入口]
  Router --> Catalog[目录 Raft 组：3 个独立副本]
  Router --> A[数据分片 A：3 个独立副本]
  Router --> B[数据分片 B：3 个独立副本]
  Catalog -->|分配、迁移协调| A
  Catalog -->|分配、迁移协调| B
```

目录持久化分片地址、租户位置、路由代次和迁移阶段。路由入口逐请求取得目录的强一致位置，再向所属组的当前 Leader 转发。数据组分别复制、提交和生成快照；一个数据组失去多数派不阻止其他数据组读写。目录失去多数派时入口停止接收图请求。全局租户列表需要所有注册分片可用，不返回不完整的成功结果。

入口没有本地数据，可在负载均衡器后部署多份。每个 Raft 副本必须使用独立的本地目录和磁盘，不使用共享文件系统。目录、数据组和路由使用相同的私网通信令牌；每个组有不同的集群 ID。私网监听器只向可信网络开放，HTTP 本身没有 TLS。图 API 的外部认证仍由现有网关负责；集群管理接口另外校验入口令牌。

## 启动与新增分片

[docker-compose.sharded.yml](../docker-compose.sharded.yml) 提供目录组、两个数据组和一个入口，十个进程，九个独立数据卷。默认入口为本机 8080，未发布节点的 8080/8081。示例中的容器同属一台宿主机；生产副本需分布到不同故障域。

```sh
export GRAPHDB_RAFT_TOKEN="$(openssl rand -hex 32)"
# 先启动目录、一个数据组及入口。
docker --context orbstack compose -f docker-compose.sharded.yml up --build -d catalog1 catalog2 catalog3 a1 a2 a3 router

curl -fsS http://127.0.0.1:8080/v1/cluster/shards \
  -H "Authorization: Bearer $GRAPHDB_RAFT_TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"shard-a","cluster_id":"graphdb-shard-a","peers":{"1":"http://a1:8081","2":"http://a2:8081","3":"http://a3:8081"}}'

curl -fsS http://127.0.0.1:8080/v1/tenants -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo"}'

# 扩容：启动另一组并注册，已有租户的位置保持不变。
docker --context orbstack compose -f docker-compose.sharded.yml up -d b1 b2 b3
curl -fsS http://127.0.0.1:8080/v1/cluster/shards \
  -H "Authorization: Bearer $GRAPHDB_RAFT_TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"shard-b","cluster_id":"graphdb-shard-b","peers":{"1":"http://b1:8081","2":"http://b2:8081","3":"http://b3:8081"}}'
```

注册会探测数据组，校验组 ID、分片角色和对象前缀。新租户按已分配租户数量选择组，平局按分片 ID 排序；位置经目录复制后固定。创建租户、首次 commit / ingest / import 可以自动分配。分配失败的租户保留 `assigning` 状态，组恢复后继续；可从目录查看所属组。当前没有按字节、流量或热点自动均衡。

| 配置 | 使用者 | 语义 |
| --- | --- | --- |
| `GRAPHDB_RAFT_CATALOG=true` | 目录副本 | 只提供目录及协调服务，不承载用户图 |
| `GRAPHDB_RAFT_SHARD_ID` | 数据副本 | 同组所有副本使用相同分片 ID |
| `GRAPHDB_ROUTER_TOKEN` | `serve-router` | 至少 32 字节，与各组的 `GRAPHDB_RAFT_TOKEN` 相同 |
| `GRAPHDB_ROUTER_CATALOG_CLUSTER_ID` | 入口 | 目录组的集群 ID |
| `GRAPHDB_ROUTER_CATALOG_PEERS` | 入口 | 至少三个私网节点地址的 JSON 映射 |
| `GRAPHDB_ADDR` | 入口 | 默认 `:8080` |

目录和数据副本仍使用 [Raft 配置](raft-ha.zh-CN.md)，均以 `graphdb serve` 启动；入口以 `graphdb serve-router` 启动。目录角色与数据角色互斥。普通 Raft 不设置这两个角色参数，单机不设置 Raft 参数。

## 迁移已有租户

扩容后通过显式迁移释放旧组容量；不会自动搬动已有租户。

```sh
curl -fsS http://127.0.0.1:8080/v1/cluster/moves \
  -H "Authorization: Bearer $GRAPHDB_RAFT_TOKEN" -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","target":"shard-b"}'

curl -fsS http://127.0.0.1:8080/v1/cluster \
  -H "Authorization: Bearer $GRAPHDB_RAFT_TOKEN"
```

`202` 表示目录已受理，完成条件为租户 `state=active`、`shard_id` 为目标、`move` 消失。迁移暂停这个租户的读写，入口返回可重试的 `503`，其他租户继续服务。已受理 WAL 和维护任务先排空，再冻结源组。迁移不丢弃已多数派受理的写入，也不会在源、目标同时允许写入。未完成的任务会延迟迁移；需要人工处理任务时，先取消迁移、恢复源服务，再使用现有租户任务 API。

迁移快照携带完整图对象、版本和幂等记录、终态 WAL 受理记录、租户代次及清理标记。目标重建自己的 writer fence，保留 `min_version` 和原接入批次查询身份。数据按 1MiB 分块多数派持久化，校验完整校验和后安装。安装、路由切换、激活和源数据删除分别有持久化阶段；目录 Leader 或数据 Leader 切换后自动重试。目标安装和应用位置使用现有恢复日志原子提交；进程在安装中崩溃时先回滚，再重放。

| `move.phase` | 含义 | 取消 |
| --- | --- | --- |
| `copy` | 排空、冻结、传输和安装目标 | 可以 |
| `activate` | 目录已切换位置，激活目标 | 不可以 |
| `cleanup` | 目标已服务，删除源数据 | 不可以 |
| `cancel` | 恢复源服务并推进路由代次 | 已在取消 |
| `discard` | 源已恢复，清理未启用的目标 | 已在取消 |

`move.error` 显示最近一次失败，协调器持续重试。取消只允许目录切换前，并推进源的路由代次，使旧协调器和旧请求失效。目标不可用时，源可以先恢复服务，目标清理等待其恢复。

```sh
curl -fsS -X POST http://127.0.0.1:8080/v1/cluster/moves/demo/cancel \
  -H "Authorization: Bearer $GRAPHDB_RAFT_TOKEN"
```

重复发起同一目标的未完成迁移会返回当前状态。响应丢失后先读取目录，确认状态后再重试。普通图写入沿用原幂等键，入口不盲目重放写入。图响应增加 `X-GraphDB-Shard-ID` 和 `X-GraphDB-Route-Epoch`；过期请求在数据组的入口和提交应用时都会被拒绝。迁移完成后只保留源组的归属隔离记录，源图对象和受理记录被删除。租户可以迁回旧组。

## 管理接口与兼容性

集群管理只在路由入口提供，必须带 `Authorization: Bearer <GRAPHDB_ROUTER_TOKEN>`。

| 接口 | 请求 | 返回 |
| --- | --- | --- |
| `GET /v1/cluster` | 无 | `200`，目录版本、分片、租户和迁移状态 |
| `POST /v1/cluster/shards` | `id`、`cluster_id`、`peers` | `202`，注册或更新同一组的地址；组身份不可变 |
| `POST /v1/cluster/placements` | `tenant_id`、可选 `target` | `202`，显式分配租户；不能覆盖已有位置 |
| `POST /v1/cluster/moves` | `tenant_id`、`target` | `202`，发起迁移 |
| `POST /v1/cluster/moves/{tenant}/cancel` | 无 | `202`，取消尚未切换的迁移 |

无管理令牌返回 `401`；冲突返回 `409`，`shard_conflict` 表示位置或操作冲突，`shard_epoch_changed` 表示数据组隔离了过期请求；`shard_unavailable` 通常为 `503`。图 API 的业务错误继续由所属组返回。节点 `/metrics` 单独采集，入口没有聚合指标；入口 readiness 校验目录可用，不表示所有数据组均可用。

租户 clone 将目标分配到源组，之后可单独迁移；已在其他组的 clone 目标返回冲突。restore-drill 仅允许清理临时目标，禁止 `cleanup=false` 留下未分配数据。备份、恢复和维护仍按租户路由，沿用 [单组 Raft 的限制](raft-ha.zh-CN.md)。

节点替换仍按原有 learner 加入、追赶、提升、移除流程执行。入口能从存活种子节点获知新 Leader 地址；替换后重新提交同一分片 ID/集群 ID 的完整地址映射，目录组替换后同步更新所有入口的种子配置。

已有单组 Raft 可以成为数据分片：先停止客户端写入，排空受理队列及任务，停止全组；保持原集群 ID、对象前缀和整个副本目录，在所有节点增加同一 `GRAPHDB_RAFT_SHARD_ID` 后重启。注册组，对每个已有租户提交指定 `target` 的 placements，等到 active 再切换入口。目录组使用新目录。分片目录首次启动即保存角色，之后不能删除角色配置绕过隔离，也不能直接改为其他分片或目录角色。单机导入仍需要新的 Raft 目录。

## 容量和验证边界

迁移期间该租户不可用，没有增量双写或不停机迁移。单次租户迁移的 JSON 封装和各组完整快照受 `GRAPHDB_RAFT_MAX_SNAPSHOT_BYTES` 约束，默认 512MiB；目录、源和目标都需足够预算。目标暂存块与安装数据会同时存在，需要额外磁盘和快照空间。分块避免超大单条提案，但导出/安装仍在内存中组装完整租户，超预算会停留在迁移状态，可在切换前取消。大租户容量、迁移延迟和跨故障域部署尚需专项资格验证。

运行验证见 [分片验收记录](sharding-validation.zh-CN.md)。
