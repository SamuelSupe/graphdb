# Raft 不停机滚动升级

滚动升级适用于至少三个投票副本的 Raft 组。分片的目录组和每个数据组分别遵守同一流程，入口至少两个 router。Compose 的 HAProxy 持续解析 Docker 服务名，避免替换容器改变 IP 后仍连接旧地址；其他部署需配置对应 DNS/服务发现。每次只替换一个进程，保留节点 ID、配置、数据目录和 Raft 日志；该副本追赶并恢复健康后才操作下一个。始终保留多数派，正常图请求继续执行强一致屏障。

Leader 切换、连接关闭或健康检查传播期间，单次请求仍可能遇到可重试错误。写客户端保留原幂等键或 WAL 批次身份，不能将响应丢失视为未提交。服务端不会自动重放结果不确定的写请求。单机单进程升级仍需重启；本流程不能提供单机不停机保证。跨宿主机按现有决定保留待验收。

## 兼容窗口

`protocol_version=1` 约束传输、持久化编码和命令执行语义，不等于产品版本字符串。兼容补丁可以保留协议 1，但发布前仍需真实双版本验收。新增命令、不兼容编码或执行语义改变时，必须使用新的协议及单独升级方案，不能因为 JSON 能解析就继续发送协议 1。

| 来源 | 目标 | 方式 |
| --- | --- | --- |
| 第四轮修复 `d7b535b0` 对应的已验收二进制 | 本轮 `2.1.2-raft-rolling1` 候选 | 首次桥接，临时接收 legacy 通信；以本轮实测记录为准 |
| 本轮协议 1 程序 | 同程序重启、配置滚动生效 | 严格协议检查和滚动协调脚本 |
| `2.1.2-raft-diagnostics1` 已验收二进制 | `2.1.2-raft-isolation9` 候选 | 协议 1 的真实单组/分片混部、反向快照、有限回退和严格滚动协调；[本轮证据](raft-isolation-validation-2026-10-02.zh-CN.md) |
| 诊断开发基线 `b22cac8b046a695f14e7d02909d6570f546e989a` 的发行门禁构建 | `v2.2.1` 发行二进制 | 协议 1；以包内 `release/evidence/raft-gate/rolling/metadata.json` 的 PASS 与来源/目标 SHA256 为准，发行流水线同时核对目标与下载包二进制一致 |
| 未做双版本验收的未来补丁 | 任意版本 | 尚未资格验证 |
| 更早的 Raft 程序、协议不同的版本 | 本轮或未来版本 | 不套用此流程，保持相应版本的维护或迁移要求 |

桥接来源二进制 SHA256：`cb3688cfd2433fd644458a42fffc35211b9471abdd261d7fd22cfcbbe706783a`；源码摘要：`0fece2e1612a961f9e48f18a001a97dea3599971daf69d1cbd07bde3f2c79e13`。它已包含终态 WAL 摘要、恢复分块、成员竞态修复和导入源 SHA256 语义，见 [第四轮审核](raft-p0-p1-review4-2026-10-02.zh-CN.md)。更早的程序不能据此认定为兼容。

本次诊断候选至隔离候选的来源二进制 SHA256 为 `d6bff204faa791f06c903780c3924b36031a34c93989b13127d98aba514c17a3`，目标为 `857b4d6c68a0bea7d0fa95a3e6024b4f5b85eab6fe5ee59c24b22200c95fcc3b`。该来源已具备摘流接口。持续业务验证最终逻辑失败为 0，记录 6 次临时重试；资格仅覆盖此窗口和协议 1。

上表诊断开发基线不是已发布的单机 `v2.1.2`。从单机 2.1.2 转到 Raft，应导入或恢复到新副本目录，不能把单机进程作为 Raft 成员滚动替换。发行门禁从固定提交重新构建来源并验证整个窗口，不将先前 dirty 候选的二进制摘要作为发行来源。

新程序默认拒绝没有协议声明或声明不支持协议的 Raft 通信，支持的最高协议为 3，缺省配置仍为 1。历史磁盘日志/快照的缺省字段按兼容旧编码读取；兼容模式新日志/快照外层显式携带协议 1，业务载荷不变。桥接来源程序忽略新增外层字段的行为需要实际二进制验收。严格模式下，没有协议声明的旧程序即使保留原目录，也不能参与多数派通信。

## 私有运维接口

`/raft/*` 请求携带 `Authorization: Bearer <令牌>` 和 `X-Raft-Cluster: <集群 ID>`，只在受保护的运维网络或本机隧道访问。默认 Compose 不发布私有监听器。

| 接口 | 语义 |
| --- | --- |
| `GET /raft/status` | 节点、集群、构建身份、协议、投票成员、提交/应用位置、错误和 draining 状态 |
| `GET /raft/upgrade` | 检查全组可达、无故障、无其他摘流节点、已追赶、无 learner 或联合配置，返回 `safe_to_restart` 与原因 |
| `POST /raft/drain` | 阻止新本地提案进入、等待已有提案完成，复制一条空操作并等待所有投票副本应用后重新检查全组；Leader 交接到已追赶投票节点后返回 `204`。复制继续运行，本节点 readiness 返回 `503` |
| `POST /raft/upgrade/barrier` | 内部摘流探针：只由 Leader 多数派提交并应用一条空操作，不修改租户图；单次多数派成功本身不许可停副本，drain 还等待全部投票副本应用 |
| `POST /raft/resume` | 放弃升级时恢复本进程 readiness；维护仍进行时拒绝并发操作 |
| `POST /raft/transfer`，正文 `{"target":2}` | 请求向健康投票节点交接；新版 follower 也可向旧 Leader 发起。未追赶或不安全返回 `409` |

摘流失败不得停止节点。超时或响应丢失后先查询实际状态，不据此推断交接失败。重启清除进程 draining 状态，追赶到当前提交位置后重新准入。检查只是当前观测，不是分布式升级锁：部署控制器必须全组串行，禁止不同主机或 inventory 并行升级同一组；升级时暂停成员变更和租户迁移。

Follower 将正常数据请求经已认证的私有监听器转发到 Leader，由 Leader 执行多数派及应用屏障。转发最多三跳，网络错误和已提交提案失去领导权不会触发自动重放 mutation。就绪检查允许未摘流 follower 通过 Leader 确认服务；健康响应仍描述本地节点，不能使用 readiness 判断其是否是 Leader。隔离节点不能通过该路径提供过时数据。

## 首次桥接

1. 核对上表来源二进制摘要、配置和目录，备份并保留原程序，暂停其他升级、成员变更和迁移，保持业务入口工作。
2. 保留原 Leader，逐个替换两个 follower。新版暂时设置 `GRAPHDB_RAFT_ALLOW_LEGACY_PROTOCOL=true`，每次等待全组追赶。来源没有摘流接口，操作前确认其余两票健康。
3. 向新版 follower 请求 `/raft/transfer`，target 为该新版投票节点。等待全组观察新 Leader、入口健康，再替换原 Leader。`409` 表示尚不满足条件，等待追赶再检查。
4. 目录组和全部数据组完成后，通过冗余入口逐个替换 router。首次升级的旧 router 无摘流接口，先通过负载均衡控制面摘流并等待在途请求完成；不能同时重启两个 router。
5. 全部升级后将 legacy 开关改为 `false`，用新版 drain 接口逐个重启、追赶，关闭旧通信窗口。该开关只临时许可已经校验的来源，不能证明任意旧版本兼容。

## 日常滚动升级

[`scripts/raft_rolling_upgrade.py`](../scripts/raft_rolling_upgrade.py) 使用 Python 标准库，先检查整个 inventory，再逐个摘流、替换、核对目标构建并等待全组健康；失败即停止后续节点。目标版本的兼容资格由发布验收保证，脚本不会自动证明兼容或拉取未知镜像。

inventory 为 `{"groups":[{"cluster_id":"graphdb-ha","nodes":[...]}]}`，每个 node 包含 `id`、运维端可访问的私有 `url` 和 `restart` 参数数组。URL 可经 SSH/TLS 隧道转发；例如某节点：

```json
{"id":1,"url":"http://127.0.0.1:19081","restart":["docker","compose","-p","graphdb-ha","-f","docker-compose.raft.yml","up","-d","--no-deps","--no-build","--force-recreate","node1"]}
```

group 可配置 `protocol_version` 为 1、2 或 3，缺省为 1；协调器要求组内所有成员保持该协议，不能用一次滚动操作混合或激活不同协议。启用协议 3 前，先在原协议下升级全部副本和 router，并确认每个副本的 `protocol_max` 至少为 3，再按独立变更启用。协议 3 提案持久化后，最高支持协议为 2 的旧程序不能原目录回退。协议 3 同构建串行重启的本机证据见[本轮验证](raft-isolation-validation-2026-10-02.zh-CN.md)。

inventory 必须包含该组全部投票节点。restart 只替换对应进程并复用原数据卷，不经过 shell。相同 inventory 的本机文件锁防止本机重复执行；不同宿主机的串行约束由部署控制器保证。

每个副本 drain 成功后默认等待 7 秒，使示例 HAProxy 的每秒检查、连续三次失败摘流策略完成传播，再执行 restart。顶层 `replica_drain_seconds` 可按实际负载均衡传播时间调整；这段等待不代替全部投票节点追赶和 Leader 交接检查。

```sh
export GRAPHDB_RAFT_TOKEN='<现有令牌>'
export GRAPHDB_HA_IMAGE='<已经验证的目标镜像>'
python3 scripts/raft_rolling_upgrade.py --inventory upgrade.json
python3 scripts/raft_rolling_upgrade.py --inventory upgrade.json --execute \
  --target-version '<目标构建版本>' --target-commit '<目标构建提交>' \
  --report upgrade-results.json
```

分片 inventory 包含目录组和全部数据组，可追加 `routers` 数组，每项为 `id`、router 运维 URL、`restart` 数组，至少两个。脚本逐个调用带令牌的 `POST /v1/router/drain`，等待健康检查摘流后重启；默认等 7 秒，顶层 `router_drain_seconds` 必须覆盖实际入口的检查传播时间。重开验证构建和 readiness 再继续；放弃升级可调用 `POST /v1/router/resume`。令牌默认取 `GRAPHDB_RAFT_TOKEN`，数据组用 `token_env`、router 用顶层 `router_token_env` 指定其他环境变量名。`--force` 用于同构建配置生效或滚动重启，否则跳过已经是目标构建的进程。

副本/router 未恢复时保留其目录和日志，不继续升级。相同协议且经过反向验收的程序才允许原目录回退；本轮反向资格仅针对上表来源。不同协议或未验证来源不能无损原目录降级。

## 验收入口

首次桥接的版本窗口、负载和限制见 [滚动验收报告](raft-rolling-validation-2026-10-02.zh-CN.md)；诊断候选至当前目标的最终结果见 [故障隔离与迁移验收](raft-isolation-validation-2026-10-02.zh-CN.md)。

[`scripts/raft_rolling_gate.py`](../scripts/raft_rolling_gate.py) 绑定真实来源二进制，验收两个不同二进制的混部、换主、旧程序安装新版快照、导入任务、单组与分片持续读写、关闭 legacy 窗口以及严格模式协调脚本。负载按原幂等身份重试，记录尝试次数、重试和最终失败；每个副本轮流成为 Leader 后核对自己的强一致导出。临时独立项目和卷结束后清理。

```sh
export GRAPHDB_ROLLING_BASE_IMAGE='<上述来源候选镜像>'
export GRAPHDB_ROLLING_TARGET_IMAGE='<本轮目标镜像>'
export GRAPHDB_ROLLING_OUTPUT='<新的证据目录>'
export GRAPHDB_RAFT_TOKEN='<本次临时验收令牌>'
DOCKER_CONTEXT=orbstack python3 scripts/raft_rolling_gate.py
```

本机容器门禁不能替代跨宿主机、真实容量及最终发布资产验收；也不能将本轮结果推广到所有历史或未来版本。
