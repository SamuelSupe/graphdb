# 错误与故障排查

[English](errors-troubleshooting.md)

## 错误响应

所有非 2xx HTTP 错误使用：

```json
{
  "error": "message",
  "code": "stable_code",
  "message": "message",
  "retryable": false,
  "detail": {}
}
```

自动化应使用 `code`；`error` 仅为兼容旧客户端保留。完整合同见
[error_codes.md](../error_codes.md)。

## 常见错误码

| code | 含义 | 运维动作 |
| --- | --- | --- |
| `tenant_required` | 缺少 `X-Tenant-ID` | 添加租户 header。 |
| `invalid_tenant` | 租户 ID 无效 | 修正租户 ID 格式。 |
| `tenant_disabled` | 写入被阻断 | 启用租户或切换到活跃租户。 |
| `tenant_deleted` | 租户已软删除 | 恢复/克隆，或停止使用该租户。 |
| `operation_disabled` | 操作被禁用 | 检查本地服务配置和操作权限。 |
| `reader_not_fresh` | reader 未追上所需版本 | 稍后重试，降低 `min_version` 或允许旧读。 |
| `write_admission_queue_timeout` | 写入队列已满 | 用相同幂等键重试并降低并发。 |
| `write_backpressure` | 系统背压 | 遵守 `Retry-After` 并检查 `reasons`。 |
| `commit_tail_too_long` | 可见 commit 过多 | 执行 compact 或等待自动 compact。 |
| `index_rebuild_running` | 索引重建阻塞写入 | 等待，或在合适时取消任务。 |
| `quota_exceeded` | 将超过租户配额 | 提高配额或删除数据。 |
| `idempotency_conflict` | 相同 key 对应不同 payload | 使用新 key，或重发完全相同 body。 |
| `idempotency_in_progress` | 另一个请求正在处理相同 key | 退避后用相同 key 重试完全相同的请求。 |
| `write_conflict` | 并发提交或前置条件冲突 | 查看本地状态并保持幂等键；带前置条件的写入重新读取 head 并解决冲突。 |
| `version_conflict` | `expected_version` 已不匹配 | 重新读取 head 并处理调用方前置条件，不要盲目重试。 |
| `lease_held` | 本地模式重复/陈旧 writer 保护 | 确保每租户只有一个本地协调 writer。 |
| `index_stale` | 索引缺失或过期 | 重建索引，或在支持时允许 fallback。 |
| `repair_required` | 完整性问题阻断操作 | 执行 audit 和 repair。 |

## 429 处理

GGraphDB 使用 429 表示准入或背压。客户端应：

1. 读取 `Retry-After` 和 `retry_after_ms`；
2. 使用相同 `idempotency_key` 重试；
3. 重复出现时在来源侧降低并发；
4. 同一原因跨多个重试窗口持续时告警。

同步 WAL 在 fsync 完成后受理请求。重启后服务从同一本地目录恢复受理记录；
`recovery_pending` 表示恢复尚未完成。达到 WAL 高水位时拒绝新准入，已有记录继续通过
返回的 `status_url` 查询。检查磁盘空间、目录权限和恢复日志；对象备份故障不影响在线主存储。

示例：

```json
{
  "code": "write_backpressure",
  "retry_after_ms": 2000,
  "reasons": [
    {"code": "commit_tail_too_long", "current": 1501, "threshold": 1500}
  ]
}
```

## 本地读取新鲜度

`reader_not_fresh` 或 `/v1/control/reader-freshness` 中的 `version_lag` 表示本地
读取视图尚未满足请求版本，不代表存在独立 reader 服务。

```sh
curl -sS "$BASE/v1/readiness"
curl -sS "$BASE/v1/control/reader-freshness" -H 'X-Tenant-ID: demo'
curl -sS "$BASE/v1/control/reader-traffic-gate" -H 'X-Tenant-ID: demo'
```

- 确认本地盘可读写、WAL 恢复完成；`202` 仅表示接管，需要查询批次是否 committed。
- 核对请求的 `min_version` 与已提交版本，检查 `GRAPHDB_READER_CATCHUP_TIMEOUT`。
- 仅在业务允许旧读时使用 `allow_stale=true`，不能通过降低版本要求替代写入完成检查。
- 持续落后时检查 WAL、索引任务和本地恢复日志，再决定是否重启进程。

`reader-fleet-readiness` 是兼容路由，仅汇总本地进程的 reader 状态记录；不发现其他实例，
也不提供集群就绪保证。常规排障使用上面的单实例接口。

## 慢查询或高成本查询

检查：

```sh
curl -sS "$READER/v1/queries/running" -H 'X-Tenant-ID: demo'
```

处理：

- 添加 `timeout_ms` 和 `cost_limit`；
- 添加 `kind`、关系类型和已索引过滤条件；
- 使用 `EXPLAIN` 或 `PROFILE`；
- 取消异常的进程内查询：

```sh
curl -sS -X DELETE "$READER/v1/queries/running/<query-id>" \
  -H 'X-Tenant-ID: demo'
```

## 索引问题

检查：

```sh
curl -sS "$READER/v1/indexes/health" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/indexes" -H 'X-Tenant-ID: demo'
```

处理：

- 通过本实例管理接口发起异步重建；
- `?deep=true` 只用于明确校验；
- GC 延迟回收仍被本地活跃读视图保护的文件；检查 `checkpoint.deferred_files`，扫描结束不代表所有孤儿文件均已删除。

## 完整性问题

检查：

```sh
curl -sS "$WRITER/v1/control/integrity-audit?deep=true" \
  -H 'X-Tenant-ID: demo'
```

repair 预演：

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":false}'
```

确认计划动作后再 apply：

```sh
curl -sS -X POST "$WRITER/v1/control/repair" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d '{"apply":true}'
```

## 日志和指标

指标：

```sh
curl -sS "$BASE/metrics"
```

关注：按原因统计的写入背压、写入准入队列延迟、本地文件存储操作延迟和错误（部分指标保留 `object_store` 名称）、
manifest CAS 冲突、commit tail、reader 可见版本/落后量、慢查询日志和
query profile 算子耗时。日志以 JSON 行输出，可按 `tenant_id`、`event`、
`query_id`、`task_id` 和 `reason` 搜索。
