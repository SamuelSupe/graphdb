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
| `operation_disabled` | reader 模式尝试写入 | 把写入/配置/任务变更发送到 writer。 |
| `reader_not_fresh` | reader 未追上所需版本 | 稍后重试，降低 `min_version` 或允许旧读。 |
| `write_admission_queue_timeout` | 写入队列已满 | 用相同幂等键重试并降低并发。 |
| `write_backpressure` | 系统背压 | 遵守 `Retry-After` 并检查 `reasons`。 |
| `commit_tail_too_long` | 可见 commit 过多 | 执行 compact 或等待自动 compact。 |
| `index_rebuild_running` | 索引重建阻塞写入 | 等待，或在合适时取消任务。 |
| `quota_exceeded` | 将超过租户配额 | 提高配额或删除数据。 |
| `idempotency_conflict` | 相同 key 对应不同 payload | 使用新 key，或重发完全相同 body。 |
| `idempotency_in_progress` | 另一个 writer 正在处理相同 key | 退避后用相同 key 重试完全相同的请求。 |
| `write_conflict` | PG head 在重试预算内持续变化 | 用相同幂等键重试；持续出现时检查 CAS 冲突率。 |
| `version_conflict` | `expected_version` 已不匹配 | 重新读取 head 并处理调用方前置条件，不要盲目重试。 |
| `coordinator_unavailable` | PostgreSQL coordinator 不可用 | 恢复 PG 连接；写入绝不会回退到 local 模式。 |
| `lease_held` | 本地模式重复/陈旧 writer 保护 | 确保每租户只有一个本地协调 writer。 |
| `index_stale` | 索引缺失或过期 | 重建索引，或在支持时允许 fallback。 |
| `repair_required` | 完整性问题阻断操作 | 执行 audit 和 repair。 |

## 429 处理

GGraphDB 使用 429 表示准入或背压。客户端应：

1. 读取 `Retry-After` 和 `retry_after_ms`；
2. 使用相同 `idempotency_key` 重试；
3. 重复出现时在来源侧降低并发；
4. 同一原因跨多个重试窗口持续时告警。

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

## Reader 新鲜度问题

现象：

- `reader_not_fresh`
- `/v1/control/reader-freshness` 中的 `version_lag`
- fleet readiness 未就绪

检查：

```sh
curl -sS "$READER/v1/control/reader-freshness" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/control/reader-fleet-readiness?min_ready=1" -H 'X-Tenant-ID: demo'
curl -sS "$READER/v1/control/reader-traffic-gate?min_ready=1" -H 'X-Tenant-ID: demo'
```

处理：

- 确认 reader 可访问对象存储；
- 检查 `GRAPHDB_POLL_INTERVAL` 和 `GRAPHDB_READER_CATCHUP_TIMEOUT`；
- 只在业务允许旧读时使用 `allow_stale=true`；
- reader 持续落后时，将其移出流量并重启。

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

- 从 writer 发起异步重建；
- `?deep=true` 只用于明确校验；
- 重建后，当 reader watermark 允许时，GC 可以清理孤立索引对象。

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

关注：按原因统计的写入背压、写入准入队列延迟、对象存储操作延迟和错误、
manifest CAS 冲突、commit tail、reader 可见版本/落后量、慢查询日志和
query profile 算子耗时。日志以 JSON 行输出，可按 `tenant_id`、`event`、
`query_id`、`task_id` 和 `reason` 搜索。
