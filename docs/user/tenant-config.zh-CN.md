# 租户与配置

[English](tenant-config.md)

租户生命周期 API 不受租户 header 限制；数据 API 仍通过 \`X-Tenant-ID\`
限定租户。

## 生命周期

创建：

```sh
curl -sS -X POST "$WRITER/v1/tenants" \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "demo",
    "name": "Demo",
    "labels": {"env": "dev"}
  }'
```

列出：

```sh
curl -sS "$WRITER/v1/tenants"
curl -sS "$WRITER/v1/tenants?include_legacy=true"
```

查看：

```sh
curl -sS "$WRITER/v1/tenants/demo"
```

更新元数据：

```sh
curl -sS -X PUT "$WRITER/v1/tenants/demo" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Demo CMDB","labels":{"env":"prod"}}'
```

禁用和启用：

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/disable"
curl -sS -X POST "$WRITER/v1/tenants/demo/enable"
```

软删除：

```sh
curl -sS -X DELETE "$WRITER/v1/tenants/demo"
```

软删除后清理对象数据：

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/purge?force=true"
```

克隆：

```sh
curl -sS -X POST "$WRITER/v1/tenants/demo/clone" \
  -H 'Content-Type: application/json' \
  -d '{"target_tenant_id":"demo-repro","name":"Demo Repro"}'
```

## Source Policy

读取：

```sh
curl -sS "$WRITER/v1/source-policy" -H 'X-Tenant-ID: demo'
```

更新：

```sh
curl -sS -X PUT "$WRITER/v1/source-policy" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/source-policy.json
```

推荐基线：

```json
{
  "default_priority": 0,
  "sources": [
    {"name": "manual", "priority": 1000},
    {"name": "agent", "priority": 100},
    {"name": "cloud", "priority": 50},
    {"name": "aws", "priority": 50}
  ],
  "field_aliases": [
    {
      "source": "aws",
      "kind": "host",
      "aliases": {
        "privateIpAddress": "private_ip",
        "instanceName": "hostname"
      }
    },
    {
      "source": "agent",
      "aliases": {
        "host_name": "hostname"
      }
    }
  ],
  "field_priorities": [
    {
      "source": "aws",
      "kind": "host",
      "fields": {
        "hostname": 1200,
        "private_ip": 900
      }
    }
  ]
}
```

\`field_aliases\` 只在写入时重写顶层实体字段。保存的图、索引、查询 DSL、
scan 和 export API 只暴露规范字段名。\`field_priorities\` 同样使用规范字段名，
只改变字段级合并归属，不改变实体级 source priority。

## Tenant Config

读取：

```sh
curl -sS "$WRITER/v1/tenant-config" -H 'X-Tenant-ID: demo'
```

更新：

```sh
curl -sS -X PUT "$WRITER/v1/tenant-config" \
  -H 'X-Tenant-ID: demo' \
  -H 'Content-Type: application/json' \
  -d @examples/tenant-config.json
```

配置区段：

- \`backpressure\`：对象延迟阈值、CAS 冲突窗口、commit tail 阈值和重试提示；
- \`quota\`：租户最大实体数和边数；
- \`maintenance\`：自动 compact、GC 间隔、保留策略、小文件阈值和孤立索引清理；
- \`indexes\`：自动重建行为。

根据字段不同，\`0\` 通常表示没有配额或关闭阈值。

## Tenant Usage

```sh
curl -sS "$WRITER/v1/tenant-usage" -H 'X-Tenant-ID: demo'
```

使用 tenant usage 观察对象数、字节数和分类增长，再决定是否启用硬配额。

## CLI

```sh
go run ./cmd/graphdb list-tenants
go run ./cmd/graphdb tenant demo
go run ./cmd/graphdb create-tenant demo
go run ./cmd/graphdb set-tenant-metadata demo metadata.json
go run ./cmd/graphdb disable-tenant demo
go run ./cmd/graphdb enable-tenant demo
go run ./cmd/graphdb delete-tenant demo
go run ./cmd/graphdb purge-tenant demo --force
go run ./cmd/graphdb clone-tenant demo demo-repro
go run ./cmd/graphdb source-policy demo
go run ./cmd/graphdb set-source-policy demo examples/source-policy.json
go run ./cmd/graphdb tenant-config demo
go run ./cmd/graphdb set-tenant-config demo examples/tenant-config.json
go run ./cmd/graphdb tenant-usage demo
```

若要从配置存储中永久清理所有受管和 legacy 租户，使用带保护的脚本，并先
用 \`--dry-run\` 检查目标：

```sh
GRAPHDB_BIN=./graphdb scripts/purge_all_tenants.sh --dry-run
GRAPHDB_BIN=./graphdb scripts/purge_all_tenants.sh
```
