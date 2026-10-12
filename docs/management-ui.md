# Management console

**GGraphDB 2.3.1** · [简体中文](management-ui.zh-CN.md) · [Documentation](README.md) · [API guide](user/README.md)

The console is embedded in the Go binary and supports English and Simplified
Chinese. Runtime needs no Node, npm, frontend build or separate UI service.
Previously released 2.2.4 binaries do not include it.

[Open the console](#open-the-console) · [Page map](#pages) · [Tenant paging](#large-tenant-registries) · [Connections](#connections-and-authorization)

## Open the console

1. Start the service from the repository root:

   ```sh
   go build -o bin/graphdb ./cmd/graphdb
   GRAPHDB_ADDR=127.0.0.1:8080 GRAPHDB_DATA_DIR=./.graphdb bin/graphdb serve
   ```

2. Open **[http://127.0.0.1:8080/ui/](http://127.0.0.1:8080/ui/)**.
3. Select a tenant in the top bar. Change language at the bottom of the sidebar.
   The first language follows the browser; a manual choice is saved locally.

| Listener | Where to open the UI |
| --- | --- |
| Combined | `/ui/` on the normal service address |
| Split with `GRAPHDB_ADMIN_ADDR` | `/ui/` on the management listener; the data listener returns 404 |
| `serve-router` | `/ui/` on the router address |

## Pages

| Page | Main workflows |
| --- | --- |
| Overview | Current status, disk, storage, concurrency, task and version charts; diagnostics in Inspect dialogs |
| Data explorer | Entity/edge filters, cursor pages, details, version-checked edits/deletes, neighbors, bounded graph and snapshot export |
| Query workbench | JSON DSL, GraphQL and legacy DSL; examples, plan/profile, table/JSON/graph results, next page, saved queries and process-local cancellation |
| Schema | Inspect/create/edit/delete entity types, relation types and relation field schemas |
| Tenants | Create/edit/select, enable/disable, clone, soft delete and permanent purge |
| Indexes | Create/drop, asynchronous rebuild, definitions, catalog and health |
| Tasks & backups | Inspect/filter/cancel/retry; backup, restore/drill, audit, recovery, repair, cleanup, GC and automation status/reset |
| Ingestion | Atomic commits, collector batches, CSV/JSONL import, collector/batch status, dead letters and replay |
| Governance | Source policy, tenant configuration and storage usage |
| Cluster | Shard registration/drain/resume/removal; tenant assignment/migration/cancellation; router drain/resume; private Raft status, upgrades, membership, transfer, barrier and drain/resume |
| Connection | Separate data, management, router and private Raft URLs and tokens |

## Read the overview charts

Charts use the current API snapshot and update on Refresh or tenant changes.

| Chart | Scope and meaning |
| --- | --- |
| Health / readiness | Local instance status |
| Disk | Used and available space on the data filesystem |
| Tenant storage | Six largest storage categories plus the remaining categories combined |
| Concurrency | Active requests against each instance admission limit; queue counts shown separately |
| Tasks | Up to 100 most recent tasks for the selected tenant |
| Versions | Published, visible and snapshot versions, with exact numeric labels |

Missing or failed measurements show **unavailable**; valid empty samples show zero.
Charts do not establish historical throughput, totals beyond the task sample or
cluster-wide health. Inspect opens diagnostic, traffic-gate, writer-generation
and metrics responses in dialogs.

## Edit and inspect records

Records use grouped properties, typed field summaries, tables, status indicators
and task progress/timelines. Entity/schema edits, writes, labels/metadata, task
parameters and governance settings use structured forms with repeatable fields.

| Action | Behavior |
| --- | --- |
| Add a field or array item | Starts empty or with defaults; does not copy an existing record |
| Use advanced JSON | Supports complex/unknown fields; switching views preserves values and large numeric tokens |
| Submit invalid fields | Duplicate names or values incompatible with the selected type block submission |
| Inspect raw responses | Raw JSON is collapsed by default; explicit query JSON results remain available |
| Use a long dialog | Content scrolls while its title and action buttons stay visible |
| Navigate tabs | Arrow, Home and End keys switch tabs |

Controls and explanatory copy are bilingual. API identifiers, custom field names
and original data remain verbatim.

### Complete an operation

1. Review the destination and payload shown before submission.
2. For a destructive action, enter the resource ID. Restore defaults to dry run.
3. For HTTP 202, inspect task or WAL status to confirm completion.

Requests do not retry automatically. After a timeout, inspect server state before
resubmitting. Stopping browser waiting does not guarantee server cancellation;
use Running queries on the relevant process. Deployment and backup requirements
still apply; unsupported operations show the backend error.

## Large tenant registries

| Surface | Search and page size |
| --- | --- |
| Top-bar picker | Server-side ID search; 20 matches per load |
| Tenant table | 25 rows by default, 50/100 options, ID search and next/previous pages |
| Cluster placements | Server-side ID search and 25-row pages; the shard view does not fetch all placements |
| Legacy discovery | Explicit opt-in on the tenant page; scans storage objects; select a discovered tenant there |

Refresh and language changes preserve page/filter state. Tenant changes ask
before discarding unfinished edits. Search matches **IDs, not names**. Normal
navigation reads metadata/manifests only for the current page.

### API paging contract

```http
GET /v1/tenants?limit=25&q=team
GET /v1/cluster?limit=25&q=team
```

| Parameter / response | Contract |
| --- | --- |
| `limit` | 1–200; supplying only `q` or `cursor` defaults to 50 |
| `q` | Tenant ID substring search |
| `next_cursor` | Last tenant ID; pass it as `cursor` with the same query for the next page |
| Tenant response | `tenants`, matching `total`, optional `next_cursor` |
| Cluster response | Similarly bounds the catalog's `tenants` map |
| No paging parameters | Original full response remains supported |

Pages are live, without snapshot isolation. The ID registry/catalog is still read
in full: paging bounds detail I/O and browser rendering, not registry storage cost.
See [tenant API examples](user/tenant-config.md).

## Connections and authorization

| Deployment | Connection settings |
| --- | --- |
| Combined listener | Page origin for data and management APIs |
| Split behind a same-origin gateway | Data: gateway origin; management: `<origin>/admin` |
| Router / private Raft | Separate URLs and tokens; Raft also needs a cluster ID |
| Cross-origin gateway | Configure CORS/preflight for `Authorization`, `Content-Type`, `X-Tenant-ID` and `X-Raft-Cluster` |

The [gateway example](../deploy/nginx/graphdb.conf.example) maps `/admin/ui/` to
`/ui/` for static assets. `/admin/v1/...` still requires operator/admin authorization.
Same-origin session cookies and entered Bearer tokens are supported. The database
adds no permissive CORS.

The console adds no authentication system, arbitrary-destination server proxy or
role relaxation, and does not expose private APIs. Reach private APIs through an
authorized management network/gateway; protect static UI routes with session login
when required. See [security deployment](security-deployment.md).

## Saved settings and drafts

| State | Lifetime / behavior |
| --- | --- |
| URLs, tenant, language | Saved locally |
| Tokens | Memory only; clear on page reload |
| Query drafts | Page lifetime, separately for each language; examples replace content only after Load example |
| Ingestion drafts | Page lifetime; valid drafts survive tab changes; unfinished fields/JSON block switching |
| Configuration edits | Warn before discarding changes |
| Connection groups | Core APIs separate from optional router/Raft endpoints; configured optional groups expand automatically |

## Display and export limits

| Area | Limit |
| --- | --- |
| Graph views | Returned data only, up to 100 nodes and 200 edges; not the tenant's full graph |
| Numeric precision | Use current Chrome for native lossless parsing of API integers; precision-sensitive field values should use strings under the API contract |
| Browser exports | Use memory; large exports should use CLI or streaming clients |

Lossless browser parsing cannot recover precision already lost in backend numeric fields.

## Development

Sources in `internal/adminui/static/` use `go:embed`; rebuild and restart after edits.

```sh
node --test internal/adminui/console_test.mjs
go test -mod=readonly ./internal/httpapi ./internal/sharding
```

Validate rendering/interactions in native Chrome and Linux services in OrbStack.
Static checks do not replace runtime browser validation.

<details>
<summary>Rendering and request behavior</summary>

Collapsed details, task checkpoints and raw JSON render on first expansion.
Query results build only the selected table/JSON/graph view; query-list tabs fetch
only their own API. Task inspection fetches the latest detail without reloading
the list. Form validation coalesces after 150 ms idle; submission, navigation and
language changes read current fields synchronously to preserve edits.

</details>
