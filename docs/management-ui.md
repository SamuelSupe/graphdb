# Management console

[简体中文](management-ui.zh-CN.md)

The current source embeds an English / Simplified Chinese console in the Go binary.
No Node, npm, frontend build or separate UI service is required at runtime.
Previously released 2.2.4 binaries do not contain this addition.

```sh
go build -o bin/graphdb ./cmd/graphdb
GRAPHDB_ADDR=127.0.0.1:8080 GRAPHDB_DATA_DIR=./.graphdb bin/graphdb serve
```

Open `http://127.0.0.1:8080/ui/`. With `GRAPHDB_ADMIN_ADDR`, only the management
listener serves `/ui/`; the data-only listener returns 404. `serve-router` also
serves `/ui/`. Select a tenant in the top bar. Language controls are at the bottom
of the sidebar; initial language follows the browser, and manual choice is saved.

## Large tenant registries

The top-bar picker searches tenant IDs on the server and loads 20 matches at a time.
The tenant table defaults to 25 rows, with 50/100-row options, ID search and next/previous
pages. Cluster placements also use server-side ID search and 25-row pages; the shard view
does not fetch every tenant placement. Page/filter state survives console refresh and language
changes. Switching tenants still asks before discarding unfinished edits.

Normal navigation and refresh do not fetch all tenants. Only the current page's metadata
and manifest are read. Legacy prefix discovery is opt-in on the tenant page because it scans
storage objects; select a discovered tenant there to use it. Search matches IDs, not names.

`GET /v1/tenants?limit=25&q=team` returns `tenants`, matching `total` and an optional
`next_cursor`. Pass that last tenant ID with the same query for the next page.
`GET /v1/cluster?limit=25&q=team` bounds the catalog's `tenants` map similarly. Limits are
1–200; supplying `q` or `cursor` alone defaults to 50. Requests without paging parameters
retain the original full response. Pages are live rather than snapshot-isolated. The ID
registry/catalog is still read in full, so this bounds detail I/O and browser rendering,
not the cost of registry storage itself.

## Pages

| Page | Operations |
| --- | --- |
| Overview | Status gauges, disk/task donuts, storage/concurrency/version charts; diagnostic, traffic-gate, writer-generation and metrics dialogs |
| Data explorer | Filtered entity/edge scans, cursor pagination, details, version-checked edits/deletes, neighbor graph, bounded graph view, snapshot export |
| Query workbench | JSON DSL, GraphQL and legacy DSL; examples, plan/profile, table/JSON/graph, next page, saved queries, running-query cancellation |
| Schema | Inspect/create/edit/delete entity types, relation types and relation field schemas |
| Tenants | Create/edit/select, enable/disable, clone, soft delete and permanent purge |
| Indexes | Create/drop, async rebuild, definitions, catalog and health |
| Tasks & backups | Filter/inspect/cancel/retry tasks; backup, restore/drill, audit, recovery, repair, cleanup, GC, automation status/reset |
| Ingestion | Atomic commit, collector batches, CSV/JSONL import, collector/batch status, dead letters/replay |
| Governance | Source policy, tenant configuration and storage usage |
| Cluster | Shard registration/drain/resume/removal; tenant assignment/migration/cancellation; router drain/resume; private Raft status/upgrade/membership/transfer/barrier/drain/resume |
| Connection | Separate data, management, router and private Raft URLs and tokens |

Records and responses use grouped properties, typed field summaries, tables, status
indicators and task progress/timelines. Entity/schema edits, writes, labels/metadata,
task parameters and governance settings use structured forms with repeatable fields
and items. Advanced JSON editing retains access to complex and unknown API fields;
switching views preserves the values, including large numeric tokens. Duplicate field
names and values that do not match their selected type are rejected before submission.
Raw JSON is collapsed by default. The query language editor and explicit JSON result
tab remain available. Long dialogs scroll their content while keeping the title and
action buttons visible. Controls and explanatory copy are bilingual; API identifiers,
user-defined field names and original data remain verbatim. Graph views show returned
data only, up to 100 nodes and 200 edges.

Collapsed response details, task checkpoints and raw JSON are rendered on first
expansion. Query results build only the selected table/JSON/graph view. Query-list
tabs fetch their own API, and inspecting a task fetches its latest detail without
reloading the entire list. Form input validation is coalesced after 150 ms of idle
time; submission, navigation and language switching read the current fields
synchronously, so pending validation cannot lose edits or submit stale values.

Overview charts use the current API snapshot and update on Refresh or tenant changes.
They show local health/readiness, data-filesystem used/available space, the selected
tenant's six largest storage categories plus the remaining categories, active requests
against each instance admission limit (queue counts separately), up to 100 most recent
tenant tasks, and published/visible/snapshot versions. Unknown or failed measurements
show unavailable rather than zero; valid empty samples show zero. Versions retain their
exact numeric labels. These charts do not imply historical throughput, task totals beyond
the sample, or cluster-wide health. Inspect opens the original response in a dialog;
raw diagnostics do not occupy the overview.

## Connections and authorization

Combined listeners use the page origin for both APIs. For split listeners, use
same-origin gateway paths. The [gateway example](../deploy/nginx/graphdb.conf.example)
maps `/admin/ui/` to `/ui/` for static assets; `/admin/v1/...` still requires
operator/admin authorization. Set data to the gateway origin and management to
`<origin>/admin`. Same-origin session cookies and entered Bearer tokens are supported.
Cross-origin calls require gateway CORS/preflight, including `Authorization`,
`Content-Type`, `X-Tenant-ID` and `X-Raft-Cluster`. The database adds no permissive CORS.

The console adds no authentication or arbitrary-destination server proxy, does not
relax roles, and does not expose private APIs. Router/Raft tokens are separate; Raft
also needs a cluster ID. Use an authorized management network/gateway for private APIs.
Protect static UI routes with your deployment's session login when required.
See [security deployment](security-deployment.md).

URLs, tenant and language are saved locally. Tokens stay in memory and clear on reload.
Query/ingestion drafts last for the page lifetime. Configuration editors warn before
discarding changes. Each query language keeps its own draft; selecting an example
does not replace the editor until Load example is clicked. Ingestion tabs keep valid
drafts and block switching while fields/JSON are unfinished. New repeatable items start
with empty/default values rather than copying existing records. Tabs support arrow,
Home and End keys. Connection settings group the core APIs separately from optional
router/Raft endpoints, which expand automatically when configured.
Requests never retry automatically; after a timeout inspect server
state first. Stopping browser waiting does not guarantee query cancellation; use the
process-local Running queries page to cancel on the server.

Changes show destination/payload before submission. Destructive actions require a
resource ID confirmation. Restore defaults to dry run. HTTP 202 means accepted;
check task/WAL status to confirm completion. Backend mode and backup requirements
still apply. Unsupported operations display the backend error.

Use current Chrome for lossless native JSON parsing of integers returned by the API.
This does not recover precision already lost in backend numeric fields; represent
precision-sensitive large field values as strings under the existing API contract.
Browser exports
use memory; use CLI/streaming clients for large exports.

## Development

Sources in `internal/adminui/static/` are embedded with `go:embed`; rebuild/restart after edits.

```sh
node --test internal/adminui/console_test.mjs
go test -mod=readonly ./internal/httpapi ./internal/sharding
```

Validate rendering/interactions in native Chrome and Linux services in OrbStack.
Static checks do not replace runtime browser validation.
