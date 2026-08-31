# Production Security Boundary

[中文](security-deployment.zh-CN.md)

GGraphDB 1.1 keeps authentication and authorization at the gateway or service
mesh. The database does not treat `X-Tenant-ID` as proof of identity. A
production deployment must prevent clients from reaching either GGraphDB
listener directly.

## Secure Defaults

- `GRAPHDB_PPROF_ENABLED=false` by default.
- The compatibility listener at `GRAPHDB_ADDR` never exposes pprof.
- Set `GRAPHDB_ADMIN_ADDR` to create separate data and admin listeners.
- Enabling pprof requires a distinct `GRAPHDB_ADMIN_ADDR`; startup fails if it
  is absent or equal to `GRAPHDB_ADDR`.
- PostgreSQL coordination never falls back to an uncoordinated writer. Direct
  commits fail closed when PostgreSQL is unavailable; the 1.3 WAL ingest path
  may finish local durable admission until its bounded WAL high-water policy
  rejects new payloads.

Recommended production settings:

```sh
GRAPHDB_ADDR=0.0.0.0:8080
GRAPHDB_ADMIN_ADDR=127.0.0.1:8081
GRAPHDB_PPROF_ENABLED=false
```

Bind the admin listener to loopback, a private pod interface, or a dedicated
management network. Enable pprof temporarily only while diagnosing an incident.

## Listener Responsibilities

When `GRAPHDB_ADMIN_ADDR` is unset, GGraphDB keeps the 1.0-compatible combined
listener, except that pprof remains disabled. When it is set:

| Listener | Surface |
| --- | --- |
| Data | commits, ingest/import, graph reads, schema reads, query, saved-query execution |
| Admin | tenant lifecycle, policies/config, task control, indexes, maintenance/control, metrics, optional pprof |
| Both | health, readiness, OpenAPI |

Admin routes intentionally return `404` on the data listener. Data mutation and
query routes return `404` on the admin listener.

## Gateway Contract

The reference NGINX configuration is
`deploy/nginx/graphdb.conf.example`. Its identity provider contract is:

1. Validate the client bearer token or mTLS identity.
2. Reject requests without an authorized tenant, and return that tenant as
   `X-GraphDB-Tenant-ID` only on a successful auth response.
3. Enforce the roles supplied in `X-GraphDB-Required-Roles`; the admin auth
   subrequest allows only `admin` and `operator`.
4. The gateway removes every client-supplied `X-Tenant-ID` and writes the
   verified tenant value.
5. Return `401` or `403` from the auth service when identity, tenant, or role
   checks fail.

The example terminates TLS 1.2/1.3 and exposes admin paths under `/admin/` only
at the gateway. Role enforcement stays in the auth service because NGINX
rewrite conditions run before `auth_request`. Adjust role-to-route policy for
your identity platform; do not forward a tenant header supplied by the caller.

The reference role matrix is:

| Route class | Allowed roles |
| --- | --- |
| Graph reads, schema reads, and queries | `reader`, `writer`, `operator`, `admin` |
| Commit, ingest, and import | `writer`, `operator`, `admin` |
| `/admin/` lifecycle, policy, task, index, and control routes | `operator`, `admin` |

The identity provider must treat each comma-separated value as an any-of
requirement and must evaluate the original method, URI, identity, and tenant
together.

For the 1.3 WAL status route, the gateway must preserve and validate
`/v1/ingest/writers/{writer_id}/...`, then route the request to the registered
writer whose stable `GRAPHDB_INSTANCE_ID` equals `writer_id`. Unknown writer
IDs must not fall through to a random writer pool. The reference NGINX file
shows explicit writer-A and writer-B routes that operators extend for their
fleet.

## Required Network Controls

- Expose only the TLS gateway to clients.
- Deny direct network access to the data and admin listeners.
- Restrict the PostgreSQL coordination schema and object-store credentials to
  GGraphDB service identities.
- Give every 1.3 WAL writer a unique stable `GRAPHDB_INSTANCE_ID` and an
  independently protected persistent WAL volume. Do not share a WAL volume
  between writers.
- Give 1.0 readers read-only object-store credentials. Revoke all 1.0 writer
  routes and write credentials before PostgreSQL bootstrap.
- Protect metrics because tenant labels and operational state may be sensitive.
- Send access and audit logs to an append-only or centrally controlled sink.

GGraphDB does not currently implement row-level authorization inside a tenant.
If users need sub-tenant visibility, enforce it upstream or use separate
tenants.
