# Changelog

All notable GGraphDB changes are recorded here. Versions follow semantic
versioning; release tags and binaries expose the exact build commit and date.

## [1.1.1] - 2026-07-30

### Fixed

- Treat ingest batches whose resulting logical graph is unchanged as no-op
  writes, while distinguishing `logical_noop` from `idempotent_replay` in the
  API, Go SDK, metrics, and audit output.
- Copy only graph maps affected by a storage mutation instead of cloning
  unrelated entity, edge, schema, and index maps before every commit.
- Build conflict-to-item associations with a batch index instead of repeatedly
  scanning large ingest requests.
- Encode dual-key ingest metadata once, publish the idempotency and batch
  records concurrently, and probe legacy compatibility keys with bounded
  concurrency while retaining current-key precedence.

### Compatibility

- The object layout and stored ingest record contract remain compatible with
  GGraphDB 1.1.0. `skip_reason` is an additive response field and is not
  persisted as idempotency state.

## [1.1.0] - 2026-07-27

### Added

- Domain-neutral entity type aliases, entity labels, relation property schemas,
  JSONL/CSV bulk import, and bounded graph pattern queries.
- GraphQL query transport with documents, operation names, variables, aliases,
  fragments, directives, and the standard `data`/`errors` envelope.
- Optional PostgreSQL tenant-head CAS for 2–8 concurrent writers, immutable
  manifests/write contexts, idempotency ownership, task fencing, legacy
  manifest outbox, and asynchronous derived-index catch-up.
- Coordinator migration/bootstrap/status/sync/rollback commands and coordinator
  availability, head revision, conflict, mirror lag, and backlog metrics.
- Separate data/admin listeners with pprof disabled by default.
- Complete OpenAPI coverage, versioned Go/Python SDK user agents, binary build
  metadata, reproducible capacity reports, and strict release gates.

### Compatibility

- Core layout version 2 entity, relation, commit, snapshot, and manifest formats
  remain readable by GGraphDB 1.0.
- `GGraphDB` is the public product name. The `graphdb` binary, module paths,
  `GRAPHDB_*` configuration, `X-GraphDB-*` response headers, and object keys
  remain unchanged for 1.0 compatibility.
- The former `GQL` name is retired. `/v1/query/gql`, `graphdb gql`, and SDK
  `GQL` methods remain deprecated aliases for the 1.0 `FIND`/`MATCH` text DSL;
  they are not GraphQL.
- Labels are persisted in `fields.__graphdb_labels`; relation schemas are an
  optional sidecar.
- The release gate builds `release_20260722_01` and validates both 1.0
  writer/1.1 reader and 1.1 writer/1.0 reader directions.
- PostgreSQL mode permits 1.0 readers through the legacy manifest mirror but
  rejects every 1.0/local writer.

### Security

- `/debug/pprof/*` is no longer exposed on the compatibility/data listener.
- Enabling pprof requires a distinct management listener.
- Production gateway guidance defines TLS termination, authenticated tenant
  header replacement, admin RBAC, and private upstream networking.

## [1.0.0] - 2026-07-22

- Initial current-state property graph release with Parquet object storage,
  tenant manifests, single-writer coordination, reader fleets, JSON query DSL,
  `FIND`/`MATCH` text DSL, ingestion governance, indexes, maintenance, and
  operations APIs.
