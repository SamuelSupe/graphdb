# Contributing

Keep changes small, tenant-safe, and compatible with the published object
layout. Do not commit generated graph state, credentials, capacity runs, or
customer data.

`main` ships GGraphDB 2.0: one process owns local disk; object storage holds
snapshot backups. Use a fresh data directory. There is no 1.x migration or
cross-major storage compatibility requirement. The `data_hash` contract and
Go module use the 2.0 format/version.

Before submitting a change:

```sh
gofmt -w <changed-go-files>
go test -mod=readonly ./...
go vet -mod=readonly ./...
go test -mod=readonly -race ./...
python3 -m unittest discover -s sdk/python/tests -p 'test_*.py'
```

Release candidates use OrbStack/Docker with a Linux local data volume:

```sh
scripts/release_gate.sh
GRAPHDB_GATE_SOAK=1 RELEASE_GATE_SKIP_STATIC=1 scripts/release_gate.sh
```

See [local disk validation](docs/local-disk.md) for the reproducible performance
comparison. Historical cloud-storage release evidence does not certify this edition.

Update OpenAPI, SDK types, error codes, both English and Chinese user
documentation, and `CHANGELOG.md` when the public contract changes. A new
persisted field or object must document its layout version, upgrade behavior,
rollback behavior, and reachability/GC rules.
