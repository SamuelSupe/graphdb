# Contributing

Keep changes small, tenant-safe, and compatible with the published object
layout. Do not commit generated graph state, credentials, capacity runs, or
customer data.

GGraphDB 1.1 is feature-frozen from the commit that first contains
`release/freeze-1.1.yaml`. Until 1.1 GA, changes are limited to release
blockers, security, compatibility, tests/gates, documentation, and operations
corrections. New public features, query syntax, incompatible APIs, and object
layout changes move to the next release.

Before submitting a change:

```sh
gofmt -w <changed-go-files>
go test -mod=readonly ./...
go vet -mod=readonly ./...
go test -mod=readonly -race ./...
python3 -m unittest discover -s sdk/python/tests -p 'test_*.py'
```

Changes to persistence, manifests, entity encoding, or migration behavior must
also run:

```sh
scripts/compatibility_v1_0_v1_1.sh
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
