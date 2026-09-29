# Contributing

Keep changes small, tenant-safe, and compatible with the published object
layout. Do not commit generated graph state, credentials, capacity runs, or
customer data.

`main` ships GGraphDB 2.x: one process owns local disk; object storage holds
snapshot backups. The release version is recorded in `VERSION`; HTTP `/v1`, the
Go module `/v2`, and the persisted format are separate version identifiers.
2.1 retains the 2.0 data format. A fresh directory is required for a new
installation or replacement of 1.x; stop the old process before reusing an
existing 2.0/2.1 directory. There is no 1.x migration. See
[version boundaries](docs/naming-and-compatibility.md).

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
comparison. Historical release evidence does not certify a new candidate.
The [release checklist](docs/release-checklist.md) distinguishes default gates
from the documented 2.1.2 endurance-test waiver; skipped checks are not passes.

Update OpenAPI, SDK types, error codes, both English and Chinese user
documentation, and `CHANGELOG.md` when the public contract changes. A new
persisted field or object must document its layout version, upgrade behavior,
rollback behavior, and reachability/GC rules.
