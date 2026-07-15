# Result 05: Integration and verification

Status: completed

- `go test -mod=readonly ./...` passed.
- `go vet -mod=readonly ./...` passed.
- `go test -race -mod=readonly ./internal/storage ./internal/httpapi ./internal/query` passed.
- Focused fencing, lifecycle, task CAS, fingerprint, and pagination tests passed, including repeated race-window runs.
- Same-machine detached-`main` benchmark comparison passed with lower latency, bytes, and allocations.
- OrbStack context was active and `TestS3StoreIntegration` passed against RustFS at `127.0.0.1:39000`, covering conditional writes, purge, and explicit recreate.
- `git diff --check` passed; unrelated `internal/.DS_Store` remains untracked and untouched.
