# Packet 01: Fencing and tenant lifecycle

Objective: prevent stale writers and purge/recreate overlap from publishing or deleting a newer tenant generation.

Context: lease TTL is shorter than write timeout; purge currently deletes the lease inside the tenant prefix; lifecycle metadata cache is not revalidated while purge tombstone reads are uncached.

Ownership: `lease*`, `tenant_lifecycle*`, `tenant_purge_tombstone*`, lifecycle cache and related tests.

Do: introduce a backward-compatible fencing generation/epoch, keep the purge synchronization object outside the deleted prefix, validate ownership before final publish, and use bounded lifecycle caching.

Do not: require conditional DELETE support or change tenant-facing API semantics.

Expected output: code plus cross-instance takeover, soft-delete visibility, purge/recreate and tombstone hot-path tests.

Verification: focused storage/httpapi tests and race coverage.
