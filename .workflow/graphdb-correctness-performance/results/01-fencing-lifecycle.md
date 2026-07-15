# Result 01: Fencing and tenant lifecycle

Status: completed

- Writer lease and manifest now carry a backward-compatible token plus monotonic fence epoch. A delayed old acquirer cannot overwrite a newer published fence.
- Purge uses a CAS state machine (`running` -> `complete` -> `cleared`) outside the deleted tenant prefix. Recreate is blocked until completion.
- Purge retains its lease until the marker is complete. Unsupported conditional DELETE falls back to CAS-expiring the same lease, avoiding ABA deletion.
- Normal lease acquisition performs a fresh purge-state check only on cache miss/takeover; the hot lease path remains free of per-request tombstone reads.
- Lifecycle status uses a one-second bounded cache and refreshes both metadata and purge state across instances.
- Regression coverage includes stale final publish, delayed fence publication, purge/recreate overlap, stale lifecycle cache, unsupported conditional delete, and cross-instance cache expiry.

Verification: focused lifecycle tests repeatedly passed, storage package tests passed, and critical storage race tests passed.
