# Packet P1-A: graph apply and write path

## Objective

Remove quadratic affected-ID aggregation and redundant whole-graph work from small commits while preserving atomic apply, content-hash, replay, and OCC semantics.

## Context and ownership

- Own graph apply/copy/hash/index internals plus storage commit/load/write-cache files.
- Other workers are editing query, maintenance, and index-object cache modules concurrently; do not revert their work.

## Do

- Replace repeated unique-slice rebuilding with insertion-ordered set semantics.
- Add a storage-only copy-on-write mutation path and in-place private replay.
- Avoid snapshot/index cloning during logical hash generation and reuse the previous cached hash.
- Reject mismatched `ExpectedVersion` before history loading.
- Bound the per-tenant writer graph cache.

## Do not

- Change public deep-copy isolation, graph ordering, logical hash encoding, manifest CAS, or cross-instance visibility.

## Expected output and verification

- Focused graph/storage regressions, compatibility tests, race tests, vet, and affected-ID/write-copy benchmarks.
