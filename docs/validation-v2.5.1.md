# GGraphDB v2.5.1 validation

The [v2.5.1 tag workflow](https://github.com/SamuelSupe/graphdb/actions/workflows/release.yml?query=branch%3Av2.5.1) records exact-tag qualification. The publication job requires static, SDK, standalone recovery, S3 backup, gateway and Raft gates, including 30-minute standalone and Raft soaks. The full archive ships the runtime evidence under `release/evidence/`. Both Linux archives carry build metadata and SHA-256 sidecars. See [release deployment](user/release-deployment.md) for verification steps.

## Standalone WAL development tests

The [index catch-up change](https://github.com/SamuelSupe/graphdb/commit/38da37c) was tested on OrbStack with 63,247 entities, 105,491 edges, four writers and eight readers. Both runs used a 512 MiB maintenance budget, a 100 ms WAL flush interval and 60 seconds of input.

| Metric | Before segmentation | With segmentation |
| --- | ---: | ---: |
| Durable acceptance P99 | 263 ms | 190 ms |
| Acceptance to terminal commit P99 | 11.30 s | 569 ms |
| Completed batches | 208 | 240 |
| Deferred full rebuilds | 4 | 0 |

The candidate split five deltas and caught up to graph version 241 about 95 seconds after writes stopped. A separate process test killed the server while segments were queued. After restart, all three committed batches remained visible, retry returned the original version, and index maintenance caught up. These tests characterize the measured standalone workload. They do not establish a production latency bound, a capacity envelope, or qualification for protocol 5 rolling upgrades.
