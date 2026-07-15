# Packet 07: fingerprint compatibility

## Objective

Preserve the public `data_md5` equality contract across upgrades while keeping no-op detection mutation-sized and race-free.

## Verification

Legacy hash fixtures, idempotency replay compatibility, same-store concurrent first Load/Commit race coverage, and no-op tests.
