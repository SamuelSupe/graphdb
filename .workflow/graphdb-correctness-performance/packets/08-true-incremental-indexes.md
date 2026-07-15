# Packet 08: true incremental indexes

## Objective

Update only affected secondary indexes, edge shards, and entity pages after a supported mutation; do not rebuild full-graph artifacts to discover changes.

## Verification

Index correctness tests plus benchmark/allocation scaling at 10k and larger graphs.
