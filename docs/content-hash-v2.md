# Logical content hash in 2.0

The API field is `data_hash`, with algorithm prefix `sha256-shards-v2:` and a
64-character lowercase SHA-256 digest. It is independent of graph version,
commit IDs and operational timestamps. Backups still have separate whole-object
SHA-256 checksums for byte-level integrity.

The root contains four categories, in order: CI types, entities, relation types,
and edges. Logical values use the existing normalized logical-item JSON (sorted
JSON object keys, normalized source metadata, no version or timestamps).

For each key, compute unsigned 32-bit FNV-1a over its UTF-8 bytes (offset
2166136261, multiplier 16777619), then select bucket
`uint8(hash ^ (hash >> 16))`. Each category has 256 buckets.

A leaf is SHA-256 of the category name (`ci_type`, `entity`, `relation_type` or
`edge`), a zero byte, the key byte length as unsigned 64-bit big endian, the key
bytes, and logical-value JSON bytes. Within a nonempty bucket, sort keys by Go
string byte order and SHA-256 the concatenated 32-byte leaf digests. An empty
bucket is represented by 32 zero bytes. The final root is SHA-256 of
`sha256-shards-v2` followed by a zero byte and all 1024 bucket digests in category
then bucket order.

A published graph owns immutable leaf/bucket state. Small updates replace only
touched buckets and their digests; the root hashes 32 KiB, not the full graph.
The byte estimate used for memory admission sums logical-item JSON sizes and is
not a serialized snapshot length. Cold construction still visits the whole graph.

This algorithm replaces the 1.x complete-JSON MD5 contract. No old-MD5 endpoint
or digest migration is provided.
