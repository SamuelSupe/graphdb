package storage

import (
	"crypto/sha256"
	"encoding/binary"
)

const ingestMetadataBloomHashes = 7
const ingestMetadataBloomBits = 4096

type ingestMetadataBloom struct {
	Bits []byte `json:"bits"`
	K    uint8  `json:"k"`
}

func newIngestMetadataBloom(entries int) ingestMetadataBloom {
	return ingestMetadataBloom{
		Bits: make([]byte, ingestMetadataBloomBits/8),
		K:    ingestMetadataBloomHashes,
	}
}

func (b *ingestMetadataBloom) Add(value string) {
	if len(b.Bits) == 0 {
		*b = newIngestMetadataBloom(1)
	}
	first, second := ingestMetadataBloomHashesFor(value)
	bitCount := uint64(len(b.Bits) * 8)
	for i := uint8(0); i < b.K; i++ {
		bit := (first + uint64(i)*second) % bitCount
		b.Bits[bit/8] |= 1 << (bit % 8)
	}
}

func (b ingestMetadataBloom) MayContain(value string) bool {
	if len(b.Bits) == 0 || b.K == 0 {
		return true
	}
	first, second := ingestMetadataBloomHashesFor(value)
	bitCount := uint64(len(b.Bits) * 8)
	for i := uint8(0); i < b.K; i++ {
		bit := (first + uint64(i)*second) % bitCount
		if b.Bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func mergeIngestMetadataBlooms(filters ...ingestMetadataBloom) ingestMetadataBloom {
	maxBytes := 0
	for _, filter := range filters {
		maxBytes = max(maxBytes, len(filter.Bits))
	}
	if maxBytes == 0 {
		return ingestMetadataBloom{}
	}
	merged := ingestMetadataBloom{Bits: make([]byte, maxBytes), K: ingestMetadataBloomHashes}
	for _, filter := range filters {
		if len(filter.Bits) == 0 {
			continue
		}
		if len(filter.Bits) == maxBytes {
			for i := range filter.Bits {
				merged.Bits[i] |= filter.Bits[i]
			}
			continue
		}
		// Bloom filters with different widths cannot be ORed. Rebuilding is
		// handled by callers that still have the exact segment identities.
		return ingestMetadataBloom{}
	}
	return merged
}

func ingestMetadataBloomHashesFor(value string) (uint64, uint64) {
	sum := sha256.Sum256([]byte(value))
	first := binary.LittleEndian.Uint64(sum[:8])
	second := binary.LittleEndian.Uint64(sum[8:16]) | 1
	return first, second
}

func ingestMetadataBatchIdentity(source string, collectorID string, batchID string) string {
	return source + "\x00" + collectorID + "\x00batch\x00" + batchID
}

func ingestMetadataIdempotencyIdentity(source string, collectorID string, key string) string {
	return source + "\x00" + collectorID + "\x00idempotency\x00" + key
}

func ingestMetadataCollectorIdentity(source string, collectorID string) string {
	return source + "\x00" + collectorID
}
