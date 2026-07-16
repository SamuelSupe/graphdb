package storage

import (
	"crypto/sha256"
	"strings"
)

const indexShardBuckets = 64

var indexShardHex = func() [256]string {
	const digits = "0123456789abcdef"
	var values [256]string
	for i := range values {
		values[i] = string([]byte{digits[i>>4], digits[i&0x0f]})
	}
	return values
}()

func hashedIndexShardID(value string) string {
	if value == "" {
		return "default"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(value)))
	return indexShardHex[int(sum[0])%indexShardBuckets]
}

func legacyIndexShardID(value string) string {
	if value == "" {
		return "default"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(value)))
	return indexShardHex[sum[0]]
}

func indexShardIDCandidates(value string) []string {
	primary := hashedIndexShardID(value)
	legacy := legacyIndexShardID(value)
	if legacy == primary {
		return []string{primary}
	}
	return []string{primary, legacy}
}

func indexShardIDMatches(value string, shardID string) bool {
	for _, candidate := range indexShardIDCandidates(value) {
		if candidate == shardID {
			return true
		}
	}
	return false
}
