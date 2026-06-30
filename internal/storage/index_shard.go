package storage

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

const indexShardBuckets = 64

func hashedIndexShardID(value string) string {
	if value == "" {
		return "default"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(value)))
	return fmt.Sprintf("%02x", int(sum[0])%indexShardBuckets)
}

func legacyIndexShardID(value string) string {
	if value == "" {
		return "default"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(value)))
	return fmt.Sprintf("%02x", sum[0])
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
