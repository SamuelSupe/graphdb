package storage

import "bytes"

func reuseCachedIndexObject(
	existing *indexObjectMemoryEntry,
	contentHash string,
	entry cachedIndexObject,
) bool {
	if existing == nil || !sameCachedIndexObject(existing.value, entry) {
		return false
	}
	if entry.validatedAt.After(existing.value.validatedAt) {
		existing.value.validatedAt = entry.validatedAt
		existing.value.meta = entry.meta
	}
	if entry.verified {
		markCachedIndexContentVerified(existing, contentHash)
	}
	return true
}

func sameCachedIndexObject(left cachedIndexObject, right cachedIndexObject) bool {
	if left.meta.ETag != "" && right.meta.ETag != "" {
		return left.meta.ETag == right.meta.ETag
	}
	return bytes.Equal(left.data, right.data)
}

func markCachedIndexContentVerified(
	entry *indexObjectMemoryEntry,
	contentHash string,
) {
	if entry.verifiedContent == nil {
		entry.verifiedContent = map[string]struct{}{}
	}
	entry.verifiedContent[contentHash] = struct{}{}
}
