package storage

const (
	commitTailCacheMaxBytes = int64(8 * 1024 * 1024)
	commitTailCacheOverhead = int64(256)
)

type commitTailCache struct {
	items    []commitSegmentItem
	bytes    int64
	complete bool
}

func emptyCommitTailCache() commitTailCache {
	return commitTailCache{complete: true}
}

func buildCommitTailCache(
	items []commitSegmentItem,
	keys []string,
) commitTailCache {
	if len(items) != len(keys) {
		return commitTailCache{}
	}
	cache := commitTailCache{
		items:    make([]commitSegmentItem, 0, len(items)),
		complete: true,
	}
	for i, item := range items {
		if item.Key != keys[i] || !cache.add(item) {
			return commitTailCache{}
		}
	}
	return cache
}

func appendCommitTailCache(
	cache commitTailCache,
	keys []string,
	item commitSegmentItem,
) commitTailCache {
	if !cache.matches(keys) {
		return commitTailCache{}
	}
	next := commitTailCache{
		items:    append([]commitSegmentItem(nil), cache.items...),
		bytes:    cache.bytes,
		complete: true,
	}
	if !next.add(item) {
		return commitTailCache{}
	}
	return next
}

func (cache *commitTailCache) add(item commitSegmentItem) bool {
	payload, err := commitPayloadJSON(item.Commit)
	if err != nil {
		return false
	}
	size := int64(len(payload))*4 +
		int64(len(item.Key)) +
		commitTailCacheOverhead
	if size < 0 ||
		size > commitTailCacheMaxBytes ||
		cache.bytes > commitTailCacheMaxBytes-size {
		return false
	}
	cache.items = append(cache.items, item)
	cache.bytes += size
	return true
}

func (cache commitTailCache) matches(keys []string) bool {
	if !cache.complete || len(cache.items) != len(keys) {
		return false
	}
	for i, key := range keys {
		if cache.items[i].Key != key {
			return false
		}
	}
	return true
}
