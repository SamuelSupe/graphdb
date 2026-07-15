package storage

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func estimateEntityPageBytes(page EntityPageData) int64 {
	size := int64(128 + len(page.TenantID) + len(page.Shard))
	for _, entity := range page.Entities {
		size += 256 + int64(len(entity.ID)+len(entity.Kind)+len(entity.Source)+len(entity.ExternalID)+len(entity.SplitFrom))
		size += estimateAnyMap(entity.Fields) + estimateAnyMap(entity.Identity)
		for key, source := range entity.FieldSources {
			size += 96 + int64(len(key)+len(source.Source))
		}
		for _, source := range entity.Sources {
			size += 96 + int64(len(source.Source)+len(source.ExternalID))
		}
		for _, id := range entity.MergedFrom {
			size += 16 + int64(len(id))
		}
	}
	// Map buckets, interface boxes, allocator spans, and the cache's defensive
	// deep copy are not represented by payload lengths. Heap sampling on 10k
	// entity pages measured roughly 4.6x the structural estimate, so charge a
	// conservative 5x weight to keep the configured byte ceiling meaningful.
	if size > (1<<63-1)/5 {
		return 1<<63 - 1
	}
	return size * 5
}

func entityPagePackBytes(page EntityPageData) int64 {
	page.TenantID = ""
	return estimateEntityPageBytes(page)
}

func estimateAnyMap(values map[string]any) int64 {
	if values == nil {
		return 0
	}
	size := int64(48)
	for key, value := range values {
		size += 24 + int64(len(key)) + estimateAnyBytes(value)
	}
	return size
}

func estimateAnyBytes(value any) int64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return 16 + int64(len(typed))
	case []string:
		size := int64(24)
		for _, item := range typed {
			size += 16 + int64(len(item))
		}
		return size
	case []any:
		size := int64(24)
		for _, item := range typed {
			size += estimateAnyBytes(item)
		}
		return size
	case graph.Fields:
		return estimateAnyMap(typed)
	case map[string]any:
		return estimateAnyMap(typed)
	default:
		return 16
	}
}
