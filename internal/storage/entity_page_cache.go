package storage

import (
	"strconv"
	"sync"

	"graphdb/internal/graph"
)

type entityPageCache struct {
	mu    sync.Mutex
	max   int
	data  map[string]cachedEntityPage
	order []string
}

type cachedEntityPage struct {
	page EntityPageData
	etag string
}

func newEntityPageCache(max int) *entityPageCache {
	return &entityPageCache{max: max, data: map[string]cachedEntityPage{}}
}

func entityPageCacheKey(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) string {
	return tenantID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + objectKey + "\x00" + contentHash + "\x00" + schemaHash
}

func (s *TenantStore) cachedEntityPage(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) (EntityPageData, string, bool) {
	if s.entityPageCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return EntityPageData{}, "", false
	}
	entry, ok := s.entityPageCache.get(entityPageCacheKey(tenantID, version, objectKey, contentHash, schemaHash))
	if !ok {
		return EntityPageData{}, "", false
	}
	return copyEntityPage(entry.page), entry.etag, true
}

func (s *TenantStore) dropCachedEntityPage(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) {
	if s.entityPageCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return
	}
	s.entityPageCache.drop(entityPageCacheKey(tenantID, version, objectKey, contentHash, schemaHash))
}

func (s *TenantStore) putCachedEntityPage(tenantID string, version int64, objectKey string, contentHash string, schemaHash string, page EntityPageData, etag string) {
	if s.entityPageCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return
	}
	key := entityPageCacheKey(tenantID, version, objectKey, contentHash, schemaHash)
	s.entityPageCache.put(key, cachedEntityPage{page: copyEntityPage(page), etag: etag})
}

func (c *entityPageCache) get(key string) (cachedEntityPage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	if !ok {
		return cachedEntityPage{}, false
	}
	c.touchLocked(key)
	entry.page = copyEntityPage(entry.page)
	return entry, true
}

func (c *entityPageCache) put(key string, entry cachedEntityPage) {
	if c == nil || c.max <= 0 {
		return
	}
	entry.page = copyEntityPage(entry.page)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.data[key]; !ok {
		c.order = append(c.order, key)
	}
	c.data[key] = entry
	c.touchLocked(key)
	for len(c.data) > c.max && len(c.order) > 0 {
		evicted := c.order[0]
		c.order = c.order[1:]
		delete(c.data, evicted)
	}
}

func (c *entityPageCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	for i, value := range c.order {
		if value == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func (c *entityPageCache) touchLocked(key string) {
	for i, value := range c.order {
		if value != key {
			continue
		}
		copy(c.order[i:], c.order[i+1:])
		c.order[len(c.order)-1] = key
		return
	}
}

func copyEntityPage(page EntityPageData) EntityPageData {
	page.Entities = copyGraphEntities(page.Entities)
	return page
}

func copyGraphEntities(entities []graph.Entity) []graph.Entity {
	if len(entities) == 0 {
		return nil
	}
	out := make([]graph.Entity, len(entities))
	for i, entity := range entities {
		out[i] = copyEntityShape(entity)
	}
	return out
}

func copyEntityShape(entity graph.Entity) graph.Entity {
	entity.Fields = copyFieldsShape(entity.Fields)
	entity.FieldSources = copyFieldSourcesShape(entity.FieldSources)
	if entity.ExistenceSource != nil {
		source := *entity.ExistenceSource
		entity.ExistenceSource = &source
	}
	entity.Identity = copyMapAnyShape(entity.Identity)
	entity.Sources = append([]graph.EntitySource(nil), entity.Sources...)
	entity.MergedFrom = append([]string(nil), entity.MergedFrom...)
	return entity
}

func copyFieldsShape(fields graph.Fields) graph.Fields {
	if fields == nil {
		return nil
	}
	out := make(graph.Fields, len(fields))
	for key, value := range fields {
		out[key] = copyAnyShape(value)
	}
	return out
}

func copyFieldSourcesShape(sources map[string]graph.FieldSource) map[string]graph.FieldSource {
	if sources == nil {
		return nil
	}
	out := make(map[string]graph.FieldSource, len(sources))
	for key, value := range sources {
		out[key] = value
	}
	return out
}

func copyMapAnyShape(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = copyAnyShape(value)
	}
	return out
}

func copyAnyShape(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case graph.Fields:
		return copyFieldsShape(typed)
	case map[string]any:
		return copyMapAnyShape(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = copyAnyShape(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case []int:
		return append([]int(nil), typed...)
	case []int64:
		return append([]int64(nil), typed...)
	case []float64:
		return append([]float64(nil), typed...)
	case []bool:
		return append([]bool(nil), typed...)
	default:
		return value
	}
}
