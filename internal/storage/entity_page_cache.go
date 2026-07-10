package storage

import (
	"container/list"
	"strconv"
	"sync"
	"time"
)

type entityPageCache struct {
	mu              sync.Mutex
	max             int
	maxBytes        int64
	bytes           int64
	revalidateAfter time.Duration
	data            map[string]cachedEntityPage
	order           *list.List
	nodes           map[string]*list.Element
}

type cachedEntityPage struct {
	page        EntityPageData
	etag        string
	validatedAt time.Time
	size        int64
}

func newEntityPageCache(max int) *entityPageCache {
	return newEntityPageCacheWithRevalidation(max, 0)
}

func newConfiguredEntityPageCache(max int) *entityPageCache {
	return newEntityPageCacheWithRevalidation(max, 30*time.Second)
}

func newEntityPageCacheWithRevalidation(max int, revalidateAfter time.Duration) *entityPageCache {
	return &entityPageCache{
		max:             max,
		maxBytes:        entityPageCacheByteLimit(max),
		revalidateAfter: revalidateAfter,
		data:            map[string]cachedEntityPage{},
		order:           list.New(),
		nodes:           map[string]*list.Element{},
	}
}

func entityPageCacheKey(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) string {
	return tenantID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + objectKey + "\x00" + contentHash + "\x00" + schemaHash
}

func (s *TenantStore) cachedEntityPage(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) (EntityPageData, string, bool) {
	page, etag, _, ok := s.cachedEntityPageForRead(tenantID, version, objectKey, contentHash, schemaHash)
	return page, etag, ok
}

func (s *TenantStore) cachedEntityPageForRead(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) (EntityPageData, string, bool, bool) {
	entry, revalidate, ok := s.borrowCachedEntityPage(tenantID, version, objectKey, contentHash, schemaHash)
	if !ok {
		return EntityPageData{}, "", false, false
	}
	return copyEntityPage(entry.page), entry.etag, revalidate, true
}

func (s *TenantStore) borrowCachedEntityPage(tenantID string, version int64, objectKey string, contentHash string, schemaHash string) (cachedEntityPage, bool, bool) {
	if s.entityPageCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return cachedEntityPage{}, false, false
	}
	entry, ok := s.entityPageCache.get(entityPageCacheKey(tenantID, version, objectKey, contentHash, schemaHash))
	if !ok {
		return cachedEntityPage{}, false, false
	}
	return entry, s.entityPageCache.needsRevalidation(entry), true
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
	s.entityPageCache.put(key, cachedEntityPage{page: copyEntityPage(page), etag: etag, validatedAt: time.Now()})
}

func (c *entityPageCache) get(key string) (cachedEntityPage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	if !ok {
		return cachedEntityPage{}, false
	}
	c.order.MoveToBack(c.nodes[key])
	return entry, true
}

func (c *entityPageCache) put(key string, entry cachedEntityPage) {
	if c == nil || c.max <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.size = estimateEntityPageBytes(entry.page)
	if previous, ok := c.data[key]; ok {
		c.bytes -= previous.size
	}
	if node := c.nodes[key]; node != nil {
		c.order.MoveToBack(node)
	} else {
		c.nodes[key] = c.order.PushBack(key)
	}
	c.data[key] = entry
	c.bytes += entry.size
	for len(c.data) > c.max || c.bytes > c.maxBytes {
		front := c.order.Front()
		if front == nil {
			break
		}
		evicted := front.Value.(string)
		c.order.Remove(front)
		delete(c.nodes, evicted)
		c.bytes -= c.data[evicted].size
		delete(c.data, evicted)
	}
}

func (c *entityPageCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bytes -= c.data[key].size
	delete(c.data, key)
	if node := c.nodes[key]; node != nil {
		c.order.Remove(node)
		delete(c.nodes, key)
	}
}

func (c *entityPageCache) needsRevalidation(entry cachedEntityPage) bool {
	return c.revalidateAfter <= 0 || entry.validatedAt.IsZero() || time.Since(entry.validatedAt) >= c.revalidateAfter
}

func (c *entityPageCache) markValidated(key string, etag string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.data[key]
	if !ok {
		return
	}
	entry.validatedAt = time.Now()
	if etag != "" {
		entry.etag = etag
	}
	c.data[key] = entry
}

func entityPageCacheByteLimit(entries int) int64 {
	if entries <= 0 {
		return 0
	}
	const ceiling = int64(512 << 20)
	limit := int64(entries) << 20
	if limit > ceiling {
		return ceiling
	}
	return limit
}
