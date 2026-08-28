package storage

import (
	"container/list"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultIndexCacheMaxBytes     = 512 << 20
	defaultIndexDiskCacheMaxBytes = 4 << 30
	defaultIndexDiskCacheTTL      = 7 * 24 * time.Hour
	defaultIndexRevalidateTTL     = 30 * time.Second
	maxIndexObjectPrefetches      = 16
)

type IndexObjectCacheConfig struct {
	MaxEntries    int
	MaxBytes      int64
	DiskDir       string
	DiskMaxBytes  int64
	DiskTTL       time.Duration
	RevalidateTTL time.Duration
}

type indexObjectCache struct {
	mu            sync.Mutex
	max           int
	maxBytes      int64
	bytes         int64
	data          map[string]*indexObjectMemoryEntry
	lru           *list.List
	prefetching   map[string]struct{}
	disk          *indexDiskCache
	revalidateTTL time.Duration
}

type indexObjectMemoryEntry struct {
	value           cachedIndexObject
	size            int64
	elem            *list.Element
	verifiedContent map[string]struct{}
}

type cachedIndexObject struct {
	data        []byte
	meta        ObjectMeta
	validatedAt time.Time
	verified    bool
}

func newIndexObjectCache(max int) *indexObjectCache {
	if max < 0 {
		max = 0
	}
	return &indexObjectCache{
		max:         max,
		maxBytes:    indexCacheByteLimit(max, defaultIndexCacheMaxBytes),
		data:        map[string]*indexObjectMemoryEntry{},
		lru:         list.New(),
		prefetching: map[string]struct{}{},
	}
}

func indexCacheByteLimit(entries int, ceiling int64) int64 {
	if entries <= 0 {
		return 0
	}
	limit := int64(entries) << 20
	if limit > ceiling {
		return ceiling
	}
	return limit
}

func (s *TenantStore) ConfigureIndexObjectCache(config IndexObjectCacheConfig) {
	cache := newIndexObjectCache(config.MaxEntries)
	if config.RevalidateTTL < 0 {
		cache.revalidateTTL = 0
	} else if config.RevalidateTTL == 0 {
		cache.revalidateTTL = defaultIndexRevalidateTTL
	} else {
		cache.revalidateTTL = config.RevalidateTTL
	}
	if config.MaxBytes > 0 {
		cache.maxBytes = config.MaxBytes
	}
	if config.DiskDir != "" && cache.max > 0 {
		diskMaxBytes := config.DiskMaxBytes
		if diskMaxBytes <= 0 {
			diskMaxBytes = indexCacheByteLimit(cache.max*4, defaultIndexDiskCacheMaxBytes)
		}
		ttl := config.DiskTTL
		if ttl <= 0 {
			ttl = defaultIndexDiskCacheTTL
		}
		cache.disk = newIndexDiskCache(config.DiskDir, cache.max, diskMaxBytes, ttl)
	}
	s.indexCache = cache
	s.entityPageCache = newConfiguredEntityPageCache(config.MaxEntries)
	s.edgeLookupCache = configuredEdgeLookupCache(
		config.MaxEntries, config.MaxBytes,
	)
}

func (s *TenantStore) cachedIndexObject(ctx context.Context, kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) ([]byte, ObjectMeta, bool, error) {
	data, meta, _, ok, err := s.cachedIndexObjectWithVerification(ctx, kind, tenantID, version, objectKey, contentHash, schemaHash)
	return data, meta, ok, err
}

func (s *TenantStore) cachedIndexObjectWithVerification(ctx context.Context, kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) ([]byte, ObjectMeta, bool, bool, error) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" {
		return nil, ObjectMeta{}, false, false, nil
	}
	cacheKey := indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash)
	entry, ok, err := s.indexCache.getForContent(ctx, cacheKey, objectKey, contentHash)
	s.recordIndexCache(tenantID, kind, cacheStatus(ok, err))
	if err != nil || !ok {
		return nil, ObjectMeta{}, false, ok, err
	}
	if s.indexCache.needsRevalidation(entry.validatedAt) {
		meta, metaErr := objectMeta(ctx, s.Objects, objectKey)
		if errors.Is(metaErr, ErrNotFound) || (metaErr == nil && entry.meta.ETag != "" && meta.ETag != entry.meta.ETag) {
			s.indexCache.drop(cacheKey)
			s.recordIndexCache(tenantID, kind, "stale")
			return nil, ObjectMeta{}, false, false, nil
		}
		if metaErr != nil {
			s.recordIndexCache(tenantID, kind, "error")
			return nil, ObjectMeta{}, false, false, metaErr
		}
		cachedETag := entry.meta.ETag
		entry.meta = meta
		entry.validatedAt = time.Now()
		if cachedETag == "" || meta.ETag == "" {
			entry.verified = false
			s.indexCache.markContentUnverified(cacheKey)
		}
		s.indexCache.markValidated(cacheKey, meta, entry.validatedAt)
	}
	return entry.data, entry.meta, entry.verified, true, nil
}

func cacheStatus(ok bool, err error) string {
	if err != nil {
		return "error"
	}
	if ok {
		return "hit"
	}
	return "miss"
}

func (s *TenantStore) recordIndexCache(tenantID string, kind string, status string) {
	if s.cacheObserver != nil {
		s.cacheObserver.RecordReaderCache(tenantID, kind+"_"+status)
	}
}

func (s *TenantStore) putCachedIndexObject(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string, data []byte, meta ObjectMeta) {
	s.putCachedIndexObjectState(kind, tenantID, version, objectKey, contentHash, schemaHash, data, meta, false)
}

func (s *TenantStore) putVerifiedCachedIndexObject(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string, data []byte, meta ObjectMeta) {
	s.putCachedIndexObjectState(kind, tenantID, version, objectKey, contentHash, schemaHash, data, meta, true)
}

func (s *TenantStore) putCachedIndexObjectState(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string, data []byte, meta ObjectMeta, verified bool) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" || len(data) == 0 {
		return
	}
	cacheKey := indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash)
	s.indexCache.putForContent(cacheKey, contentHash, cachedIndexObject{data: data, meta: meta, validatedAt: time.Now(), verified: verified})
}

func (s *TenantStore) markCachedIndexObjectVerified(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) {
	if s.indexCache == nil {
		return
	}
	s.indexCache.markContentVerified(
		indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash),
		contentHash,
	)
}

func (s *TenantStore) dropCachedIndexObject(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return
	}
	s.indexCache.drop(indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash))
}

func (s *TenantStore) prefetchIndexObject(ctx context.Context, kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) {
	if s.indexCache == nil || s.indexCache.max == 0 ||
		contentHash == "" || schemaHash == "" || objectKey == "" {
		return
	}
	cacheKey := indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash)
	if !s.indexCache.beginPrefetch(cacheKey) {
		return
	}
	if s.indexCache.has(cacheKey) {
		s.indexCache.finishPrefetch(cacheKey)
		return
	}
	go func() {
		defer s.indexCache.finishPrefetch(cacheKey)
		data, meta, err := s.Objects.GetWithMeta(context.WithoutCancel(ctx), objectKey)
		if err == nil {
			s.indexCache.put(cacheKey, cachedIndexObject{data: data, meta: meta, validatedAt: time.Now()})
		}
	}()
}

func (s *TenantStore) prefetchParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) {
	key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
	s.prefetchIndexObject(ctx, "entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
}

func (s *TenantStore) prefetchParquetEdgeShardObject(ctx context.Context, tenantID string, version int64, spec EdgeShard) {
	key := firstIndexObjectKey(spec.Objects, "shard", s.parquetEdgeShardVersionKey(tenantID, version, spec.RelationType, spec.Shard))
	s.prefetchIndexObject(ctx, "edge_shard", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
}

func indexObjectCacheKey(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) string {
	if strings.Contains(objectKey, "/packs/") {
		contentHash = ""
	}
	return kind + "\x00" + tenantID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + objectKey + "\x00" + contentHash + "\x00" + schemaHash
}

func (c *indexObjectCache) get(ctx context.Context, cacheKey string, objectKey string) (cachedIndexObject, bool, error) {
	return c.getForContent(ctx, cacheKey, objectKey, "")
}

func (c *indexObjectCache) getForContent(ctx context.Context, cacheKey string, objectKey string, contentHash string) (cachedIndexObject, bool, error) {
	if err := objectContextErr(ctx); err != nil {
		return cachedIndexObject{}, false, err
	}
	c.mu.Lock()
	if entry := c.data[cacheKey]; entry != nil {
		c.lru.MoveToFront(entry.elem)
		value := entry.value
		_, value.verified = entry.verifiedContent[contentHash]
		c.mu.Unlock()
		// Cache payloads are immutable after insertion. All callers are internal
		// decoders, so lending the slice avoids a full Parquet copy on every hit.
		value.meta = normalizeObjectMeta(objectKey, value.meta)
		return value, true, nil
	}
	disk := c.disk
	c.mu.Unlock()
	if disk == nil {
		return cachedIndexObject{}, false, nil
	}
	entry, ok, err := disk.get(cacheKey, objectKey)
	if err != nil || !ok {
		return cachedIndexObject{}, ok, err
	}
	c.putMemoryForContent(cacheKey, contentHash, entry)
	return entry, true, objectContextErr(ctx)
}

func (c *indexObjectCache) put(cacheKey string, entry cachedIndexObject) {
	c.putForContent(cacheKey, "", entry)
}

func (c *indexObjectCache) putForContent(cacheKey string, contentHash string, entry cachedIndexObject) {
	if c == nil || len(entry.data) == 0 {
		return
	}
	changed := c.putMemoryForContent(cacheKey, contentHash, entry)
	c.mu.Lock()
	disk := c.disk
	c.mu.Unlock()
	if changed && disk != nil {
		_ = disk.put(cacheKey, entry)
	}
}

func (c *indexObjectCache) putMemory(cacheKey string, entry cachedIndexObject) {
	c.putMemoryForContent(cacheKey, "", entry)
}

func (c *indexObjectCache) putMemoryForContent(cacheKey string, contentHash string, entry cachedIndexObject) bool {
	if c.max == 0 || len(entry.data) == 0 {
		return false
	}
	c.mu.Lock()
	if existing := c.data[cacheKey]; reuseCachedIndexObject(existing, contentHash, entry) {
		c.lru.MoveToFront(existing.elem)
		c.mu.Unlock()
		return false
	}
	c.mu.Unlock()

	verified := entry.verified
	entry.verified = false
	entry.data = append([]byte(nil), entry.data...)
	size := int64(len(entry.data) + len(cacheKey) + len(entry.meta.Key) + len(entry.meta.ETag) + 96)
	if c.maxBytes > 0 && size > c.maxBytes {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.data[cacheKey]; reuseCachedIndexObject(existing, contentHash, entry) {
		if verified {
			markCachedIndexContentVerified(existing, contentHash)
		}
		c.lru.MoveToFront(existing.elem)
		return false
	}
	verifiedContent := map[string]struct{}{}
	if verified {
		verifiedContent[contentHash] = struct{}{}
	}
	if existing := c.data[cacheKey]; existing != nil {
		c.bytes -= existing.size
		existing.value = entry
		existing.size = size
		existing.verifiedContent = verifiedContent
		c.bytes += size
		c.lru.MoveToFront(existing.elem)
	} else {
		memoryEntry := &indexObjectMemoryEntry{
			value: entry, size: size, verifiedContent: verifiedContent,
		}
		memoryEntry.elem = c.lru.PushFront(cacheKey)
		c.data[cacheKey] = memoryEntry
		c.bytes += size
	}
	c.evictMemoryLocked()
	return true
}

func (c *indexObjectCache) needsRevalidation(validatedAt time.Time) bool {
	return c.revalidateTTL <= 0 || validatedAt.IsZero() || time.Since(validatedAt) >= c.revalidateTTL
}

func (c *indexObjectCache) markValidated(cacheKey string, meta ObjectMeta, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.data[cacheKey]; entry != nil {
		entry.value.meta = meta
		entry.value.validatedAt = at
	}
}

func (c *indexObjectCache) markContentVerified(cacheKey string, contentHash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.data[cacheKey]; entry != nil {
		markCachedIndexContentVerified(entry, contentHash)
	}
}

func (c *indexObjectCache) markContentUnverified(cacheKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.data[cacheKey]; entry != nil {
		entry.verifiedContent = nil
	}
}

func (c *indexObjectCache) evictMemoryLocked() {
	for len(c.data) > c.max || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		elem := c.lru.Back()
		if elem == nil {
			return
		}
		c.removeMemoryLocked(elem.Value.(string))
	}
}

func (c *indexObjectCache) removeMemoryLocked(cacheKey string) {
	entry := c.data[cacheKey]
	if entry == nil {
		return
	}
	c.bytes -= entry.size
	c.lru.Remove(entry.elem)
	delete(c.data, cacheKey)
}

func (c *indexObjectCache) has(cacheKey string) bool {
	c.mu.Lock()
	entry := c.data[cacheKey]
	if entry != nil {
		c.lru.MoveToFront(entry.elem)
		c.mu.Unlock()
		return true
	}
	disk := c.disk
	c.mu.Unlock()
	return disk != nil && disk.has(cacheKey)
}

func (c *indexObjectCache) drop(cacheKey string) {
	c.mu.Lock()
	c.removeMemoryLocked(cacheKey)
	disk := c.disk
	c.mu.Unlock()
	if disk != nil {
		disk.drop(cacheKey)
	}
}

func (c *indexObjectCache) beginPrefetch(cacheKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.prefetching[cacheKey]; ok {
		return false
	}
	if len(c.prefetching) >= maxIndexObjectPrefetches {
		return false
	}
	c.prefetching[cacheKey] = struct{}{}
	return true
}

func (c *indexObjectCache) finishPrefetch(cacheKey string) {
	c.mu.Lock()
	delete(c.prefetching, cacheKey)
	c.mu.Unlock()
}
