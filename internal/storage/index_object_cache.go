package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type IndexObjectCacheConfig struct {
	MaxEntries int
	DiskDir    string
}

type indexObjectCache struct {
	mu          sync.Mutex
	max         int
	diskDir     string
	data        map[string]cachedIndexObject
	order       []string
	prefetching map[string]struct{}
}

type cachedIndexObject struct {
	data []byte
	meta ObjectMeta
}

type indexObjectDiskMeta struct {
	Key  string `json:"key"`
	ETag string `json:"etag"`
}

func newIndexObjectCache(max int) *indexObjectCache {
	return &indexObjectCache{max: max, data: map[string]cachedIndexObject{}, prefetching: map[string]struct{}{}}
}

func (s *TenantStore) ConfigureIndexObjectCache(config IndexObjectCacheConfig) {
	if config.MaxEntries < 0 {
		config.MaxEntries = 0
	}
	cache := newIndexObjectCache(config.MaxEntries)
	cache.diskDir = config.DiskDir
	s.indexCache = cache
	s.entityPageCache = newEntityPageCache(config.MaxEntries)
}

func (s *TenantStore) cachedIndexObject(ctx context.Context, kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) ([]byte, ObjectMeta, bool, error) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" {
		return nil, ObjectMeta{}, false, nil
	}
	cacheKey := indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash)
	entry, ok, err := s.indexCache.get(ctx, cacheKey, objectKey)
	if err != nil || !ok {
		s.recordIndexCache(tenantID, kind, cacheStatus(ok, err))
		return nil, ObjectMeta{}, ok, err
	}
	if entry.meta.ETag != "" {
		meta, err := objectMeta(ctx, s.Objects, objectKey)
		if errors.Is(err, ErrNotFound) {
			s.indexCache.drop(cacheKey)
			s.recordIndexCache(tenantID, kind, "stale")
			return nil, ObjectMeta{}, false, nil
		}
		if err != nil {
			s.recordIndexCache(tenantID, kind, "error")
			return nil, ObjectMeta{}, false, err
		}
		if meta.ETag != entry.meta.ETag {
			s.indexCache.drop(cacheKey)
			s.recordIndexCache(tenantID, kind, "stale")
			return nil, ObjectMeta{}, false, nil
		}
		entry.meta = meta
	}
	s.recordIndexCache(tenantID, kind, "hit")
	return append([]byte(nil), entry.data...), entry.meta, true, nil
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
	if s.CacheObserver != nil {
		s.CacheObserver.RecordReaderCache(tenantID, kind+"_"+status)
	}
}

func (s *TenantStore) putCachedIndexObject(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string, data []byte, meta ObjectMeta) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" || len(data) == 0 {
		return
	}
	cacheKey := indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash)
	s.indexCache.put(cacheKey, cachedIndexObject{data: data, meta: meta})
}

func (s *TenantStore) dropCachedIndexObject(kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return
	}
	s.indexCache.drop(indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash))
}

func (s *TenantStore) prefetchIndexObject(ctx context.Context, kind string, tenantID string, version int64, objectKey string, contentHash string, schemaHash string) {
	if s.indexCache == nil || contentHash == "" || schemaHash == "" || objectKey == "" {
		return
	}
	cacheKey := indexObjectCacheKey(kind, tenantID, version, objectKey, contentHash, schemaHash)
	if s.indexCache.has(cacheKey) || !s.indexCache.beginPrefetch(cacheKey) {
		return
	}
	go func() {
		defer s.indexCache.finishPrefetch(cacheKey)
		pctx := context.WithoutCancel(ctx)
		data, meta, err := s.Objects.GetWithMeta(pctx, objectKey)
		if err != nil {
			return
		}
		s.indexCache.put(cacheKey, cachedIndexObject{data: data, meta: meta})
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
	return kind + "\x00" + tenantID + "\x00" + strconv.FormatInt(version, 10) + "\x00" + objectKey + "\x00" + contentHash + "\x00" + schemaHash
}

func (c *indexObjectCache) get(ctx context.Context, cacheKey string, objectKey string) (cachedIndexObject, bool, error) {
	c.mu.Lock()
	if entry, ok := c.data[cacheKey]; ok {
		c.touchLocked(cacheKey)
		c.mu.Unlock()
		entry.meta.Exists = true
		if entry.meta.Key == "" {
			entry.meta.Key = objectKey
		}
		entry.data = append([]byte(nil), entry.data...)
		return entry, true, nil
	}
	diskDir := c.diskDir
	c.mu.Unlock()

	if diskDir == "" {
		return cachedIndexObject{}, false, nil
	}
	entry, ok, err := readIndexObjectCacheFile(diskDir, cacheKey, objectKey)
	if err != nil || !ok {
		return cachedIndexObject{}, ok, err
	}
	c.put(cacheKey, entry)
	entry.data = append([]byte(nil), entry.data...)
	return entry, true, objectContextErr(ctx)
}

func (c *indexObjectCache) put(cacheKey string, entry cachedIndexObject) {
	if c == nil || len(entry.data) == 0 {
		return
	}
	entry.data = append([]byte(nil), entry.data...)
	c.mu.Lock()
	if c.max > 0 {
		if _, ok := c.data[cacheKey]; !ok {
			c.order = append(c.order, cacheKey)
		}
		c.data[cacheKey] = entry
		c.touchLocked(cacheKey)
		for len(c.data) > c.max && len(c.order) > 0 {
			evicted := c.order[0]
			c.order = c.order[1:]
			delete(c.data, evicted)
		}
	}
	diskDir := c.diskDir
	c.mu.Unlock()
	if diskDir != "" {
		_ = writeIndexObjectCacheFile(diskDir, cacheKey, entry)
	}
}

func (c *indexObjectCache) has(cacheKey string) bool {
	c.mu.Lock()
	if _, ok := c.data[cacheKey]; ok {
		c.touchLocked(cacheKey)
		c.mu.Unlock()
		return true
	}
	diskDir := c.diskDir
	c.mu.Unlock()
	if diskDir == "" {
		return false
	}
	_, err := os.Stat(indexObjectCachePath(diskDir, cacheKey))
	return err == nil
}

func (c *indexObjectCache) drop(cacheKey string) {
	c.mu.Lock()
	delete(c.data, cacheKey)
	for i, key := range c.order {
		if key == cacheKey {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	diskDir := c.diskDir
	c.mu.Unlock()
	if diskDir != "" {
		_ = os.Remove(indexObjectCachePath(diskDir, cacheKey))
		_ = os.Remove(indexObjectCacheMetaPath(diskDir, cacheKey))
	}
}

func (c *indexObjectCache) beginPrefetch(cacheKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prefetching == nil {
		c.prefetching = map[string]struct{}{}
	}
	if _, ok := c.prefetching[cacheKey]; ok {
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

func (c *indexObjectCache) touchLocked(cacheKey string) {
	for i, key := range c.order {
		if key == cacheKey {
			copy(c.order[i:], c.order[i+1:])
			c.order[len(c.order)-1] = cacheKey
			return
		}
	}
	if c.max > 0 {
		c.order = append(c.order, cacheKey)
	}
}

func indexObjectCachePath(diskDir string, cacheKey string) string {
	return filepath.Join(diskDir, objectContentHash([]byte(cacheKey))+".parquet")
}

func indexObjectCacheMetaPath(diskDir string, cacheKey string) string {
	return indexObjectCachePath(diskDir, cacheKey) + ".meta"
}

func readIndexObjectCacheFile(diskDir string, cacheKey string, objectKey string) (cachedIndexObject, bool, error) {
	data, err := os.ReadFile(indexObjectCachePath(diskDir, cacheKey))
	if errors.Is(err, os.ErrNotExist) {
		return cachedIndexObject{}, false, nil
	}
	if err != nil {
		return cachedIndexObject{}, false, err
	}
	metaData, err := os.ReadFile(indexObjectCacheMetaPath(diskDir, cacheKey))
	if errors.Is(err, os.ErrNotExist) {
		return cachedIndexObject{}, false, nil
	}
	if err != nil {
		return cachedIndexObject{}, false, err
	}
	var diskMeta indexObjectDiskMeta
	if err := json.Unmarshal(metaData, &diskMeta); err != nil {
		return cachedIndexObject{}, false, nil
	}
	meta := ObjectMeta{Key: objectKey, ETag: diskMeta.ETag, Exists: true}
	if diskMeta.Key != "" {
		meta.Key = diskMeta.Key
	}
	return cachedIndexObject{data: data, meta: meta}, true, nil
}

func writeIndexObjectCacheFile(diskDir string, cacheKey string, entry cachedIndexObject) error {
	if err := os.MkdirAll(diskDir, 0o755); err != nil {
		return err
	}
	path := indexObjectCachePath(diskDir, cacheKey)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, entry.data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	metaData, err := json.Marshal(indexObjectDiskMeta{Key: entry.meta.Key, ETag: entry.meta.ETag})
	if err != nil {
		return err
	}
	metaPath := indexObjectCacheMetaPath(diskDir, cacheKey)
	metaTmp := metaPath + ".tmp"
	if err := os.WriteFile(metaTmp, metaData, 0o644); err != nil {
		return err
	}
	return os.Rename(metaTmp, metaPath)
}
