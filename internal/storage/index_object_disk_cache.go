package storage

import (
	"container/list"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type indexDiskCache struct {
	mu         sync.Mutex
	dir        string
	maxEntries int
	maxBytes   int64
	ttl        time.Duration
	bytes      int64
	entries    map[string]*indexDiskEntry
	lru        *list.List
}

type indexDiskEntry struct {
	id       string
	size     int64
	modified time.Time
	elem     *list.Element
}

type indexObjectDiskMeta struct {
	Key  string `json:"key"`
	ETag string `json:"etag"`
}

func newIndexDiskCache(dir string, maxEntries int, maxBytes int64, ttl time.Duration) *indexDiskCache {
	cache := &indexDiskCache{
		dir:        dir,
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		ttl:        ttl,
		entries:    map[string]*indexDiskEntry{},
		lru:        list.New(),
	}
	cache.loadExisting()
	return cache
}

func (c *indexDiskCache) loadExisting() {
	items, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	type candidate struct {
		id       string
		size     int64
		modified time.Time
	}
	now := time.Now()
	candidates := make([]candidate, 0, len(items)/2)
	for _, item := range items {
		if item.IsDir() || !strings.HasSuffix(item.Name(), ".parquet") {
			continue
		}
		dataInfo, dataErr := item.Info()
		metaInfo, metaErr := os.Stat(filepath.Join(c.dir, item.Name()+".meta"))
		if dataErr != nil || metaErr != nil {
			c.removeFiles(item.Name())
			continue
		}
		modified := dataInfo.ModTime()
		if metaInfo.ModTime().After(modified) {
			modified = metaInfo.ModTime()
		}
		if c.ttl > 0 && now.Sub(modified) > c.ttl {
			c.removeFiles(item.Name())
			continue
		}
		candidates = append(candidates, candidate{id: item.Name(), size: dataInfo.Size() + metaInfo.Size(), modified: modified})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].modified.Before(candidates[j].modified) })
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, item := range candidates {
		entry := &indexDiskEntry{id: item.id, size: item.size, modified: item.modified}
		entry.elem = c.lru.PushFront(item.id)
		c.entries[item.id] = entry
		c.bytes += item.size
	}
	c.evictLocked()
}

func (c *indexDiskCache) get(cacheKey string, objectKey string) (cachedIndexObject, bool, error) {
	id := indexDiskCacheID(cacheKey)
	c.mu.Lock()
	if tracked := c.entries[id]; tracked != nil && c.ttl > 0 && time.Since(tracked.modified) >= c.ttl {
		c.removeLocked(id)
		c.mu.Unlock()
		return cachedIndexObject{}, false, nil
	}
	c.mu.Unlock()
	entry, ok, err := readIndexObjectCacheFile(c.dir, id, objectKey)
	if err != nil || !ok {
		c.dropID(id)
		return cachedIndexObject{}, ok, err
	}
	dataInfo, dataErr := os.Stat(filepath.Join(c.dir, id))
	metaInfo, metaErr := os.Stat(filepath.Join(c.dir, id+".meta"))
	c.mu.Lock()
	if tracked := c.entries[id]; tracked != nil {
		c.lru.MoveToFront(tracked.elem)
	} else if dataErr == nil && metaErr == nil {
		tracked := &indexDiskEntry{id: id, size: dataInfo.Size() + metaInfo.Size(), modified: time.Now()}
		tracked.elem = c.lru.PushFront(id)
		c.entries[id] = tracked
		c.bytes += tracked.size
		c.evictLocked()
	}
	c.mu.Unlock()
	return entry, true, nil
}

func (c *indexDiskCache) put(cacheKey string, entry cachedIndexObject) error {
	id := indexDiskCacheID(cacheKey)
	if err := writeIndexObjectCacheFile(c.dir, id, entry); err != nil {
		return err
	}
	dataInfo, err := os.Stat(filepath.Join(c.dir, id))
	if err != nil {
		return err
	}
	metaInfo, err := os.Stat(filepath.Join(c.dir, id+".meta"))
	if err != nil {
		return err
	}
	size := dataInfo.Size() + metaInfo.Size()
	c.mu.Lock()
	if previous := c.entries[id]; previous != nil {
		c.bytes -= previous.size
		previous.size = size
		previous.modified = time.Now()
		c.bytes += size
		c.lru.MoveToFront(previous.elem)
	} else {
		tracked := &indexDiskEntry{id: id, size: size, modified: time.Now()}
		tracked.elem = c.lru.PushFront(id)
		c.entries[id] = tracked
		c.bytes += size
	}
	c.evictLocked()
	c.mu.Unlock()
	return nil
}

func (c *indexDiskCache) has(cacheKey string) bool {
	id := indexDiskCacheID(cacheKey)
	c.mu.Lock()
	entry := c.entries[id]
	if entry != nil {
		if c.ttl > 0 && time.Since(entry.modified) >= c.ttl {
			c.removeLocked(id)
			c.mu.Unlock()
			return false
		}
		c.lru.MoveToFront(entry.elem)
	}
	c.mu.Unlock()
	return entry != nil
}

func (c *indexDiskCache) drop(cacheKey string) {
	c.dropID(indexDiskCacheID(cacheKey))
}

func (c *indexDiskCache) dropID(id string) {
	c.mu.Lock()
	c.removeLocked(id)
	c.mu.Unlock()
}

func (c *indexDiskCache) evictLocked() {
	for len(c.entries) > c.maxEntries || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		elem := c.lru.Back()
		if elem == nil {
			return
		}
		c.removeLocked(elem.Value.(string))
	}
}

func (c *indexDiskCache) removeLocked(id string) {
	entry := c.entries[id]
	if entry != nil {
		c.bytes -= entry.size
		c.lru.Remove(entry.elem)
		delete(c.entries, id)
	}
	c.removeFiles(id)
}

func (c *indexDiskCache) removeFiles(id string) {
	_ = os.Remove(filepath.Join(c.dir, id))
	_ = os.Remove(filepath.Join(c.dir, id+".meta"))
}

func indexDiskCacheID(cacheKey string) string {
	return objectContentHash([]byte(cacheKey)) + ".parquet"
}

func readIndexObjectCacheFile(dir string, id string, objectKey string) (cachedIndexObject, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, id))
	if errors.Is(err, os.ErrNotExist) {
		return cachedIndexObject{}, false, nil
	}
	if err != nil {
		return cachedIndexObject{}, false, err
	}
	metaData, err := os.ReadFile(filepath.Join(dir, id+".meta"))
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
	// Disk bytes are never trusted across process lifetimes; the caller must
	// decode and verify their catalog content hash before enabling fast hits.
	return cachedIndexObject{data: data, meta: meta}, true, nil
}

func writeIndexObjectCacheFile(dir string, id string, entry cachedIndexObject) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := writeIndexCacheFileAtomic(filepath.Join(dir, id), entry.data); err != nil {
		return err
	}
	metaData, err := json.Marshal(indexObjectDiskMeta{Key: entry.meta.Key, ETag: entry.meta.ETag})
	if err != nil {
		return err
	}
	return writeIndexCacheFileAtomic(filepath.Join(dir, id+".meta"), metaData)
}

func writeIndexCacheFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
