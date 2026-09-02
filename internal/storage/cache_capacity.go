package storage

import (
	"errors"
	"fmt"
)

var ErrReaderCacheEntryTooLarge = errors.New("reader cache entry exceeds capacity")

const (
	defaultReaderCacheMaxTenants = 64
	defaultReaderCacheMaxBytes   = int64(512 * 1024 * 1024)
)

func (c *ReaderCache) ConfigureCapacity(maxTenants int, maxBytes int64) {
	if maxTenants <= 0 {
		maxTenants = defaultReaderCacheMaxTenants
	}
	if maxBytes <= 0 {
		maxBytes = defaultReaderCacheMaxBytes
	}
	c.mu.Lock()
	c.MaxTenants = maxTenants
	c.MaxBytes = maxBytes
	c.bytes = 0
	for tenantID, entry := range c.entries {
		if entry.bytes <= 0 {
			entry.bytes = normalizedWriteCacheBytes(loadedGraph{
				Graph: entry.graph, Manifest: entry.manifest, Meta: entry.meta,
			})
			c.entries[tenantID] = entry
		}
		c.bytes = addWriteCacheBytes(c.bytes, entry.bytes)
	}
	c.evictCapacityLocked("")
	c.mu.Unlock()
}

func (c *ReaderCache) storeEntryLocked(tenantID string, entry cacheEntry) error {
	if entry.bytes <= 0 {
		entry.bytes = normalizedWriteCacheBytes(loadedGraph{
			Graph: entry.graph, Manifest: entry.manifest, Meta: entry.meta,
		})
	}
	if entry.bytes > c.MaxBytes {
		c.deleteEntryLocked(tenantID)
		return fmt.Errorf(
			"%w: tenant=%q bytes=%d limit=%d",
			ErrReaderCacheEntryTooLarge,
			tenantID,
			entry.bytes,
			c.MaxBytes,
		)
	}
	c.deleteEntryLocked(tenantID)
	c.entries[tenantID] = entry
	c.bytes = addWriteCacheBytes(c.bytes, entry.bytes)
	c.evictCapacityLocked(tenantID)
	return nil
}

func (c *ReaderCache) deleteEntryLocked(tenantID string) {
	entry, ok := c.entries[tenantID]
	if !ok {
		return
	}
	c.bytes -= entry.bytes
	if c.bytes < 0 {
		c.bytes = 0
	}
	delete(c.entries, tenantID)
}

func (c *ReaderCache) evictCapacityLocked(preferTenantID string) {
	for len(c.entries) > c.MaxTenants || c.bytes > c.MaxBytes {
		victim := ""
		for tenantID, entry := range c.entries {
			if tenantID == preferTenantID && len(c.entries) > 1 {
				continue
			}
			if victim == "" || entry.lastAccess.Before(c.entries[victim].lastAccess) {
				victim = tenantID
			}
		}
		if victim == "" {
			victim = preferTenantID
		}
		c.deleteEntryLocked(victim)
	}
}

func readerCacheEntryBytes(loaded loadedGraph) int64 {
	if bytes := writeCacheBytesWithoutCommitTail(loaded); bytes > 0 {
		return bytes
	}
	loaded.CacheBytes = 0
	loaded.CommitTail = emptyCommitTailCache()
	return normalizedWriteCacheBytes(loaded)
}
