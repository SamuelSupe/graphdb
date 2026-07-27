package storage

import "time"

func (c *ReaderCache) extendStaleEntry(
	tenantID string,
	minVersion int64,
	markAccess bool,
) (cacheEntry, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[tenantID]
	if !ok || entry.manifest.Version < minVersion {
		return cacheEntry{}, false
	}
	entry.expiresAt = now.Add(c.TTL)
	if markAccess {
		entry.lastAccess = now
	}
	c.entries[tenantID] = entry
	return entry, true
}
