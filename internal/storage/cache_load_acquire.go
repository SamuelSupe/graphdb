package storage

import "time"

func (c *ReaderCache) cacheEntryAfterLoadAcquire(
	tenantID string,
	minVersion int64,
) (cacheEntry, bool, uint64, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[tenantID]
	generation := c.gens[tenantID]
	if !ok || !cacheEntryFresh(entry, now, minVersion) {
		return entry, ok, generation, false
	}
	entry.lastAccess = now
	c.entries[tenantID] = entry
	return entry, true, generation, true
}
