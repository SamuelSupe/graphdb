package storage

import (
	"context"
	"sync"
	"time"
)

const readerCacheRefreshConcurrency = 8

func (c *ReaderCache) RefreshCached(ctx context.Context) {
	tenantIDs := c.cachedTenantsForRefresh()
	if len(tenantIDs) == 0 {
		return
	}
	workers := min(readerCacheRefreshConcurrency, len(tenantIDs))
	jobs := make(chan string)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for tenantID := range jobs {
				if ctx.Err() != nil {
					return
				}
				_, _, _ = c.refresh(ctx, tenantID, false)
			}
		}()
	}

send:
	for _, tenantID := range tenantIDs {
		select {
		case jobs <- tenantID:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	group.Wait()
}

func (c *ReaderCache) cachedTenantsForRefresh() []string {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	tenantIDs := make([]string, 0, len(c.entries))
	for tenantID, entry := range c.entries {
		if cacheEntryIdle(entry, now, c.IdleTTL) {
			c.deleteEntryLocked(tenantID)
			continue
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	return tenantIDs
}
