package httpapi

import (
	"context"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

const (
	defaultTenantUsageCacheTTL = 60 * time.Second
	maxTenantUsageCacheEntries = 4096
)

type tenantUsageCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]tenantUsageCacheEntry
	loading map[string]*tenantUsageLoad
}

type tenantUsageCacheEntry struct {
	report   storage.TenantUsageReport
	cachedAt time.Time
}

type tenantUsageLoad struct {
	done     chan struct{}
	report   storage.TenantUsageReport
	err      error
	canceled bool
	cached   bool
	cachedAt time.Time
}

func newTenantUsageCache(ttl time.Duration) *tenantUsageCache {
	return &tenantUsageCache{
		ttl:     ttl,
		entries: map[string]tenantUsageCacheEntry{},
		loading: map[string]*tenantUsageLoad{},
	}
}

func (c *tenantUsageCache) freshLocked(
	tenantID string,
	now time.Time,
) (storage.TenantUsageReport, bool) {
	entry, ok := c.entries[tenantID]
	if !ok || now.Sub(entry.cachedAt) > c.ttl {
		return storage.TenantUsageReport{}, false
	}
	report := cloneTenantUsageReport(entry.report)
	report.Cached = true
	report.CacheAgeMS = nonNegativeDuration(now.Sub(entry.cachedAt)).Milliseconds()
	return report, true
}

func (c *tenantUsageCache) getOrLoad(
	ctx context.Context,
	tenantID string,
	now time.Time,
	loader func(context.Context, string) (storage.TenantUsageReport, error),
) (storage.TenantUsageReport, error) {
	if c == nil {
		return loader(ctx, tenantID)
	}
	for {
		c.mu.Lock()
		if c.ttl >= 0 {
			if report, ok := c.freshLocked(tenantID, now); ok {
				c.mu.Unlock()
				return report, nil
			}
		}
		if active := c.loading[tenantID]; active != nil {
			c.mu.Unlock()
			select {
			case <-active.done:
				if active.canceled && ctx.Err() == nil {
					now = time.Now().UTC()
					continue
				}
				report := cloneTenantUsageReport(active.report)
				if active.cached {
					report.Cached = true
					report.CacheAgeMS = nonNegativeDuration(
						time.Since(active.cachedAt),
					).Milliseconds()
				}
				return report, active.err
			case <-ctx.Done():
				return storage.TenantUsageReport{}, ctx.Err()
			}
		}
		active := &tenantUsageLoad{done: make(chan struct{})}
		c.loading[tenantID] = active
		c.mu.Unlock()

		report, err := loader(ctx, tenantID)
		active.report = cloneTenantUsageReport(report)
		active.err = err
		active.canceled = err != nil && ctx.Err() != nil
		active.cachedAt = now
		active.cached = err == nil && c.ttl >= 0

		c.mu.Lock()
		if active.cached {
			c.putLocked(tenantID, report, now)
		}
		delete(c.loading, tenantID)
		close(active.done)
		c.mu.Unlock()
		return report, err
	}
}

func (c *tenantUsageCache) stale(tenantID string, now time.Time, reason string) (storage.TenantUsageReport, bool) {
	if c == nil {
		return storage.TenantUsageReport{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[tenantID]
	if !ok {
		return storage.TenantUsageReport{}, false
	}
	report := cloneTenantUsageReport(entry.report)
	report.Cached = true
	report.Stale = true
	report.CacheAgeMS = nonNegativeDuration(now.Sub(entry.cachedAt)).Milliseconds()
	report.StaleReason = reason
	return report, true
}

func (c *tenantUsageCache) putLocked(
	tenantID string,
	report storage.TenantUsageReport,
	now time.Time,
) {
	if _, exists := c.entries[tenantID]; !exists &&
		len(c.entries) >= maxTenantUsageCacheEntries {
		for cachedTenantID := range c.entries {
			delete(c.entries, cachedTenantID)
			break
		}
	}
	report.Cached = false
	report.Stale = false
	report.CacheAgeMS = 0
	report.StaleReason = ""
	c.entries[tenantID] = tenantUsageCacheEntry{report: cloneTenantUsageReport(report), cachedAt: now}
}

func nonNegativeDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

func cloneTenantUsageReport(report storage.TenantUsageReport) storage.TenantUsageReport {
	report.Categories = append([]storage.TenantUsageCategory(nil), report.Categories...)
	return report
}
