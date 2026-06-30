package httpapi

import (
	"sync"
	"time"

	"graphdb/internal/storage"
)

const defaultTenantUsageCacheTTL = 60 * time.Second

type tenantUsageCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]tenantUsageCacheEntry
}

type tenantUsageCacheEntry struct {
	report   storage.TenantUsageReport
	cachedAt time.Time
}

func newTenantUsageCache(ttl time.Duration) *tenantUsageCache {
	return &tenantUsageCache{ttl: ttl, entries: map[string]tenantUsageCacheEntry{}}
}

func (c *tenantUsageCache) fresh(tenantID string, now time.Time) (storage.TenantUsageReport, bool) {
	if c == nil || c.ttl < 0 {
		return storage.TenantUsageReport{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[tenantID]
	if !ok || now.Sub(entry.cachedAt) > c.ttl {
		return storage.TenantUsageReport{}, false
	}
	report := cloneTenantUsageReport(entry.report)
	report.Cached = true
	report.CacheAgeMS = now.Sub(entry.cachedAt).Milliseconds()
	return report, true
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
	report.CacheAgeMS = now.Sub(entry.cachedAt).Milliseconds()
	report.StaleReason = reason
	return report, true
}

func (c *tenantUsageCache) put(tenantID string, report storage.TenantUsageReport, now time.Time) {
	if c == nil || c.ttl < 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	report.Cached = false
	report.Stale = false
	report.CacheAgeMS = 0
	report.StaleReason = ""
	c.entries[tenantID] = tenantUsageCacheEntry{report: cloneTenantUsageReport(report), cachedAt: now}
}

func cloneTenantUsageReport(report storage.TenantUsageReport) storage.TenantUsageReport {
	report.Categories = append([]storage.TenantUsageCategory(nil), report.Categories...)
	return report
}
