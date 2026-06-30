package storage

import (
	"context"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type ReaderCache struct {
	Store    *TenantStore
	TTL      time.Duration
	Observer ReaderCacheObserver

	mu      sync.RWMutex
	entries map[string]cacheEntry
	gens    map[string]uint64
	loading map[string]*cacheLoad
}

type ReaderCacheObserver interface {
	RecordReaderCache(tenantID string, status string)
	RecordReaderVisibleVersion(tenantID string, version int64)
}

type cacheEntry struct {
	graph      *graph.Graph
	manifest   Manifest
	cachedAt   time.Time
	expiresAt  time.Time
	lastAccess time.Time
}

type cacheLoad struct {
	done chan struct{}
}

type ReaderCacheStatus struct {
	Cached     bool
	Loading    bool
	Version    int64
	Manifest   Manifest
	CachedAt   time.Time
	ExpiresAt  time.Time
	LastAccess time.Time
	TTL        time.Duration
}

func NewReaderCache(store *TenantStore, ttl time.Duration) *ReaderCache {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &ReaderCache{
		Store:   store,
		TTL:     ttl,
		entries: map[string]cacheEntry{},
		gens:    map[string]uint64{},
		loading: map[string]*cacheLoad{},
	}
}

func (c *ReaderCache) Start(ctx context.Context) {
	ticker := time.NewTicker(c.TTL)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.RefreshCached(ctx)
			}
		}
	}()
}

func (c *ReaderCache) Load(ctx context.Context, tenantID string) (*graph.Graph, Manifest, error) {
	return c.load(ctx, tenantID, 0)
}

func (c *ReaderCache) LoadAtLeast(ctx context.Context, tenantID string, minVersion int64) (*graph.Graph, Manifest, error) {
	return c.load(ctx, tenantID, minVersion)
}

func (c *ReaderCache) load(ctx context.Context, tenantID string, minVersion int64) (*graph.Graph, Manifest, error) {
	for {
		now := time.Now()
		c.mu.RLock()
		entry, ok := c.entries[tenantID]
		if ok && cacheEntryFresh(entry, now, minVersion) {
			c.mu.RUnlock()
			c.touch(tenantID)
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cloneCacheEntry(entry)
		}
		startGen := c.gens[tenantID]
		c.mu.RUnlock()

		manifest, _, err := c.Store.getManifest(ctx, tenantID)
		if err != nil {
			return nil, Manifest{}, err
		}
		c.mu.Lock()
		now = time.Now()
		entry, ok = c.entries[tenantID]
		if ok && cacheEntryFresh(entry, now, minVersion) {
			if entry.cachedAt.IsZero() {
				entry.cachedAt = now
			}
			entry.lastAccess = now
			c.entries[tenantID] = entry
			c.mu.Unlock()
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cloneCacheEntry(entry)
		}
		startGen = c.gens[tenantID]
		if ok && manifest.Version == entry.manifest.Version && entry.manifest.Version >= minVersion {
			if entry.cachedAt.IsZero() {
				entry.cachedAt = now
			}
			entry.expiresAt = now.Add(c.TTL)
			entry.lastAccess = now
			c.entries[tenantID] = entry
			c.mu.Unlock()
			c.recordCache(tenantID, "revalidated")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cloneCacheEntry(entry)
		}
		c.mu.Unlock()

		load, ok, err := c.beginLoad(ctx, tenantID)
		if err != nil {
			return nil, Manifest{}, err
		}
		if !ok {
			c.recordCache(tenantID, "wait")
			continue
		}
		c.recordCache(tenantID, "miss")
		g, loaded, err := c.Store.LoadAtLeast(ctx, tenantID, minVersion)
		if err != nil {
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "miss_error")
			return nil, Manifest{}, err
		}
		c.mu.Lock()
		entry, ok = c.entries[tenantID]
		if ok && entry.manifest.Version > loaded.Version && entry.manifest.Version >= minVersion {
			c.mu.Unlock()
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "hit_newer")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cloneCacheEntry(entry)
		}
		if c.gens[tenantID] != startGen {
			c.mu.Unlock()
			c.finishLoad(tenantID, load)
			if err := ctx.Err(); err != nil {
				return nil, Manifest{}, err
			}
			continue
		}
		now = time.Now()
		c.entries[tenantID] = cacheEntry{graph: g.Clone(), manifest: loaded, cachedAt: now, expiresAt: now.Add(c.TTL), lastAccess: now}
		c.mu.Unlock()
		c.finishLoad(tenantID, load)
		c.recordVisible(tenantID, loaded.Version)
		return g, loaded, nil
	}
}

func (c *ReaderCache) recordCache(tenantID string, status string) {
	if c.Observer != nil {
		c.Observer.RecordReaderCache(tenantID, status)
	}
}

func (c *ReaderCache) recordVisible(tenantID string, version int64) {
	if c.Observer != nil {
		c.Observer.RecordReaderVisibleVersion(tenantID, version)
	}
}

func (c *ReaderCache) beginLoad(ctx context.Context, tenantID string) (*cacheLoad, bool, error) {
	c.mu.Lock()
	if current := c.loading[tenantID]; current != nil {
		c.mu.Unlock()
		if err := waitCacheLoad(ctx, current); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	load := &cacheLoad{done: make(chan struct{})}
	if c.loading == nil {
		c.loading = map[string]*cacheLoad{}
	}
	c.loading[tenantID] = load
	c.mu.Unlock()
	return load, true, nil
}

func waitCacheLoad(ctx context.Context, load *cacheLoad) error {
	select {
	case <-load.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ReaderCache) finishLoad(tenantID string, load *cacheLoad) {
	c.mu.Lock()
	if c.loading[tenantID] == load {
		delete(c.loading, tenantID)
	}
	close(load.done)
	c.mu.Unlock()
}

func cacheEntryFresh(entry cacheEntry, now time.Time, minVersion int64) bool {
	if !now.Before(entry.expiresAt) {
		return false
	}
	return minVersion <= 0 || entry.manifest.Version >= minVersion
}

func (c *ReaderCache) RefreshCached(ctx context.Context) {
	now := time.Now()
	c.mu.Lock()
	tenantIDs := make([]string, 0, len(c.entries))
	for tenantID, entry := range c.entries {
		if cacheEntryIdle(entry, now, c.TTL) {
			delete(c.entries, tenantID)
			continue
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	c.mu.Unlock()
	for _, tenantID := range tenantIDs {
		_, _, _ = c.refresh(ctx, tenantID, false)
	}
}

func (c *ReaderCache) Refresh(ctx context.Context, tenantID string) (*graph.Graph, Manifest, error) {
	return c.refresh(ctx, tenantID, true)
}

func (c *ReaderCache) refresh(ctx context.Context, tenantID string, markAccess bool) (*graph.Graph, Manifest, error) {
	c.mu.RLock()
	startGen := c.gens[tenantID]
	c.mu.RUnlock()
	manifest, _, err := c.Store.getManifest(ctx, tenantID)
	if err != nil {
		return nil, Manifest{}, err
	}
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	c.mu.RUnlock()
	if !ok && !markAccess {
		return nil, Manifest{}, nil
	}
	if ok && manifest.Version == entry.manifest.Version {
		now := time.Now()
		if entry.cachedAt.IsZero() {
			entry.cachedAt = now
		}
		entry.expiresAt = now.Add(c.TTL)
		if markAccess {
			entry.lastAccess = now
		}
		c.mu.Lock()
		if c.gens[tenantID] != startGen {
			c.mu.Unlock()
			return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
		}
		c.entries[tenantID] = entry
		c.mu.Unlock()
		return cloneCacheEntry(entry)
	}
	load, acquired, err := c.beginLoad(ctx, tenantID)
	if err != nil {
		return nil, Manifest{}, err
	}
	if !acquired {
		if !markAccess {
			return nil, Manifest{}, nil
		}
		return c.Load(ctx, tenantID)
	}
	g, loaded, err := c.Store.Load(ctx, tenantID)
	if err != nil {
		c.finishLoad(tenantID, load)
		return nil, Manifest{}, err
	}
	c.mu.Lock()
	if c.gens[tenantID] != startGen {
		c.mu.Unlock()
		c.finishLoad(tenantID, load)
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	now := time.Now()
	lastAccess := now
	if ok && !markAccess {
		lastAccess = entry.lastAccess
	}
	c.entries[tenantID] = cacheEntry{graph: g.Clone(), manifest: loaded, cachedAt: now, expiresAt: now.Add(c.TTL), lastAccess: lastAccess}
	c.mu.Unlock()
	c.finishLoad(tenantID, load)
	return g, loaded, nil
}

func (c *ReaderCache) touch(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[tenantID]
	if !ok {
		return
	}
	entry.lastAccess = time.Now()
	c.entries[tenantID] = entry
}

func (c *ReaderCache) reloadAfterGenerationChange(ctx context.Context, tenantID string, markAccess bool) (*graph.Graph, Manifest, error) {
	if !markAccess {
		return nil, Manifest{}, nil
	}
	return c.Load(ctx, tenantID)
}

func cloneCacheEntry(entry cacheEntry) (*graph.Graph, Manifest, error) {
	if entry.graph == nil {
		return nil, entry.manifest, nil
	}
	return entry.graph.Clone(), entry.manifest, nil
}

func (c *ReaderCache) Invalidate(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, tenantID)
	if c.gens == nil {
		c.gens = map[string]uint64{}
	}
	c.gens[tenantID]++
}

func (c *ReaderCache) CachedVersion(tenantID string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[tenantID]
	if !ok {
		return 0, false
	}
	return entry.manifest.Version, true
}

func (c *ReaderCache) CachedAtLeast(tenantID string, minVersion int64) (*graph.Graph, Manifest, bool, error) {
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	if !ok || entry.manifest.Version < minVersion {
		c.mu.RUnlock()
		return nil, Manifest{}, false, nil
	}
	c.mu.RUnlock()
	c.touch(tenantID)
	c.recordCache(tenantID, "hit_after_timeout")
	c.recordVisible(tenantID, entry.manifest.Version)
	g, manifest, err := cloneCacheEntry(entry)
	return g, manifest, true, err
}

func (c *ReaderCache) Status(tenantID string) ReaderCacheStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	status := ReaderCacheStatus{TTL: c.TTL}
	if load := c.loading[tenantID]; load != nil {
		status.Loading = true
	}
	entry, ok := c.entries[tenantID]
	if !ok {
		return status
	}
	status.Cached = true
	status.Version = entry.manifest.Version
	status.Manifest = entry.manifest
	status.CachedAt = entry.cachedAt
	status.ExpiresAt = entry.expiresAt
	status.LastAccess = entry.lastAccess
	return status
}

func cacheEntryIdle(entry cacheEntry, now time.Time, ttl time.Duration) bool {
	if entry.lastAccess.IsZero() {
		return false
	}
	return !now.Before(entry.lastAccess.Add(ttl))
}
