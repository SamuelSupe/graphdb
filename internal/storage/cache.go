package storage

import (
	"context"
	"errors"
	"fmt"
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
	meta       ObjectMeta
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
	return c.load(ctx, tenantID, 0, false)
}

func (c *ReaderCache) LoadAtLeast(ctx context.Context, tenantID string, minVersion int64) (*graph.Graph, Manifest, error) {
	return c.load(ctx, tenantID, minVersion, false)
}

// WithReadOnlyGraphAtLeast lends the immutable cache snapshot to fn without
// cloning it. The callback must not retain or mutate the graph. Public Load
// methods keep returning isolated copies for callers that need ownership.
func (c *ReaderCache) WithReadOnlyGraphAtLeast(ctx context.Context, tenantID string, minVersion int64, fn func(*graph.Graph, Manifest) error) error {
	if fn == nil {
		return nil
	}
	g, manifest, err := c.load(ctx, tenantID, minVersion, true)
	if err != nil {
		return err
	}
	return fn(g, manifest)
}

func (c *ReaderCache) load(ctx context.Context, tenantID string, minVersion int64, shared bool) (*graph.Graph, Manifest, error) {
	for {
		now := time.Now()
		c.mu.RLock()
		entry, ok := c.entries[tenantID]
		if ok && cacheEntryFresh(entry, now, minVersion) {
			c.mu.RUnlock()
			c.touch(tenantID)
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		startGen := c.gens[tenantID]
		c.mu.RUnlock()

		load, acquired, err := c.beginLoad(ctx, tenantID)
		if err != nil {
			return nil, Manifest{}, err
		}
		if !acquired {
			c.recordCache(tenantID, "wait")
			continue
		}
		entry, ok, startGen, fresh := c.cacheEntryAfterLoadAcquire(
			tenantID,
			minVersion,
		)
		if fresh {
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		manifest, manifestMeta, err := c.manifestForCacheEntry(
			ctx, tenantID, entry, ok,
		)
		if err != nil {
			if errors.Is(err, ErrCoordinatorUnavailable) {
				fallback, fallbackOK := c.extendStaleEntry(
					tenantID, minVersion, true,
				)
				c.finishLoad(tenantID, load)
				if fallbackOK {
					c.recordCache(tenantID, "stale_coordinator_unavailable")
					c.recordVisible(tenantID, fallback.manifest.Version)
					return cacheEntryGraph(fallback, shared)
				}
				return nil, Manifest{}, err
			}
			c.finishLoad(tenantID, load)
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
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		startGen = c.gens[tenantID]
		if ok &&
			cacheEntryMatchesManifest(entry, manifest, manifestMeta) &&
			entry.manifest.Version >= minVersion {
			if entry.cachedAt.IsZero() {
				entry.cachedAt = now
			}
			entry.expiresAt = now.Add(c.TTL)
			entry.lastAccess = now
			c.entries[tenantID] = entry
			c.mu.Unlock()
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "revalidated")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		c.mu.Unlock()

		c.recordCache(tenantID, "miss")
		loaded, err := c.loadStoreAtLeast(ctx, tenantID, minVersion)
		if err != nil {
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "miss_error")
			return nil, Manifest{}, err
		}
		c.mu.Lock()
		entry, ok = c.entries[tenantID]
		if ok && cacheEntryNewerThanLoaded(entry, loaded) && entry.manifest.Version >= minVersion {
			c.mu.Unlock()
			c.finishLoad(tenantID, load)
			c.recordCache(tenantID, "hit_newer")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
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
		cachedGraph := loaded.Graph
		if !shared {
			cachedGraph = loaded.Graph.Clone()
		}
		c.entries[tenantID] = cacheEntry{
			graph: cachedGraph, manifest: loaded.Manifest, meta: loaded.Meta,
			cachedAt: now, expiresAt: now.Add(c.TTL), lastAccess: now,
		}
		c.mu.Unlock()
		c.finishLoad(tenantID, load)
		c.recordVisible(tenantID, loaded.Manifest.Version)
		return loaded.Graph, loaded.Manifest, nil
	}
}

func (c *ReaderCache) loadStoreAtLeast(
	ctx context.Context,
	tenantID string,
	minVersion int64,
) (loadedGraph, error) {
	loaded, err := c.Store.loadWithMeta(ctx, tenantID)
	if err != nil {
		return loadedGraph{}, err
	}
	if minVersion > 0 && loaded.Manifest.Version < minVersion {
		return loadedGraph{}, fmt.Errorf(
			"loaded graph version %d is below required version %d",
			loaded.Manifest.Version,
			minVersion,
		)
	}
	return loaded, nil
}

func cacheEntryMatchesManifest(entry cacheEntry, manifest Manifest, meta ObjectMeta) bool {
	return cachedManifestMatches(
		loadedGraph{Manifest: entry.manifest, Meta: entry.meta},
		manifest,
		meta,
	)
}

func cacheEntryNewerThanLoaded(entry cacheEntry, loaded loadedGraph) bool {
	entryRevision := coordinatedMetaRevision(entry.meta)
	loadedRevision := coordinatedMetaRevision(loaded.Meta)
	if entryRevision > 0 || loadedRevision > 0 {
		return entryRevision > loadedRevision
	}
	return entry.manifest.Version > loaded.Manifest.Version
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

func (c *ReaderCache) Refresh(ctx context.Context, tenantID string) (*graph.Graph, Manifest, error) {
	return c.refresh(ctx, tenantID, true)
}

func (c *ReaderCache) refresh(ctx context.Context, tenantID string, markAccess bool) (*graph.Graph, Manifest, error) {
	c.mu.RLock()
	startGen := c.gens[tenantID]
	revalidationEntry, revalidationOK := c.entries[tenantID]
	c.mu.RUnlock()
	if !revalidationOK && !markAccess {
		return nil, Manifest{}, nil
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
	manifest, manifestMeta, err := c.manifestForCacheEntry(
		ctx, tenantID, revalidationEntry, revalidationOK,
	)
	if err != nil {
		if errors.Is(err, ErrCoordinatorUnavailable) {
			_, _ = c.extendStaleEntry(tenantID, 0, markAccess)
		}
		c.finishLoad(tenantID, load)
		return nil, Manifest{}, err
	}
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	c.mu.RUnlock()
	if !ok && !markAccess {
		c.finishLoad(tenantID, load)
		return nil, Manifest{}, nil
	}
	if ok && cacheEntryMatchesManifest(entry, manifest, manifestMeta) {
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
			c.finishLoad(tenantID, load)
			return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
		}
		c.entries[tenantID] = entry
		c.mu.Unlock()
		c.finishLoad(tenantID, load)
		return cacheEntryGraph(entry, !markAccess)
	}
	loaded, err := c.loadStoreAtLeast(ctx, tenantID, 0)
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
	cachedGraph := loaded.Graph
	if markAccess {
		cachedGraph = loaded.Graph.Clone()
	}
	c.entries[tenantID] = cacheEntry{
		graph: cachedGraph, manifest: loaded.Manifest, meta: loaded.Meta,
		cachedAt: now, expiresAt: now.Add(c.TTL), lastAccess: lastAccess,
	}
	c.mu.Unlock()
	c.finishLoad(tenantID, load)
	return loaded.Graph, loaded.Manifest, nil
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
	return cacheEntryGraph(entry, false)
}

func cacheEntryGraph(entry cacheEntry, shared bool) (*graph.Graph, Manifest, error) {
	if entry.graph == nil {
		return nil, entry.manifest, nil
	}
	if shared {
		return entry.graph, entry.manifest, nil
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
