package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (c *ReaderCache) Refresh(ctx context.Context, tenantID string) (*graph.Graph, Manifest, error) {
	return c.refresh(ctx, tenantID, true)
}

func (c *ReaderCache) refresh(ctx context.Context, tenantID string, markAccess bool) (*graph.Graph, Manifest, error) {
	c.mu.RLock()
	startGen := c.Store.readGeneration(tenantID)
	_, revalidationOK := c.entries[tenantID]
	c.mu.RUnlock()
	if !revalidationOK && !markAccess {
		return nil, Manifest{}, nil
	}
	load, acquired, err := c.beginLoad(ctx, tenantID, 0)
	if err != nil {
		return nil, Manifest{}, err
	}
	if !acquired {
		if !markAccess {
			return nil, Manifest{}, nil
		}
		return c.Load(ctx, tenantID)
	}
	loadDone := false
	defer func() {
		if !loadDone {
			c.finishLoad(tenantID, load, errors.New("reader cache refresh aborted"))
		}
	}()
	manifest, manifestMeta, err := c.Store.getManifest(load.phaseCtx, tenantID)
	if err != nil {
		loadDone = true
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	c.mu.RUnlock()
	if !ok && !markAccess {
		loadDone = true
		c.finishLoad(tenantID, load, nil)
		return nil, Manifest{}, nil
	}
	if ok && cacheEntryMatchesManifest(entry, manifest, manifestMeta) {
		now := time.Now()
		if entry.cachedAt.IsZero() {
			entry.cachedAt = now
		}
		entry.expiresAt = c.expiry(now)
		c.mu.Lock()
		if c.Store.readGeneration(tenantID) != startGen {
			c.mu.Unlock()
			loadDone = true
			c.finishLoad(tenantID, load, nil)
			return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
		}
		if current, exists := c.entries[tenantID]; exists && current.graph == entry.graph {
			current.cachedAt, current.expiresAt = entry.cachedAt, entry.expiresAt
			if markAccess {
				current.initAccess(now)
			}
			current.manifest, current.meta = entry.manifest, entry.meta
			entry = current
			c.entries[tenantID] = current
		}
		c.mu.Unlock()
		loadDone = true
		c.finishLoad(tenantID, load, nil)
		return cacheEntryGraph(entry, !markAccess)
	}
	if ok && cacheEntryMatchesLogicalGraph(entry, manifest) {
		now := time.Now()
		if entry.cachedAt.IsZero() {
			entry.cachedAt = now
		}
		entry.manifest = manifest
		entry.meta = manifestMeta
		entry.expiresAt = c.expiry(now)
		c.mu.Lock()
		if c.Store.readGeneration(tenantID) != startGen {
			c.mu.Unlock()
			loadDone = true
			c.finishLoad(tenantID, load, nil)
			return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
		}
		if current, exists := c.entries[tenantID]; exists && current.graph == entry.graph {
			current.cachedAt, current.expiresAt = entry.cachedAt, entry.expiresAt
			if markAccess {
				current.initAccess(now)
			}
			current.manifest, current.meta = entry.manifest, entry.meta
			entry = current
			c.entries[tenantID] = current
		}
		c.mu.Unlock()
		loadDone = true
		c.finishLoad(tenantID, load, nil)
		c.recordCache(tenantID, "revalidated_logical_graph")
		return cacheEntryGraph(entry, !markAccess)
	}
	viewCtx, releaseView, err := c.Store.ReadViewContext(load.phaseCtx, tenantID)
	if err != nil {
		loadDone = true
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	release, err := c.acquireStoreLoad(viewCtx)
	if err != nil {
		releaseView()
		loadDone = true
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	var loadCtx context.Context
	var cancel context.CancelFunc
	if c.LoadTimeout > 0 {
		loadCtx, cancel = context.WithTimeout(viewCtx, c.LoadTimeout)
	} else {
		loadCtx, cancel = context.WithCancel(viewCtx)
	}
	c.setLoadCancel(load, cancel, false)
	releaseLoad := sync.OnceFunc(func() { cancel(); release(); releaseView() })
	defer releaseLoad()
	loaded, err := c.loadStoreAtLeastFromEntry(
		loadCtx, tenantID, 0,
		entry, ok, manifest, manifestMeta,
	)
	releaseLoad()
	if err != nil {
		loadDone = true
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	c.mu.Lock()
	if c.Store.readGeneration(tenantID) != startGen {
		c.mu.Unlock()
		loadDone = true
		c.finishLoad(tenantID, load, nil)
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	now := time.Now()
	lastAccess := now
	if ok && !markAccess {
		lastAccess = entry.lastAccessTime()
	}
	next := cacheEntry{
		graph: loaded.Graph, manifest: loaded.Manifest, meta: loaded.Meta,
		cachedAt: now, expiresAt: c.expiry(now), lastAccess: lastAccess,
		bytes: readerCacheEntryBytes(loaded),
	}
	if err := c.storeEntryLocked(tenantID, next); err != nil {
		c.mu.Unlock()
		loadDone = true
		if errors.Is(err, ErrReaderCacheEntryTooLarge) {
			c.finishUncachedLoad(tenantID, load, next)
			c.recordCache(tenantID, "miss_uncached")
			c.recordVisible(tenantID, loaded.Manifest.Version)
			if !markAccess {
				return nil, Manifest{}, nil
			}
			return cacheEntryGraph(next, false)
		}
		c.finishLoad(tenantID, load, err)
		return nil, Manifest{}, err
	}
	c.mu.Unlock()
	loadDone = true
	c.finishLoad(tenantID, load, nil)
	return cacheEntryGraph(next, !markAccess)
}

func (c *ReaderCache) touch(tenantID string) {
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	c.mu.RUnlock()
	if !ok {
		return
	}
	if entry.touchAccess(now) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok = c.entries[tenantID]
	if !ok {
		return
	}
	entry.initAccess(now)
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

// Keep the immutable graph as a catch-up base. Compaction can replace the
// manifest without changing the graph, and a full reload would stall readers.
func (c *ReaderCache) expirePublishedView(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[tenantID]; ok {
		entry.expiresAt = time.Unix(1, 0)
		c.entries[tenantID] = entry
	}
}

func (c *ReaderCache) Invalidate(tenantID string) {
	c.Store.invalidateReadViews(tenantID, false)
}

func (c *ReaderCache) invalidate(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteEntryLocked(tenantID)
}

func (c *ReaderCache) InvalidateAll() {
	c.mu.Lock()
	tenants := make([]string, 0, len(c.entries)+len(c.loading))
	seen := make(map[string]struct{}, len(c.entries)+len(c.loading))
	for tenantID := range c.entries {
		seen[tenantID] = struct{}{}
		tenants = append(tenants, tenantID)
	}
	for tenantID := range c.loading {
		if _, ok := seen[tenantID]; !ok {
			tenants = append(tenants, tenantID)
		}
	}
	clear(c.entries)
	c.bytes = 0
	c.mu.Unlock()
	if c.Store != nil {
		for _, tenantID := range tenants {
			c.Store.invalidateReadViews(tenantID, false)
		}
	}
}

// PublishFromWriteCache makes an already-persisted copy-on-write graph visible
// to readers without decoding the same manifest again. Public cache loads still
// clone the graph, and shared callbacks retain their read-only contract.
func (c *ReaderCache) PublishFromWriteCache(tenantID string) bool {
	loaded, ok := c.Store.getWriteCache(tenantID)
	if !ok || loaded.Graph == nil {
		c.Invalidate(tenantID)
		return false
	}

	return c.publishGraph(tenantID, loaded)
}

func (c *ReaderCache) publishGraph(tenantID string, loaded loadedGraph) bool {
	now := time.Now()
	c.mu.Lock()
	if current, exists := c.entries[tenantID]; exists &&
		cacheEntryNewerThanLoaded(current, loaded) {
		c.mu.Unlock()
		return true
	}
	next := cacheEntry{
		graph:      loaded.Graph,
		manifest:   loaded.Manifest,
		meta:       loaded.Meta,
		cachedAt:   now,
		expiresAt:  c.expiry(now),
		lastAccess: now,
		bytes:      readerCacheEntryBytes(loaded),
	}
	if err := c.storeEntryLocked(tenantID, next); err != nil {
		// Fail closed: evict any stale pre-commit entry and supersede obsolete
		// in-flight loads so live readers transparently reload from disk instead
		// of failing with spurious context.Canceled.
		c.deleteEntryLocked(tenantID)
		var cancel context.CancelFunc
		if load := c.loading[tenantID]; load != nil && load.cancel != nil {
			load.superseded = true
			load.supersededGeneration = c.Store.readGeneration(tenantID)
			cancel = load.cancel
		}
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return false
	}
	var cancel context.CancelFunc
	if load := c.loading[tenantID]; load != nil && load.cancel != nil && loaded.Manifest.Version >= load.minVersion {
		// A durable resident graph makes the shared disk load obsolete. Cancel
		// it so waiters can reuse publication after its slot and view are released.
		load.superseded = true
		load.supersededGeneration = c.Store.readGeneration(tenantID)
		cancel = load.cancel
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.recordCache(tenantID, "write_through")
	c.recordVisible(tenantID, loaded.Manifest.Version)
	return true
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
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	if !ok || entry.manifest.Version < minVersion {
		c.mu.RUnlock()
		return nil, Manifest{}, false, nil
	}
	c.mu.RUnlock()
	if !entry.touchAccess(now) {
		c.touch(tenantID)
	}
	c.recordCache(tenantID, "hit_after_timeout")
	c.recordVisible(tenantID, entry.manifest.Version)
	g, manifest, err := cloneCacheEntry(entry)
	return g, manifest, true, err
}

// WithCachedReadOnlyGraphAtLeast lends a cached immutable snapshot without
// cloning when its version already satisfies minVersion.
func (c *ReaderCache) WithCachedReadOnlyGraphAtLeast(tenantID string, minVersion int64, fn func(*graph.Graph, Manifest) error) (bool, error) {
	if c == nil || fn == nil {
		return false, nil
	}
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	if !ok || entry.manifest.Version < minVersion {
		c.mu.RUnlock()
		return false, nil
	}
	c.mu.RUnlock()
	if !entry.touchAccess(now) {
		c.touch(tenantID)
	}
	c.recordCache(tenantID, "hit_after_timeout")
	c.recordVisible(tenantID, entry.manifest.Version)
	g, manifest, err := cacheEntryGraph(entry, true)
	if err != nil {
		return true, err
	}
	return true, fn(g, manifest)
}

func (c *ReaderCache) Status(tenantID string) ReaderCacheStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	status := ReaderCacheStatus{TTL: c.TTL, IdleTTL: c.IdleTTL}
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
	status.LastAccess = entry.lastAccessTime()
	return status
}

func cacheEntryIdle(entry cacheEntry, now time.Time, ttl time.Duration) bool {
	lastAccess := entry.lastAccessTime()
	if ttl <= 0 || lastAccess.IsZero() {
		return false
	}
	return !now.Before(lastAccess.Add(ttl))
}

func (c *ReaderCache) expiry(now time.Time) time.Time {
	if c.TTL <= 0 {
		return time.Time{}
	}
	return now.Add(c.TTL)
}
