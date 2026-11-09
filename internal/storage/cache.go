package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type ReaderCache struct {
	Store            *TenantStore
	TTL              time.Duration
	IdleTTL          time.Duration
	MaxTenants       int
	MaxBytes         int64
	LoadTimeout      time.Duration
	LoadQueueTimeout time.Duration
	Observer         ReaderCacheObserver

	mu        sync.RWMutex
	entries   map[string]cacheEntry
	loading   map[string]*cacheLoad
	loadSlots chan struct{}
	bytes     int64
}

var ErrReaderLoadBusy = errors.New("reader graph load admission timeout")

type ReaderCacheObserver interface {
	RecordReaderCache(tenantID string, status string)
	RecordReaderVisibleVersion(tenantID string, version int64)
}

type cacheEntry struct {
	graph           *graph.Graph
	manifest        Manifest
	meta            ObjectMeta
	cachedAt        time.Time
	expiresAt       time.Time
	lastAccess      time.Time
	bytes           int64
	entityScanOrder *cachedEntityScanOrder
}

type cacheLoad struct {
	done     chan struct{}
	err      error
	uncached cacheEntry
	bypass   bool

	minVersion           int64
	phaseCtx             context.Context
	cancelPhase          context.CancelFunc
	cancel               context.CancelFunc
	independent          bool
	abandoned            bool
	superseded           bool
	supersededGeneration uint64
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
	IdleTTL    time.Duration
}

func NewReaderCache(store *TenantStore, ttl time.Duration) *ReaderCache {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	cache := &ReaderCache{
		Store:            store,
		TTL:              ttl,
		IdleTTL:          15 * time.Minute,
		MaxTenants:       defaultReaderCacheMaxTenants,
		MaxBytes:         defaultReaderCacheMaxBytes,
		LoadTimeout:      time.Minute,
		LoadQueueTimeout: 2 * time.Second,
		entries:          map[string]cacheEntry{},
		loading:          map[string]*cacheLoad{},
		loadSlots:        make(chan struct{}, 4),
	}
	if store != nil {
		store.viewsMu.Lock()
		store.readViews = append(store.readViews, cache)
		store.viewsMu.Unlock()
	}
	return cache
}

func (c *ReaderCache) ConfigureLoadAdmission(maxConcurrent int, queueTimeout time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LoadQueueTimeout = queueTimeout
	if maxConcurrent <= 0 {
		c.loadSlots = nil
		return
	}
	c.loadSlots = make(chan struct{}, maxConcurrent)
}

func (c *ReaderCache) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	c.Store.StartBackground(ctx, func(ctx context.Context) {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.cachedTenantsForRefresh()
			}
		}
	})
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

// WithCachedReadOnlyGraph lends a fresh immutable view only when it is already
// resident. A miss never loads the tenant; fn must not retain or mutate the view.
func (c *ReaderCache) WithCachedReadOnlyGraph(ctx context.Context, tenantID string, minVersion int64, fn func(*graph.Graph, Manifest) error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	usable := ok && cacheEntryFresh(entry, time.Now(), minVersion)
	c.mu.RUnlock()
	if !usable || fn == nil {
		return false, nil
	}
	c.touch(tenantID)
	c.recordCache(tenantID, "hit")
	c.recordVisible(tenantID, entry.manifest.Version)
	return true, fn(entry.graph, entry.manifest)
}

func (c *ReaderCache) load(ctx context.Context, tenantID string, minVersion int64, shared bool) (*graph.Graph, Manifest, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, Manifest{}, err
		}
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
		startGen := c.Store.readGeneration(tenantID)
		c.mu.RUnlock()

		load, acquired, err := c.beginLoad(ctx, tenantID, minVersion)
		if err != nil {
			return nil, Manifest{}, err
		}
		if !acquired {
			if load.bypass && load.uncached.manifest.Version >= minVersion {
				c.recordCache(tenantID, "miss_uncached")
				c.recordVisible(tenantID, load.uncached.manifest.Version)
				return cacheEntryGraph(load.uncached, shared)
			}
			c.recordCache(tenantID, "wait")
			continue
		}
		entry, ok, startGen, fresh := c.cacheEntryAfterLoadAcquire(
			tenantID,
			minVersion,
		)
		if fresh {
			c.finishLoad(tenantID, load, nil)
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		manifest, manifestMeta, err := c.Store.getManifest(load.phaseCtx, tenantID)
		if err != nil {
			if err = c.finishLoad(tenantID, load, err); err != nil {
				return nil, Manifest{}, err
			}
			continue
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
			c.finishLoad(tenantID, load, nil)
			c.recordCache(tenantID, "hit")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		if startGen != c.Store.readGeneration(tenantID) {
			c.mu.Unlock()
			c.finishLoad(tenantID, load, nil)
			continue
		}
		if ok &&
			cacheEntryMatchesManifest(entry, manifest, manifestMeta) &&
			entry.manifest.Version >= minVersion {
			if entry.cachedAt.IsZero() {
				entry.cachedAt = now
			}
			entry.expiresAt = c.expiry(now)
			entry.lastAccess = now
			c.entries[tenantID] = entry
			c.mu.Unlock()
			c.finishLoad(tenantID, load, nil)
			c.recordCache(tenantID, "revalidated")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		if ok &&
			cacheEntryMatchesLogicalGraph(entry, manifest) &&
			entry.manifest.Version >= minVersion {
			if entry.cachedAt.IsZero() {
				entry.cachedAt = now
			}
			entry.manifest = manifest
			entry.meta = manifestMeta
			entry.expiresAt = c.expiry(now)
			entry.lastAccess = now
			c.entries[tenantID] = entry
			c.mu.Unlock()
			c.finishLoad(tenantID, load, nil)
			c.recordCache(tenantID, "revalidated_logical_graph")
			c.recordVisible(tenantID, entry.manifest.Version)
			return cacheEntryGraph(entry, shared)
		}
		c.mu.Unlock()

		c.recordCache(tenantID, "miss")
		if err := c.startStoreLoad(
			load.phaseCtx, tenantID, minVersion, startGen, load,
			entry, ok, manifest, manifestMeta,
		); err != nil {
			return nil, Manifest{}, err
		}
		if err := waitCacheLoad(ctx, load); err != nil {
			return nil, Manifest{}, err
		}
		if load.bypass && load.uncached.manifest.Version >= minVersion {
			c.recordCache(tenantID, "miss_uncached")
			c.recordVisible(tenantID, load.uncached.manifest.Version)
			return cacheEntryGraph(load.uncached, shared)
		}
		c.mu.Lock()
		entry, ok = c.entries[tenantID]
		now = time.Now()
		if ok && cacheEntryFresh(entry, now, minVersion) {
			entry.lastAccess = now
			c.entries[tenantID] = entry
			c.mu.Unlock()
			return cacheEntryGraph(entry, shared)
		}
		c.mu.Unlock()
	}
}
