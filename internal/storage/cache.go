package storage

import (
	"context"
	"errors"
	"fmt"
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

func (c *ReaderCache) startStoreLoad(
	parent context.Context,
	tenantID string,
	minVersion int64,
	startGen uint64,
	load *cacheLoad,
	cached cacheEntry,
	cachedOK bool,
	manifest Manifest,
	manifestMeta ObjectMeta,
) error {
	parent, releaseView, err := c.Store.ReadViewContext(parent, tenantID)
	if err != nil {
		return c.finishLoad(tenantID, load, err)
	}
	release, err := c.acquireStoreLoad(parent)
	if err != nil {
		releaseView()
		err = c.finishLoad(tenantID, load, err)
		if err != nil {
			c.recordCache(tenantID, "miss_rejected")
		}
		return err
	}
	// A cold load is shared by later requests, so the first caller must not be
	// able to abandon it after admission. LoadTimeout still bounds the work.
	loadCtx := context.WithoutCancel(parent)
	var cancel context.CancelFunc
	if c.LoadTimeout > 0 {
		loadCtx, cancel = context.WithTimeout(loadCtx, c.LoadTimeout)
	} else {
		loadCtx, cancel = context.WithCancel(loadCtx)
	}
	releaseStoreLoad := sync.OnceFunc(func() {
		cancel()
		release()
		releaseView()
	})
	c.setLoadCancel(load, cancel, true)
	go func() {
		defer releaseStoreLoad()
		loaded, err := c.loadStoreAtLeastFromEntry(
			loadCtx, tenantID, minVersion,
			cached, cachedOK, manifest, manifestMeta,
		)
		// Publish completion only after the global load slot is reusable. This
		// keeps a waiter from observing a finished load and immediately being
		// rejected by admission for work that has already stopped.
		releaseStoreLoad()
		if err != nil {
			if c.finishLoad(tenantID, load, err) != nil {
				c.recordCache(tenantID, "miss_error")
			}
			return
		}

		c.mu.Lock()
		entry, ok := c.entries[tenantID]
		if ok && cacheEntryNewerThanLoaded(entry, loaded) && entry.manifest.Version >= minVersion {
			c.mu.Unlock()
			c.recordCache(tenantID, "hit_newer")
			c.recordVisible(tenantID, entry.manifest.Version)
			c.finishLoad(tenantID, load, nil)
			return
		}
		if c.Store.readGeneration(tenantID) != startGen {
			c.mu.Unlock()
			c.finishLoad(tenantID, load, nil)
			return
		}
		now := time.Now()
		next := cacheEntry{
			graph: loaded.Graph, manifest: loaded.Manifest, meta: loaded.Meta,
			cachedAt: now, expiresAt: c.expiry(now), lastAccess: now,
			bytes: readerCacheEntryBytes(loaded),
		}
		if err := c.storeEntryLocked(tenantID, next); err != nil {
			c.mu.Unlock()
			if errors.Is(err, ErrReaderCacheEntryTooLarge) {
				c.recordCache(tenantID, "miss_uncached")
				c.recordVisible(tenantID, loaded.Manifest.Version)
				c.finishUncachedLoad(tenantID, load, next)
				return
			}
			c.recordCache(tenantID, "miss_error")
			c.finishLoad(tenantID, load, err)
			return
		}
		c.mu.Unlock()
		c.recordVisible(tenantID, loaded.Manifest.Version)
		c.finishLoad(tenantID, load, nil)
	}()
	return nil
}

func (c *ReaderCache) acquireStoreLoad(parent context.Context) (func(), error) {
	c.mu.RLock()
	slots := c.loadSlots
	queueTimeout := c.LoadQueueTimeout
	c.mu.RUnlock()
	if slots == nil {
		return func() {}, nil
	}
	ctx := parent
	cancel := func() {}
	if queueTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, queueTimeout)
	}
	defer cancel()
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		return nil, fmt.Errorf("%w after %s", ErrReaderLoadBusy, queueTimeout)
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

func (c *ReaderCache) loadStoreAtLeastFromEntry(
	ctx context.Context,
	tenantID string,
	minVersion int64,
	cached cacheEntry,
	cachedOK bool,
	manifest Manifest,
	manifestMeta ObjectMeta,
) (loadedGraph, error) {
	// A commit publishes this immutable graph after its manifest is durable,
	// before the slower index refresh finishes. Do not replay that commit again.
	if loaded, ok := c.Store.getWriteCache(tenantID); ok && loaded.Graph != nil &&
		cachedManifestMatches(loaded, manifest, manifestMeta) && loaded.Manifest.Version >= minVersion {
		c.recordCache(tenantID, "committed_graph_reuse")
		return loaded, nil
	}

	return c.loadStoreAtLeast(ctx, tenantID, minVersion)
}

func cacheEntryMatchesManifest(entry cacheEntry, manifest Manifest, meta ObjectMeta) bool {
	return cachedManifestMatches(
		loadedGraph{Manifest: entry.manifest, Meta: entry.meta},
		manifest,
		meta,
	)
}

func cacheEntryMatchesLogicalGraph(entry cacheEntry, manifest Manifest) bool {
	if entry.graph == nil || entry.graph.Version != manifest.Version ||
		entry.manifest.TenantID != manifest.TenantID ||
		entry.manifest.Version != manifest.Version ||
		entry.manifest.HeadCommitID != manifest.HeadCommitID {
		return false
	}
	return entry.manifest.DataHash != "" && entry.manifest.DataHash == manifest.DataHash
}

func cacheEntryNewerThanLoaded(entry cacheEntry, loaded loadedGraph) bool {
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

func (c *ReaderCache) beginLoad(ctx context.Context, tenantID string, minVersion int64) (*cacheLoad, bool, error) {
	c.mu.Lock()
	if current := c.loading[tenantID]; current != nil {
		c.mu.Unlock()
		if err := waitCacheLoad(ctx, current); err != nil {
			if ctx.Err() == nil && current.abandoned {
				return current, false, nil
			}
			return nil, false, err
		}
		return current, false, nil
	}
	phaseCtx, cancel := context.WithCancel(ctx)
	load := &cacheLoad{done: make(chan struct{}), phaseCtx: phaseCtx, cancelPhase: cancel, cancel: cancel, minVersion: minVersion}
	if c.loading == nil {
		c.loading = map[string]*cacheLoad{}
	}
	c.loading[tenantID] = load
	c.mu.Unlock()
	return load, true, nil
}

func waitCacheLoad(ctx context.Context, load *cacheLoad) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-load.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return load.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ReaderCache) setLoadCancel(load *cacheLoad, cancel context.CancelFunc, independent bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	load.cancel = cancel
	load.independent = independent
	if load.superseded {
		cancel()
	}
}

func (c *ReaderCache) finishLoad(tenantID string, load *cacheLoad, err error) error {
	if err != nil {
		// Cover metadata, admission and refresh failures as well as background
		// reads. Publication across all caches must finish before this decision.
		publication := c.Store.readPublication(tenantID)
		publication.mu.RLock()
		defer publication.mu.RUnlock()
	}
	c.mu.Lock()
	entry, exists := c.entries[tenantID]
	usable := exists && cacheEntryFresh(entry, time.Now(), load.minVersion) && c.Store.readGeneration(tenantID) == load.supersededGeneration
	if load.superseded && (errors.Is(err, context.Canceled) || usable) {
		err = nil
	}
	// Synchronous metadata/admission and refresh still follow their initiating
	// caller. Its cancellation must not fail a different live waiter.
	load.abandoned = !load.independent && load.phaseCtx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
	if load.cancel != nil {
		load.cancel()
	}
	if load.cancelPhase != nil {
		load.cancelPhase()
	}
	load.err = err
	if c.loading[tenantID] == load {
		delete(c.loading, tenantID)
	}
	close(load.done)
	c.mu.Unlock()
	return err
}

func (c *ReaderCache) finishUncachedLoad(tenantID string, load *cacheLoad, entry cacheEntry) {
	c.mu.Lock()
	if load.cancel != nil {
		load.cancel()
	}
	if load.cancelPhase != nil {
		load.cancelPhase()
	}
	load.uncached = entry
	load.bypass = true
	if c.loading[tenantID] == load {
		delete(c.loading, tenantID)
	}
	close(load.done)
	c.mu.Unlock()
}

func cacheEntryFresh(entry cacheEntry, now time.Time, minVersion int64) bool {
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		return false
	}
	return minVersion <= 0 || entry.manifest.Version >= minVersion
}

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
	manifest, manifestMeta, err := c.Store.getManifest(load.phaseCtx, tenantID)
	if err != nil {
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	c.mu.RLock()
	entry, ok := c.entries[tenantID]
	c.mu.RUnlock()
	if !ok && !markAccess {
		c.finishLoad(tenantID, load, nil)
		return nil, Manifest{}, nil
	}
	if ok && cacheEntryMatchesManifest(entry, manifest, manifestMeta) {
		now := time.Now()
		if entry.cachedAt.IsZero() {
			entry.cachedAt = now
		}
		entry.expiresAt = c.expiry(now)
		if markAccess {
			entry.lastAccess = now
		}
		c.mu.Lock()
		if c.Store.readGeneration(tenantID) != startGen {
			c.mu.Unlock()
			c.finishLoad(tenantID, load, nil)
			return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
		}
		if current, exists := c.entries[tenantID]; exists && current.graph == entry.graph {
			current.cachedAt, current.expiresAt, current.lastAccess = entry.cachedAt, entry.expiresAt, entry.lastAccess
			current.manifest, current.meta = entry.manifest, entry.meta
			entry = current
			c.entries[tenantID] = current
		}
		c.mu.Unlock()
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
		if markAccess {
			entry.lastAccess = now
		}
		c.mu.Lock()
		if c.Store.readGeneration(tenantID) != startGen {
			c.mu.Unlock()
			c.finishLoad(tenantID, load, nil)
			return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
		}
		if current, exists := c.entries[tenantID]; exists && current.graph == entry.graph {
			current.cachedAt, current.expiresAt, current.lastAccess = entry.cachedAt, entry.expiresAt, entry.lastAccess
			current.manifest, current.meta = entry.manifest, entry.meta
			entry = current
			c.entries[tenantID] = current
		}
		c.mu.Unlock()
		c.finishLoad(tenantID, load, nil)
		c.recordCache(tenantID, "revalidated_logical_graph")
		return cacheEntryGraph(entry, !markAccess)
	}
	viewCtx, releaseView, err := c.Store.ReadViewContext(load.phaseCtx, tenantID)
	if err != nil {
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	release, err := c.acquireStoreLoad(viewCtx)
	if err != nil {
		releaseView()
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
		if err = c.finishLoad(tenantID, load, err); err != nil {
			return nil, Manifest{}, err
		}
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	c.mu.Lock()
	if c.Store.readGeneration(tenantID) != startGen {
		c.mu.Unlock()
		c.finishLoad(tenantID, load, nil)
		return c.reloadAfterGenerationChange(ctx, tenantID, markAccess)
	}
	now := time.Now()
	lastAccess := now
	if ok && !markAccess {
		lastAccess = entry.lastAccess
	}
	next := cacheEntry{
		graph: loaded.Graph, manifest: loaded.Manifest, meta: loaded.Meta,
		cachedAt: now, expiresAt: c.expiry(now), lastAccess: lastAccess,
		bytes: readerCacheEntryBytes(loaded),
	}
	if err := c.storeEntryLocked(tenantID, next); err != nil {
		c.mu.Unlock()
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
	c.finishLoad(tenantID, load, nil)
	return cacheEntryGraph(next, !markAccess)
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
		c.mu.Unlock()
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
	status.LastAccess = entry.lastAccess
	return status
}

func cacheEntryIdle(entry cacheEntry, now time.Time, ttl time.Duration) bool {
	if ttl <= 0 || entry.lastAccess.IsZero() {
		return false
	}
	return !now.Before(entry.lastAccess.Add(ttl))
}

func (c *ReaderCache) expiry(now time.Time) time.Time {
	if c.TTL <= 0 {
		return time.Time{}
	}
	return now.Add(c.TTL)
}
