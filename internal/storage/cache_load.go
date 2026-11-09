package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

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
