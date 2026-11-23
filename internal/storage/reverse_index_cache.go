package storage

import (
	"context"
	"errors"
	"time"
)

type cachedReverseIndexCatalog struct {
	catalog ReverseIndexCatalog
	meta    ObjectMeta
	checked time.Time
}

type reverseIndexCatalogLoad struct {
	done       chan struct{}
	generation uint64
	catalog    ReverseIndexCatalog
	meta       ObjectMeta
	err        error
	canceled   bool
}

func (s *TenantStore) getCachedReverseIndexCatalogLocked(
	tenantID string,
	now time.Time,
) (ReverseIndexCatalog, ObjectMeta, bool) {
	cached, ok := s.reverseIndexCatalogCache[tenantID]
	if !ok || now.Sub(cached.checked) >= s.lifecycleCacheTTL() {
		return ReverseIndexCatalog{}, ObjectMeta{}, false
	}
	return copyReverseIndexCatalog(cached.catalog), cached.meta, true
}

func (s *TenantStore) loadReverseIndexCatalog(
	ctx context.Context,
	tenantID string,
) (ReverseIndexCatalog, ObjectMeta, error) {
	for {
		s.lockMu.Lock()
		if catalog, meta, ok := s.getCachedReverseIndexCatalogLocked(
			tenantID, time.Now(),
		); ok {
			s.lockMu.Unlock()
			return reverseIndexCatalogCacheResult(catalog, meta)
		}
		if load := s.reverseIndexCatalogLoads[tenantID]; load != nil {
			s.lockMu.Unlock()
			select {
			case <-load.done:
				if load.canceled && ctx.Err() == nil {
					continue
				}
				return copyReverseIndexCatalog(
					load.catalog,
				), load.meta, load.err
			case <-ctx.Done():
				return ReverseIndexCatalog{}, ObjectMeta{}, ctx.Err()
			}
		}
		load := &reverseIndexCatalogLoad{
			done:       make(chan struct{}),
			generation: s.reverseIndexCatalogGeneration[tenantID],
		}
		if s.reverseIndexCatalogLoads == nil {
			s.reverseIndexCatalogLoads = map[string]*reverseIndexCatalogLoad{}
		}
		s.reverseIndexCatalogLoads[tenantID] = load
		s.lockMu.Unlock()

		return s.runReverseIndexCatalogLoad(ctx, tenantID, load)
	}
}

func (s *TenantStore) runReverseIndexCatalogLoad(
	ctx context.Context,
	tenantID string,
	load *reverseIndexCatalogLoad,
) (ReverseIndexCatalog, ObjectMeta, error) {
	finished := false
	defer func() {
		if !finished {
			s.finishReverseIndexCatalogLoad(
				tenantID, load, ReverseIndexCatalog{}, ObjectMeta{},
				errors.New("reverse index catalog load aborted"), false,
			)
		}
	}()
	catalog, meta, err := s.getReverseIndexCatalogWithMeta(
		ctx, tenantID,
	)
	s.finishReverseIndexCatalogLoad(
		tenantID, load, catalog, meta, err,
		loadCanceledByContext(ctx, err),
	)
	finished = true
	return copyReverseIndexCatalog(load.catalog), load.meta, load.err
}

func (s *TenantStore) finishReverseIndexCatalogLoad(
	tenantID string,
	load *reverseIndexCatalogLoad,
	catalog ReverseIndexCatalog,
	meta ObjectMeta,
	err error,
	canceled bool,
) {
	s.lockMu.Lock()
	stale := s.reverseIndexCatalogGeneration[tenantID] != load.generation
	if cached, cachedMeta, ok := s.getCachedReverseIndexCatalogLocked(
		tenantID, time.Now(),
	); ok {
		catalog, meta = cached, cachedMeta
		_, _, err = reverseIndexCatalogCacheResult(cached, cachedMeta)
	} else if !stale {
		if err == nil {
			s.setReverseIndexCatalogCacheEntryLocked(
				tenantID,
				cachedReverseIndexCatalog{
					catalog: copyReverseIndexCatalog(catalog),
					meta:    meta,
					checked: time.Now(),
				},
			)
		} else if errors.Is(err, ErrNotFound) {
			s.setReverseIndexCatalogCacheEntryLocked(
				tenantID,
				cachedReverseIndexCatalog{
					meta:    meta,
					checked: time.Now(),
				},
			)
		}
	}
	load.catalog = copyReverseIndexCatalog(catalog)
	load.meta = meta
	load.err = err
	load.canceled = canceled
	if s.reverseIndexCatalogLoads[tenantID] == load {
		delete(s.reverseIndexCatalogLoads, tenantID)
	}
	close(load.done)
	s.lockMu.Unlock()
}

func reverseIndexCatalogCacheResult(
	catalog ReverseIndexCatalog,
	meta ObjectMeta,
) (ReverseIndexCatalog, ObjectMeta, error) {
	if !meta.Exists {
		return ReverseIndexCatalog{}, meta, ErrNotFound
	}
	return catalog, meta, nil
}

func (s *TenantStore) setCachedReverseIndexCatalog(
	tenantID string,
	catalog ReverseIndexCatalog,
	meta ObjectMeta,
) {
	s.setReverseIndexCatalogCacheEntry(tenantID, cachedReverseIndexCatalog{
		catalog: copyReverseIndexCatalog(catalog),
		meta:    meta,
		checked: time.Now(),
	})
}

func (s *TenantStore) setReverseIndexCatalogCacheEntry(
	tenantID string,
	entry cachedReverseIndexCatalog,
) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.bumpReverseIndexCatalogGenerationLocked(tenantID)
	s.setReverseIndexCatalogCacheEntryLocked(tenantID, entry)
}

func (s *TenantStore) setReverseIndexCatalogCacheEntryLocked(
	tenantID string,
	entry cachedReverseIndexCatalog,
) {
	evictOneCacheEntry(
		s.reverseIndexCatalogCache,
		tenantID,
		maxIndexCatalogCacheEntries,
	)
	s.reverseIndexCatalogCache[tenantID] = entry
}

func copyReverseIndexCatalog(
	catalog ReverseIndexCatalog,
) ReverseIndexCatalog {
	if catalog.compilation == nil {
		catalog.compilation = &reverseCatalogCompilation{}
	}
	catalog.EdgeShards = append([]EdgeShard(nil), catalog.EdgeShards...)
	for i := range catalog.EdgeShards {
		catalog.EdgeShards[i].Objects =
			append([]IndexObject(nil), catalog.EdgeShards[i].Objects...)
	}
	return catalog
}
