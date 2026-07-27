package storage

import (
	"context"
	"errors"
	"time"
)

type indexCatalogLoad struct {
	done     chan struct{}
	catalog  IndexCatalog
	meta     ObjectMeta
	err      error
	canceled bool
}

func (s *TenantStore) getCachedIndexCatalogFreshLocked(
	tenantID string,
	now time.Time,
) (IndexCatalog, ObjectMeta, bool) {
	cached, ok := s.indexCatalogCache[tenantID]
	if !ok || now.Sub(cached.checkedAt) >= s.lifecycleCacheTTL() {
		return IndexCatalog{}, ObjectMeta{}, false
	}
	return copyIndexCatalog(cached.catalog), cached.meta, true
}

func (s *TenantStore) loadIndexCatalog(
	ctx context.Context,
	tenantID string,
) (IndexCatalog, ObjectMeta, error) {
	for {
		s.lockMu.Lock()
		if catalog, meta, ok := s.getCachedIndexCatalogFreshLocked(
			tenantID, time.Now(),
		); ok {
			s.lockMu.Unlock()
			return indexCatalogCacheResult(catalog, meta)
		}
		if load := s.indexCatalogLoads[tenantID]; load != nil {
			s.lockMu.Unlock()
			select {
			case <-load.done:
				if load.canceled && ctx.Err() == nil {
					continue
				}
				return copyIndexCatalog(load.catalog), load.meta, load.err
			case <-ctx.Done():
				return IndexCatalog{}, ObjectMeta{}, ctx.Err()
			}
		}
		load := &indexCatalogLoad{done: make(chan struct{})}
		s.indexCatalogLoads[tenantID] = load
		s.lockMu.Unlock()

		catalog, meta, err := s.getIndexCatalogWithMeta(ctx, tenantID)
		s.finishIndexCatalogLoad(
			tenantID, load, catalog, meta, err,
			loadCanceledByContext(ctx, err),
		)
		return copyIndexCatalog(load.catalog), load.meta, load.err
	}
}

func (s *TenantStore) finishIndexCatalogLoad(
	tenantID string,
	load *indexCatalogLoad,
	catalog IndexCatalog,
	meta ObjectMeta,
	err error,
	canceled bool,
) {
	contentHash := catalog.contentHash
	if err == nil && contentHash == "" {
		contentHash, _ = indexCatalogContentHash(catalog)
	}

	s.lockMu.Lock()
	if cached, cachedMeta, ok := s.getCachedIndexCatalogFreshLocked(
		tenantID, time.Now(),
	); ok {
		catalog, meta = cached, cachedMeta
		_, _, err = indexCatalogCacheResult(cached, cachedMeta)
	} else if err == nil {
		catalog.contentHash = contentHash
		s.setCachedIndexCatalogEntryLocked(tenantID, cachedIndexCatalog{
			catalog:     copyIndexCatalog(catalog),
			meta:        meta,
			contentHash: contentHash,
			checkedAt:   time.Now(),
		})
	} else if errors.Is(err, ErrNotFound) {
		s.setCachedIndexCatalogEntryLocked(tenantID, cachedIndexCatalog{
			meta:      meta,
			checkedAt: time.Now(),
		})
	}
	load.catalog = copyIndexCatalog(catalog)
	load.meta = meta
	load.err = err
	load.canceled = canceled
	delete(s.indexCatalogLoads, tenantID)
	close(load.done)
	s.lockMu.Unlock()
}

func indexCatalogCacheResult(
	catalog IndexCatalog,
	meta ObjectMeta,
) (IndexCatalog, ObjectMeta, error) {
	if !meta.Exists {
		return IndexCatalog{}, meta, ErrNotFound
	}
	return catalog, meta, nil
}
