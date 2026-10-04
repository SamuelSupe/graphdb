package storage

import (
	"context"
	"strings"
	"sync/atomic"
)

// The durable manifest is the publication point for both direct and WAL writes.
// Prepared data and idempotency records must already be durable on entry.
func (s *TenantStore) publishCommittedGraph(ctx context.Context, tenantID string, before, after loadedGraph) error {
	meta, err := s.putManifestMeta(ctx, tenantID, after.Manifest, before.Meta)
	if err != nil {
		s.handleManifestPublishFailureCache(tenantID, before, err)
		return err
	}
	after.Meta = meta
	s.setWriteCache(tenantID, after)
	return nil
}

// All commit paths install immutable graphs here after durable manifest
// publication. Cache retention never controls whether a committed view exists.
func (s *TenantStore) publishGraph(tenantID string, loaded loadedGraph) {
	s.bumpReadGeneration(tenantID)
	for _, cache := range s.readerCaches() {
		cache.publishGraph(tenantID, loaded)
	}
}

// Direct file replacement (including restore) invalidates readers before a
// newly published graph can be installed. This also fences in-flight cache loads.
func (s *TenantStore) localFileChanged(key string) {
	prefix := s.Prefix + "/tenants/"
	if s.Prefix == "" {
		prefix = "tenants/"
	}
	if !strings.HasPrefix(key, prefix) {
		return
	}
	tenant, name, ok := strings.Cut(strings.TrimPrefix(key, prefix), "/")
	if !ok {
		return
	}
	if name == "control/writer-lease.parquet" {
		s.deleteCachedWriterLease(tenant)
		// A fence update does not change the graph. Retain the immutable view
		// for manifest revalidation while still discarding all writer state.
		s.invalidateReadViews(tenant, true)
		s.invalidateTenantObjectCaches(tenant)
		return
	}
	manifest := name == "manifest.parquet"
	invalidate := name == "metadata.parquet" || (strings.HasPrefix(name, "config/") && name != "config/backup-schedule.parquet")
	if manifest || invalidate || name == "indexes/catalog.parquet" {
		s.queryMu.Lock()
		delete(s.queryUnavailable, tenant)
		s.queryMu.Unlock()
	}
	if !manifest && !invalidate {
		return
	}
	s.invalidateReadViews(tenant, manifest)
}

func (s *TenantStore) readerCaches() []*ReaderCache {
	s.viewsMu.RLock()
	defer s.viewsMu.RUnlock()
	return append([]*ReaderCache(nil), s.readViews...)
}

func (s *TenantStore) readGeneration(tenant string) uint64 {
	if value, ok := s.viewGenerations.Load(tenant); ok {
		return value.(*atomic.Uint64).Load()
	}
	return 0
}

func (s *TenantStore) bumpReadGeneration(tenant string) {
	value, _ := s.viewGenerations.LoadOrStore(tenant, &atomic.Uint64{})
	value.(*atomic.Uint64).Add(1)
}

func (s *TenantStore) invalidateReadViews(tenant string, keepGraph bool) {
	s.bumpReadGeneration(tenant)
	for _, cache := range s.readerCaches() {
		if keepGraph {
			cache.expirePublishedView(tenant)
		} else {
			cache.invalidate(tenant)
		}
	}
}
