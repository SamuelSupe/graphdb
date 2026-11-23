package storage

import (
	"context"
	"path"
	"strings"
	"sync"
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
	publication := s.readPublication(tenantID)
	publication.mu.Lock()
	defer publication.mu.Unlock()
	publication.generation.Add(1)
	for _, cache := range s.readerCaches() {
		cache.publishGraph(tenantID, loaded)
	}
}

// Direct file replacement (including restore) invalidates readers before a
// newly published graph can be installed. This also fences in-flight cache loads.
func (s *TenantStore) localFileChanged(key string) {
	if key == s.tenantRegistryKey() {
		s.clearRegisteredTenantCache()
		return
	}
	purgePrefix := path.Join(s.Prefix, "control", "tenant-purges") + "/"
	if s.Prefix == "" {
		purgePrefix = "control/tenant-purges/"
	}
	if strings.HasPrefix(key, purgePrefix) {
		s.clearPurgeTombstoneCache()
		return
	}
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
	if strings.HasPrefix(name, "tasks/") && !strings.Contains(strings.TrimPrefix(name, "tasks/"), "/") {
		s.taskObjectChanged(tenant, name)
	}
	if name == "control/writer-lease.parquet" {
		s.deleteCachedWriterLease(tenant)
		// A fence update does not change the graph. Retain the immutable view
		// for manifest revalidation while still discarding all writer state.
		s.invalidateReadViews(tenant, true)
		s.invalidateTenantObjectCaches(tenant)
		return
	}
	metadata := name == "metadata.parquet" || name == "control/tenant-metadata-backup.parquet" || name == "control/tenant-metadata-pending.parquet"
	if metadata {
		s.deleteCachedTenantMetadata(tenant)
		s.taskMu.Lock()
		delete(s.queuedTaskCatalogs, tenant)
		s.taskMu.Unlock()
	}
	switch name {
	case "config/tenant-config.parquet":
		s.deleteCachedTenantConfig(tenant)
	case "config/source-policy.parquet":
		s.deleteCachedSourcePolicy(tenant)
	case "indexes/catalog.parquet":
		s.deleteCachedIndexCatalog(tenant)
	case "indexes/reverse/catalog.parquet":
		s.deleteCachedReverseIndexCatalog(tenant)
	case "manifest.parquet":
		s.deleteWriteCache(tenant)
	}
	manifest := name == "manifest.parquet"
	invalidate := metadata || (strings.HasPrefix(name, "config/") && name != "config/backup-schedule.parquet")
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

// ResetCaches drops all in-memory tenant metadata, write, catalog, object-key,
// and attached reader caches (for example after full-store snapshot restore).
func (s *TenantStore) ResetCaches() {
	s.lockMu.Lock()
	clear(s.writeCache)
	s.writeCacheOrder = s.writeCacheOrder[:0]
	s.writeCacheBytes = 0
	clear(s.writerLeaseCache)
	clear(s.registeredTenantCache)
	clear(s.collectorStatusCache)
	clear(s.readerHeartbeatCache)
	clear(s.tenantMetadataCache)
	clear(s.purgeTombstoneCache)
	clear(s.sourcePolicyCache)
	clear(s.tenantConfigCache)
	for tenantID := range s.indexCatalogCache {
		s.bumpIndexCatalogGenerationLocked(tenantID)
	}
	for tenantID := range s.indexCatalogLoads {
		s.bumpIndexCatalogGenerationLocked(tenantID)
	}
	clear(s.indexCatalogCache)
	for tenantID := range s.reverseIndexCatalogCache {
		s.bumpReverseIndexCatalogGenerationLocked(tenantID)
	}
	for tenantID := range s.reverseIndexCatalogLoads {
		s.bumpReverseIndexCatalogGenerationLocked(tenantID)
	}
	clear(s.reverseIndexCatalogCache)
	clear(s.compiledScanCatalogCache)
	s.lockMu.Unlock()

	s.resetObjectKeyCache()

	s.taskMu.Lock()
	clear(s.queuedTaskCatalogs)
	s.taskMu.Unlock()

	s.queryMu.Lock()
	clear(s.queryUnavailable)
	s.queryMu.Unlock()

	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix("")
	}
	s.viewGenerations.Range(func(_, value any) bool {
		pub := value.(*readPublicationState)
		pub.mu.Lock()
		pub.generation.Add(1)
		pub.mu.Unlock()
		return true
	})
	for _, cache := range s.readerCaches() {
		cache.InvalidateAll()
	}
}

func (s *TenantStore) readerCaches() []*ReaderCache {
	s.viewsMu.RLock()
	defer s.viewsMu.RUnlock()
	return append([]*ReaderCache(nil), s.readViews...)
}

func (s *TenantStore) readGeneration(tenant string) uint64 {
	if value, ok := s.viewGenerations.Load(tenant); ok {
		return value.(*readPublicationState).generation.Load()
	}
	return 0
}

type readPublicationState struct {
	mu         sync.RWMutex
	generation atomic.Uint64
}

func (s *TenantStore) readPublication(tenant string) *readPublicationState {
	if value, ok := s.viewGenerations.Load(tenant); ok {
		return value.(*readPublicationState)
	}
	value, _ := s.viewGenerations.LoadOrStore(tenant, &readPublicationState{})
	return value.(*readPublicationState)
}

func (s *TenantStore) invalidateReadViews(tenant string, keepGraph bool) {
	publication := s.readPublication(tenant)
	publication.mu.Lock()
	defer publication.mu.Unlock()
	publication.generation.Add(1)
	for _, cache := range s.readerCaches() {
		if keepGraph {
			cache.expirePublishedView(tenant)
		} else {
			cache.invalidate(tenant)
		}
	}
}
