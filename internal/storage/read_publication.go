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
	s.setWriteCacheContext(ctx, tenantID, after)
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
	s.localFileChangedContext(context.Background(), key)
}

func (s *TenantStore) invalidateDirectObjectChange(key string) {
	s.clearWriterObjectKey(key)
	s.deleteObjectKeyCached(key)
	for objects := s.Objects; objects != nil; {
		if protected, ok := objects.(*ReadProtectedObjectStore); ok {
			protected.invalidateRead(key)
		}
		unwrapper, ok := objects.(objectStoreUnwrapper)
		if !ok {
			break
		}
		next := unwrapper.UnwrapObjectStore()
		if next == objects {
			break
		}
		objects = next
	}
}

func (s *TenantStore) localFileChangedContext(ctx context.Context, key string) {
	cache := FindWriterObjectCache(s.Objects)
	viaWriterCache := cache != nil && ctx != nil && ctx.Value(writerObjectCacheMutationKey{}) == cache
	if !viaWriterCache && s.localFileStore() != nil {
		s.invalidateDirectObjectChange(key)
	}
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
	taskName := strings.TrimPrefix(name, "indexes/")
	taskObject := strings.HasPrefix(taskName, "tasks/") && !strings.Contains(strings.TrimPrefix(taskName, "tasks/"), "/")
	lease := name == "control/writer-lease.parquet"
	metadata := name == "metadata.parquet" || name == "control/tenant-metadata-backup.parquet" || name == "control/tenant-metadata-pending.parquet"
	if taskObject {
		s.taskObjectChanged(tenant, name)
	}
	if strings.HasPrefix(name, "ingest/") {
		if strings.Contains(name, "/collectors/") {
			s.deleteCachedCollectorStatus(key)
		} else if !s.MaterializeCollectorStatus && strings.Contains(name, "/batches/") {
			s.deleteCachedTenantCollectorStatuses(tenant)
		}
	}
	if strings.HasPrefix(name, "control/readers/") {
		s.lockMu.Lock()
		clear(s.readerHeartbeatCache)
		s.lockMu.Unlock()
	}
	if lease {
		s.deleteCachedWriterLease(tenant)
		// A fence update does not change the graph. Retain the immutable view
		// for manifest revalidation while still discarding all writer state.
		s.invalidateReadViews(tenant, true)
		s.invalidateTenantObjectCaches(tenant)
		return
	}
	if metadata {
		s.deleteCachedTenantMetadata(tenant)
		s.invalidateTenantTaskHeaders(tenant)
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
		prefix := tenant + "\x00"
		s.lockMu.Lock()
		for key := range s.compiledScanCatalogCache {
			if strings.HasPrefix(key, prefix) {
				delete(s.compiledScanCatalogCache, key)
			}
		}
		s.lockMu.Unlock()
	case "extensions/v1.1/reverse-index/catalog.json":
		s.deleteCachedReverseIndexCatalog(tenant)
	case "manifest.parquet":
		s.deleteWriteCache(tenant)
	}
	manifest := name == "manifest.parquet"
	invalidate := metadata || (strings.HasPrefix(name, "config/") && name != "config/backup-schedule.parquet")
	if manifest || invalidate || name == "indexes/catalog.parquet" || name == "extensions/v1.1/reverse-index/catalog.json" {
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
	clear(s.terminalTaskHeaders)
	s.taskHeaderEpoch++
	s.taskMu.Unlock()

	s.queryMu.Lock()
	clear(s.queryUnavailable)
	s.queryMu.Unlock()

	for objects := s.Objects; objects != nil; {
		if protected, ok := objects.(*ReadProtectedObjectStore); ok {
			protected.mu.Lock()
			clear(protected.reads)
			protected.mu.Unlock()
		}
		unwrapper, ok := objects.(objectStoreUnwrapper)
		if !ok {
			break
		}
		next := unwrapper.UnwrapObjectStore()
		if next == objects {
			break
		}
		objects = next
	}
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
