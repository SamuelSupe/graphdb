package storage

type cachedWriterLease struct {
	lease WriterLease
	meta  ObjectMeta
}

func (s *TenantStore) getCachedWriterLease(tenantID string) (WriterLease, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.writerLeaseCache[tenantID]
	if !ok {
		return WriterLease{}, ObjectMeta{}, false
	}
	if !s.cachedWriterLeaseUsable(cached.lease) {
		return WriterLease{}, ObjectMeta{}, false
	}
	return cached.lease, cached.meta, true
}

func (s *TenantStore) getCachedWriterLeaseAny(tenantID string) (WriterLease, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.writerLeaseCache[tenantID]
	return cached.lease, cached.meta, ok
}

func (s *TenantStore) setCachedWriterLease(tenantID string, lease WriterLease, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	evictOneCacheEntry(s.writerLeaseCache, tenantID, maxWriterMetadataCacheEntries)
	s.writerLeaseCache[tenantID] = cachedWriterLease{lease: lease, meta: meta}
}

func (s *TenantStore) deleteCachedWriterLease(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.writerLeaseCache, tenantID)
}

func (s *TenantStore) cachedWriterLeaseUsable(lease WriterLease) bool {
	if lease.OwnerID != s.InstanceID {
		return false
	}
	return lease.FenceToken != "" && lease.FenceEpoch > 0
}

func (s *TenantStore) invalidateWriterTakeoverState(tenantID string) {
	s.invalidateTenantState(tenantID)
}

func (s *TenantStore) invalidateTenantState(tenantID string) {
	s.invalidateReadViews(tenantID, false)
	s.invalidateTenantObjectCaches(tenantID)
}

func (s *TenantStore) invalidateTenantObjectCaches(tenantID string) {
	s.deleteWriteCache(tenantID)
	s.deleteCachedTenantMetadata(tenantID)
	s.deleteCachedTenantConfig(tenantID)
	s.deleteCachedSourcePolicy(tenantID)
	s.deleteCachedIndexCatalog(tenantID)
	s.deleteCachedTenantPurgeTombstone(tenantID)
	s.clearObjectKeyPrefix(s.tenantObjectPrefix(tenantID))
	if cache := FindWriterObjectCache(s.Objects); cache != nil {
		cache.ClearPrefix(s.tenantObjectPrefix(tenantID))
	}
	s.clearWriterObjectKey(s.tenantPurgeTombstoneKey(tenantID))
}
