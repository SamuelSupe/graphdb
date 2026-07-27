package storage

type registeredTenantCacheEntry struct {
	generation int64
	status     string
}

func (s *TenantStore) isRegisteredTenantCached(tenantID string) bool {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	_, ok := s.registeredTenantCache[tenantID]
	return ok
}

func (s *TenantStore) isRegisteredTenantGenerationCached(
	tenantID string,
	generation int64,
	status string,
) bool {
	if generation <= 0 {
		return false
	}
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	entry, ok := s.registeredTenantCache[tenantID]
	return ok && entry.generation == generation && entry.status == status
}

func (s *TenantStore) setRegisteredTenantCached(tenantID string) {
	s.setRegisteredTenantGenerationCached(tenantID, 0, "")
}

func (s *TenantStore) setRegisteredTenantGenerationCached(
	tenantID string,
	generation int64,
	status string,
) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.registeredTenantCache[tenantID] = registeredTenantCacheEntry{
		generation: generation,
		status:     status,
	}
}

func (s *TenantStore) deleteRegisteredTenantCached(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.registeredTenantCache, tenantID)
}

func (s *TenantStore) clearRegisteredTenantCache() {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.registeredTenantCache = map[string]registeredTenantCacheEntry{}
}

func (s *TenantStore) replaceRegisteredTenantCache(tenantIDs []string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.registeredTenantCache = make(
		map[string]registeredTenantCacheEntry,
		len(tenantIDs),
	)
	for _, tenantID := range tenantIDs {
		s.registeredTenantCache[tenantID] = registeredTenantCacheEntry{}
	}
}
