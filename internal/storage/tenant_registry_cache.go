package storage

func (s *TenantStore) isRegisteredTenantCached(tenantID string) bool {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	_, ok := s.registeredTenantCache[tenantID]
	return ok
}

func (s *TenantStore) setRegisteredTenantCached(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.registeredTenantCache[tenantID] = struct{}{}
}

func (s *TenantStore) deleteRegisteredTenantCached(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.registeredTenantCache, tenantID)
}

func (s *TenantStore) replaceRegisteredTenantCache(tenantIDs []string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.registeredTenantCache = make(map[string]struct{}, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		s.registeredTenantCache[tenantID] = struct{}{}
	}
}
