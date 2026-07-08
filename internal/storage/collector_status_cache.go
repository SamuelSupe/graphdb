package storage

type cachedCollectorStatus struct {
	status CollectorStatus
	meta   ObjectMeta
}

func (s *TenantStore) getCachedCollectorStatus(key string) (CollectorStatus, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.collectorStatusCache[key]
	if !ok {
		return CollectorStatus{}, ObjectMeta{}, false
	}
	return cached.status, cached.meta, true
}

func (s *TenantStore) setCachedCollectorStatus(key string, status CollectorStatus, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.collectorStatusCache[key] = cachedCollectorStatus{status: status, meta: meta}
}

func (s *TenantStore) deleteCachedCollectorStatus(key string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.collectorStatusCache, key)
}
