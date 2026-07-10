package storage

import "time"

const (
	collectorStatusCacheLimit = 4096
	collectorStatusCacheTTL   = 15 * time.Minute
)

type cachedCollectorStatus struct {
	status     CollectorStatus
	meta       ObjectMeta
	lastAccess time.Time
}

func (s *TenantStore) getCachedCollectorStatus(key string) (CollectorStatus, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.collectorStatusCache[key]
	if !ok {
		return CollectorStatus{}, ObjectMeta{}, false
	}
	now := time.Now()
	if now.Sub(cached.lastAccess) > collectorStatusCacheTTL {
		delete(s.collectorStatusCache, key)
		return CollectorStatus{}, ObjectMeta{}, false
	}
	cached.lastAccess = now
	s.collectorStatusCache[key] = cached
	return cached.status, cached.meta, true
}

func (s *TenantStore) setCachedCollectorStatus(key string, status CollectorStatus, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	now := time.Now()
	if _, exists := s.collectorStatusCache[key]; !exists && len(s.collectorStatusCache) >= collectorStatusCacheLimit {
		for candidateKey := range s.collectorStatusCache {
			delete(s.collectorStatusCache, candidateKey)
			break
		}
	}
	s.collectorStatusCache[key] = cachedCollectorStatus{status: status, meta: meta, lastAccess: now}
}

func (s *TenantStore) deleteCachedCollectorStatus(key string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.collectorStatusCache, key)
}
