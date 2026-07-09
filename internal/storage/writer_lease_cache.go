package storage

import "time"

type cachedWriterLease struct {
	lease WriterLease
	meta  ObjectMeta
}

func (s *TenantStore) getCachedWriterLease(tenantID string, now time.Time) (WriterLease, ObjectMeta, bool) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.writerLeaseCache[tenantID]
	if !ok {
		return WriterLease{}, ObjectMeta{}, false
	}
	if !s.cachedWriterLeaseUsable(cached.lease, now) {
		return WriterLease{}, ObjectMeta{}, false
	}
	return cached.lease, cached.meta, true
}

func (s *TenantStore) setCachedWriterLease(tenantID string, lease WriterLease, meta ObjectMeta) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.writerLeaseCache[tenantID] = cachedWriterLease{lease: lease, meta: meta}
}

func (s *TenantStore) deleteCachedWriterLease(tenantID string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.writerLeaseCache, tenantID)
}

func (s *TenantStore) cachedWriterLeaseUsable(lease WriterLease, now time.Time) bool {
	if lease.OwnerID != s.InstanceID {
		return false
	}
	refreshBefore := s.leaseTTL() / 3
	if refreshBefore > 5*time.Second {
		refreshBefore = 5 * time.Second
	}
	if refreshBefore < 0 {
		refreshBefore = 0
	}
	return lease.ExpiresAt.After(now.Add(refreshBefore))
}
