package storage

import "sync"

type tenantLock struct {
	mu   sync.Mutex
	refs int
}

func (s *TenantStore) lockTenant(tenantID string) func() {
	s.lockMu.Lock()
	lock := s.tenantLocks[tenantID]
	if lock == nil {
		lock = &tenantLock{}
		s.tenantLocks[tenantID] = lock
	}
	lock.refs++
	s.lockMu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.lockMu.Lock()
		defer s.lockMu.Unlock()
		lock.refs--
		if lock.refs == 0 && s.tenantLocks[tenantID] == lock {
			delete(s.tenantLocks, tenantID)
		}
	}
}
