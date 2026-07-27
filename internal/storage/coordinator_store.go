package storage

import "time"

func (s *TenantStore) SetCoordinator(coordinator WriteCoordinator) {
	s.Coordinator = coordinator
	s.clearRegisteredTenantCache()
	if coordinator != nil {
		s.cacheCoordinatorStatus(CoordinatorStatus{
			Backend:   coordinator.Backend(),
			Available: false,
			Namespace: coordinator.Namespace(),
			CheckedAt: time.Now().UTC(),
			LastError: "coordinator status has not been sampled",
		})
	}
	s.deleteAllWriteCaches()
}

func (s *TenantStore) CoordinationBackend() string {
	if s == nil || s.Coordinator == nil {
		return CoordinationLocal
	}
	return s.Coordinator.Backend()
}

func (s *TenantStore) coordinated() bool {
	return s != nil && s.Coordinator != nil && s.Coordinator.Backend() == CoordinationPostgres
}

func (s *TenantStore) coordinatorPendingReservationTTL() time.Duration {
	if s != nil && s.CoordinatorPendingTTL > 0 {
		return s.CoordinatorPendingTTL
	}
	return coordinatorPendingReservationTTL
}

func (s *TenantStore) deleteAllWriteCaches() {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	s.writeCache = map[string]loadedGraph{}
	s.writeCacheOrder = nil
	s.writeCacheBytes = 0
}
