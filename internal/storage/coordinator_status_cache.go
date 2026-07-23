package storage

import (
	"context"
	"time"
)

func (s *TenantStore) CoordinatorStatus(ctx context.Context) CoordinatorStatus {
	if !s.coordinated() {
		return localCoordinatorStatus()
	}
	status, err := s.Coordinator.Status(ctx)
	if err != nil {
		status.Backend = CoordinationPostgres
		status.Available = false
		status.Namespace = s.Coordinator.Namespace()
		status.CheckedAt = time.Now().UTC()
		status.LastError = err.Error()
	}
	s.cacheCoordinatorStatus(status)
	return status
}

func (s *TenantStore) CachedCoordinatorStatus() CoordinatorStatus {
	if !s.coordinated() {
		return localCoordinatorStatus()
	}
	s.coordinatorStatusMu.RLock()
	status := s.coordinatorStatusCache
	s.coordinatorStatusMu.RUnlock()
	if status.CheckedAt.IsZero() {
		status = CoordinatorStatus{
			Backend:   CoordinationPostgres,
			Available: false,
			Namespace: s.Coordinator.Namespace(),
			CheckedAt: time.Now().UTC(),
			LastError: "coordinator status has not been sampled",
		}
	}
	return status
}

func (s *TenantStore) StartCoordinatorStatusMonitor(
	ctx context.Context,
	interval time.Duration,
	timeout time.Duration,
) {
	if !s.coordinated() {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	refresh := func() {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_ = s.CoordinatorStatus(probeCtx)
	}
	refresh()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

func (s *TenantStore) cacheCoordinatorStatus(status CoordinatorStatus) {
	if s == nil {
		return
	}
	s.coordinatorStatusMu.Lock()
	s.coordinatorStatusCache = status
	s.coordinatorStatusMu.Unlock()
}

func localCoordinatorStatus() CoordinatorStatus {
	return CoordinatorStatus{
		Backend:       CoordinationLocal,
		Available:     true,
		SchemaVersion: 0,
		CheckedAt:     time.Now().UTC(),
	}
}
