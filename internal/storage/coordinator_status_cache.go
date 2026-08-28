package storage

import (
	"context"
	"time"
)

type coordinatorStatusCall struct {
	done     chan struct{}
	status   CoordinatorStatus
	canceled bool
}

func (s *TenantStore) CoordinatorStatus(ctx context.Context) CoordinatorStatus {
	if !s.coordinated() {
		return localCoordinatorStatus()
	}
	for {
		s.coordinatorStatusMu.Lock()
		if active := s.coordinatorStatusActive; active != nil {
			s.coordinatorStatusMu.Unlock()
			select {
			case <-active.done:
				if active.canceled && ctx.Err() == nil {
					continue
				}
				return active.status
			case <-ctx.Done():
				return unavailableCoordinatorStatus(
					s.Coordinator, ctx.Err(),
				)
			}
		}
		active := &coordinatorStatusCall{done: make(chan struct{})}
		s.coordinatorStatusActive = active
		s.coordinatorStatusMu.Unlock()

		status, err := s.Coordinator.Status(ctx)
		if err != nil {
			status = unavailableCoordinatorStatus(s.Coordinator, err)
		}
		active.status = status
		active.canceled = loadCanceledByContext(ctx, err)

		s.coordinatorStatusMu.Lock()
		if !active.canceled {
			s.coordinatorStatusCache = status
		}
		if s.coordinatorStatusActive == active {
			s.coordinatorStatusActive = nil
		}
		close(active.done)
		s.coordinatorStatusMu.Unlock()
		return status
	}
}

func unavailableCoordinatorStatus(
	coordinator CoordinatorDescriptor,
	err error,
) CoordinatorStatus {
	status := CoordinatorStatus{
		Backend:   CoordinationPostgres,
		Available: false,
		CheckedAt: time.Now().UTC(),
	}
	if coordinator != nil {
		status.Backend = coordinator.Backend()
		status.Namespace = coordinator.Namespace()
	}
	if err != nil {
		status.LastError = err.Error()
	}
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
