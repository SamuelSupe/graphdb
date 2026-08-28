package storage

import (
	"context"
	"time"
)

func (s *TenantStore) StartCoordinatorCleanup(ctx context.Context) {
	config := s.CoordinatorCleanup
	if !s.coordinated() || config.Interval <= 0 ||
		(config.IdempotencyRetention <= 0 && config.OutboxRetention <= 0) {
		return
	}
	go func() {
		_, _ = s.CleanupCoordinator(ctx)
		ticker := time.NewTicker(config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = s.CleanupCoordinator(ctx)
			}
		}
	}()
}

func (s *TenantStore) CleanupCoordinator(ctx context.Context) (CoordinatorCleanupReport, error) {
	if !s.coordinated() {
		return CoordinatorCleanupReport{}, nil
	}
	report, err := s.Coordinator.Cleanup(ctx, s.CoordinatorCleanup)
	if s.coordinatorObserver != nil {
		status := "ok"
		if err != nil {
			status = "error"
		}
		s.coordinatorObserver.RecordCoordinatorCleanup(
			status,
			report.IdempotencyDeleted,
			report.OutboxDeleted,
		)
	}
	return report, err
}
