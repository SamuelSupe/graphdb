package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type legacyManifestLeaseRenewer interface {
	RenewLegacyManifest(
		context.Context,
		LegacyManifestJob,
		time.Duration,
	) (bool, error)
}

func (s *TenantStore) startLegacyManifestLease(
	ctx context.Context,
	job LegacyManifestJob,
	ttl time.Duration,
) (context.Context, func() error) {
	renewer, ok := s.Coordinator.(legacyManifestLeaseRenewer)
	if !ok {
		return ctx, func() error { return nil }
	}
	jobCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	go func() {
		interval := max(ttl/3, 10*time.Millisecond)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				result <- nil
				return
			case <-jobCtx.Done():
				result <- jobCtx.Err()
				return
			case <-ticker.C:
				timeout := max(min(ttl/2, 5*time.Second), 10*time.Millisecond)
				renewCtx, renewCancel := context.WithTimeout(
					context.Background(), timeout,
				)
				renewed, err := renewer.RenewLegacyManifest(
					renewCtx, job, ttl,
				)
				renewCancel()
				if err != nil {
					cancel()
					result <- err
					return
				}
				if !renewed {
					cancel()
					result <- fmt.Errorf(
						"%w: legacy manifest %s/%d lease was lost",
						ErrConflict, job.TenantID, job.HeadRevision,
					)
					return
				}
			}
		}
	}()
	return jobCtx, func() error {
		once.Do(func() { close(done) })
		err := <-result
		cancel()
		return err
	}
}
