package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (s *TenantStore) startDerivedTaskLease(
	ctx context.Context,
	job DerivedTaskJob,
) (context.Context, func() error) {
	taskCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	result := make(chan error, 1)
	var once sync.Once
	ttl := s.leaseTTL()
	go func() {
		ticker := time.NewTicker(max(ttl/3, 10*time.Millisecond))
		defer ticker.Stop()
		for {
			select {
			case <-done:
				result <- nil
				return
			case <-taskCtx.Done():
				result <- taskCtx.Err()
				return
			case <-ticker.C:
				timeout := max(min(ttl/2, 5*time.Second), 10*time.Millisecond)
				renewCtx, renewCancel := context.WithTimeout(context.Background(), timeout)
				ok, err := s.Coordinator.RenewDerivedTask(renewCtx, job, ttl)
				renewCancel()
				if err != nil {
					cancel()
					result <- err
					return
				}
				if !ok {
					cancel()
					result <- fmt.Errorf(
						"%w: derived task %s/%s lease was lost",
						ErrConflict, job.TenantID, job.TaskType,
					)
					return
				}
			}
		}
	}()
	return taskCtx, func() error {
		once.Do(func() { close(done) })
		err := <-result
		cancel()
		return err
	}
}
