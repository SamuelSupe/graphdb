package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func (s *TenantStore) startCoordinatorTaskLease(
	task Task,
	cancel context.CancelFunc,
) (func(), error) {
	return s.startCoordinatorLease(
		context.Background(),
		task.TenantID,
		task.Type,
		s.InstanceID+"/"+task.ID,
		cancel,
	)
}

func (s *TenantStore) startCoordinatorOperationLease(
	ctx context.Context,
	tenantID string,
	taskType string,
) (context.Context, func(), error) {
	if !s.coordinated() {
		return ctx, func() {}, nil
	}
	operationID, err := newCommitID()
	if err != nil {
		return nil, nil, err
	}
	operationCtx, cancel := context.WithCancel(ctx)
	stop, err := s.startCoordinatorLease(
		operationCtx,
		tenantID,
		taskType,
		s.InstanceID+"/"+operationID,
		cancel,
	)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return operationCtx, func() {
		stop()
		cancel()
	}, nil
}

func (s *TenantStore) startCoordinatorLease(
	ctx context.Context,
	tenantID string,
	taskType string,
	owner string,
	cancel context.CancelFunc,
) (func(), error) {
	if !s.coordinated() {
		return func() {}, nil
	}
	ttl := s.leaseTTL()
	lease, acquired, err := s.Coordinator.AcquireTaskLease(
		ctx, tenantID, taskType, owner, ttl,
	)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, fmt.Errorf("%w: tenant %q task type %q", ErrTaskLeaseHeld, tenantID, taskType)
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(max(ttl/3, time.Second))
		defer ticker.Stop()
		current := lease
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), min(ttl/2, 5*time.Second))
				next, ok, renewErr := s.Coordinator.RenewTaskLease(renewCtx, current, ttl)
				renewCancel()
				if renewErr != nil || !ok {
					cancel()
					return
				}
				current = next
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer releaseCancel()
			_ = s.Coordinator.ReleaseTaskLease(releaseCtx, lease)
		})
	}, nil
}
