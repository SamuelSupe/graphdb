package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const coordinatorLifecycleTaskType = "tenant_lifecycle"

type coordinatorLeaseContextKey struct{}

type coordinatorLeaseContext struct {
	tenantID string
	taskType string
}

func (s *TenantStore) startCoordinatorTaskLease(
	ctx context.Context,
	task Task,
	cancel context.CancelFunc,
) (context.Context, func(), error) {
	taskType := coordinatorTaskLeaseType(task)
	if coordinatorLeaseContextMatches(
		ctx,
		task.TenantID,
		taskType,
	) {
		return ctx, func() {}, nil
	}
	stop, err := s.startCoordinatorLease(
		ctx,
		task.TenantID,
		taskType,
		s.InstanceID+"/"+task.ID,
		cancel,
	)
	if err != nil {
		return nil, nil, err
	}
	return bindCoordinatorLeaseContext(ctx, task.TenantID, taskType), stop, nil
}

func (s *TenantStore) startCoordinatorOperationLease(
	ctx context.Context,
	tenantID string,
	taskType string,
) (context.Context, func(), error) {
	if !s.coordinated() {
		return ctx, func() {}, nil
	}
	taskType = coordinatorLeaseTaskType(taskType)
	if coordinatorLeaseContextMatches(ctx, tenantID, taskType) {
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
	operationCtx = bindCoordinatorLeaseContext(operationCtx, tenantID, taskType)
	return operationCtx, func() {
		stop()
		cancel()
	}, nil
}

func coordinatorTaskLeaseType(task Task) string {
	if task.Type == TaskTypeRepair &&
		!boolTaskParam(task.Params, "apply") {
		return TaskTypeRepair
	}
	return coordinatorLeaseTaskType(task.Type)
}

func coordinatorLeaseTaskType(taskType string) string {
	switch taskType {
	case "create", "clone", "migration", "purge", "status",
		TaskTypeCompact, TaskTypeGC, TaskTypeRepair, TaskTypeIndexRebuild,
		TaskTypeTenantRestore:
		return coordinatorLifecycleTaskType
	default:
		return taskType
	}
}

func bindCoordinatorLeaseContext(
	ctx context.Context,
	tenantID string,
	taskType string,
) context.Context {
	return context.WithValue(ctx, coordinatorLeaseContextKey{}, coordinatorLeaseContext{
		tenantID: tenantID,
		taskType: taskType,
	})
}

func coordinatorLeaseContextMatches(
	ctx context.Context,
	tenantID string,
	taskType string,
) bool {
	current, ok := ctx.Value(coordinatorLeaseContextKey{}).(coordinatorLeaseContext)
	return ok && current.tenantID == tenantID && current.taskType == taskType
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
		ticker := time.NewTicker(max(ttl/3, 10*time.Millisecond))
		defer ticker.Stop()
		current := lease
		for {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				renewTimeout := max(min(ttl/2, 5*time.Second), 10*time.Millisecond)
				renewCtx, renewCancel := context.WithTimeout(
					context.Background(), renewTimeout,
				)
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
