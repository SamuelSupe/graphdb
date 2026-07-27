package storage

import (
	"context"
	"errors"
	"time"
)

var errTaskRecoveryStateChanged = errors.New("task state changed during recovery")

const inactiveTaskError = "task owner stopped before terminal state was persisted"

func (s *TenantStore) reconcileInactiveTask(
	ctx context.Context,
	task Task,
) Task {
	if !s.taskOwnerStopped(ctx, task, time.Now().UTC()) {
		return task
	}
	updated, err := s.mutateTask(
		ctx,
		task.TenantID,
		task.ID,
		func(current *Task) error {
			if current.Status != task.Status ||
				current.OwnerID != task.OwnerID ||
				!current.UpdatedAt.Equal(task.UpdatedAt) {
				return errTaskRecoveryStateChanged
			}
			now := time.Now().UTC()
			current.Status = TaskStatusFailed
			current.Phase = TaskStatusFailed
			current.Error = inactiveTaskError
			current.UpdatedAt = now
			current.FinishedAt = now
			return nil
		},
	)
	if err == nil || errors.Is(err, errTaskRecoveryStateChanged) {
		return updated
	}
	return task
}

func (s *TenantStore) taskOwnerStopped(
	ctx context.Context,
	task Task,
	now time.Time,
) bool {
	if !taskStillActive(task) ||
		task.OwnerID == "" ||
		!taskRecoveryDue(task, now, s.taskMarkerTTL()) {
		return false
	}
	active, known := s.taskOwnerActive(ctx, task, now)
	return known && !active
}

func taskRecoveryDue(task Task, now time.Time, grace time.Duration) bool {
	if grace <= 0 {
		grace = 30 * time.Second
	}
	updatedAt := task.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = task.StartedAt
	}
	return !updatedAt.IsZero() && !now.Before(updatedAt.Add(grace))
}

func (s *TenantStore) taskOwnerActive(
	ctx context.Context,
	task Task,
	now time.Time,
) (bool, bool) {
	if s.taskRuntimeActive(task.TenantID, task.ID) {
		return true, true
	}
	if task.OwnerID == s.InstanceID {
		return false, true
	}
	if s.coordinated() {
		return s.coordinatedTaskOwnerActive(ctx, task)
	}
	lease, err := s.GetWriterLease(ctx, task.TenantID)
	if errors.Is(err, ErrNotFound) {
		return false, true
	}
	if err != nil {
		return false, false
	}
	return lease.OwnerID == task.OwnerID && lease.ExpiresAt.After(now), true
}

func (s *TenantStore) coordinatedTaskOwnerActive(
	ctx context.Context,
	task Task,
) (bool, bool) {
	reader, ok := s.Coordinator.(CoordinatorTaskLeaseReader)
	if !ok {
		return false, false
	}
	expectedOwner := task.OwnerID + "/" + task.ID
	active, known := matchingCoordinatorTaskLease(
		ctx,
		reader,
		task.TenantID,
		coordinatorQueuedTaskLeaseType(task),
		expectedOwner,
	)
	if active || !known || task.Status != TaskStatusRunning {
		return active, known
	}
	return matchingCoordinatorTaskLease(
		ctx,
		reader,
		task.TenantID,
		coordinatorTaskLeaseType(task),
		expectedOwner,
	)
}

func matchingCoordinatorTaskLease(
	ctx context.Context,
	reader CoordinatorTaskLeaseReader,
	tenantID string,
	taskType string,
	expectedOwner string,
) (bool, bool) {
	lease, active, err := reader.TaskLease(ctx, tenantID, taskType)
	if err != nil {
		return false, false
	}
	return active && lease.OwnerToken == expectedOwner, true
}

func (s *TenantStore) taskRuntimeActive(tenantID string, taskID string) bool {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	_, ok := s.taskCancels[taskRuntimeKey(tenantID, taskID)]
	return ok
}
