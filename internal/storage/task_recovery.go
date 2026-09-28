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
	if !s.taskOwnerStopped(task) {
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

func (s *TenantStore) taskOwnerStopped(task Task) bool {
	s.taskMu.Lock()
	_, running := s.taskCancels[taskRuntimeKey(task.TenantID, task.ID)]
	pending := s.taskActive[taskActiveKey(task.TenantID, task.Type)].ID == task.ID
	s.taskMu.Unlock()
	return taskStillActive(task) && task.OwnerID != "" && !running && !pending
}

func (s *TenantStore) taskRuntimeActive(tenantID string, taskID string) bool {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	_, ok := s.taskCancels[taskRuntimeKey(tenantID, taskID)]
	return ok
}
