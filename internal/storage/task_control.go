package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	TaskStatusQueued    = "queued"
	TaskStatusRunning   = "running"
	TaskStatusCanceled  = "canceled"
	TaskStatusFailed    = "failed"
	TaskStatusSucceeded = "succeeded"
)

func (s *TenantStore) CancelTask(ctx context.Context, tenantID string, taskID string) (Task, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err != nil {
		return Task{}, err
	}
	if task.Result != nil {
		if _, legacy := task.Result["legacy_index_task"]; legacy {
			return Task{}, fmt.Errorf("legacy index task cancellation is not supported")
		}
	}
	task, err = s.mutateTask(ctx, tenantID, taskID, func(current *Task) error {
		if taskTerminal(current.Status) {
			return nil
		}
		now := time.Now().UTC()
		current.Status = TaskStatusCanceled
		current.Phase = TaskStatusCanceled
		current.Error = TaskStatusCanceled
		current.UpdatedAt = now
		current.FinishedAt = now
		return nil
	})
	if err != nil {
		return Task{}, err
	}
	s.cancelTaskRuntime(tenantID, taskID)
	return task, nil
}

func (s *TenantStore) RetryTask(ctx context.Context, tenantID string, taskID string) (Task, error) {
	task, err := s.GetTask(ctx, tenantID, taskID)
	if err != nil {
		return Task{}, err
	}
	if task.Status == TaskStatusQueued || task.Status == TaskStatusRunning {
		return Task{}, fmt.Errorf("task %q is still %s", taskID, task.Status)
	}
	if task.Status != TaskStatusFailed && task.Status != TaskStatusCanceled {
		return Task{}, fmt.Errorf("only failed or canceled tasks can be retried")
	}
	params := retryTaskParams(task)
	params["retry_of"] = task.ID
	return s.StartTask(ctx, tenantID, task.Type, params)
}

func retryTaskParams(task Task) map[string]any {
	params := cloneTaskParams(task.Params)
	if params == nil {
		params = map[string]any{}
	}
	if len(task.Checkpoint) > 0 {
		params[taskResumeCheckpointParam] = cloneTaskParams(task.Checkpoint)
	}
	if task.Type != TaskTypeGC && task.Type != TaskTypeReplayDeadLetter {
		return params
	}
	checkpoint := task.Checkpoint
	if len(checkpoint) == 0 {
		if resultCheckpoint, ok := task.Result["checkpoint"].(map[string]any); ok {
			checkpoint = resultCheckpoint
		}
	}
	if len(checkpoint) == 0 {
		return params
	}
	if task.Type == TaskTypeReplayDeadLetter {
		nextCursor, ok := checkpoint["next_cursor"].(string)
		if ok && nextCursor != "" {
			params["cursor"] = nextCursor
		}
		return params
	}
	nextCursor, ok := checkpoint["next_cursor"].(string)
	if ok && nextCursor != "" {
		params["cursor"] = nextCursor
	}
	return params
}

func taskTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case TaskStatusCanceled, TaskStatusFailed, TaskStatusSucceeded:
		return true
	default:
		return false
	}
}

func taskRuntimeKey(tenantID string, taskID string) string {
	return tenantID + "\x00" + taskID
}

func (s *TenantStore) registerTaskCancel(tenantID string, taskID string, cancel context.CancelFunc) {
	if cancel == nil {
		return
	}
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if s.taskCancels == nil {
		s.taskCancels = map[string]context.CancelFunc{}
	}
	s.taskCancels[taskRuntimeKey(tenantID, taskID)] = cancel
}

func (s *TenantStore) unregisterTaskCancel(tenantID string, taskID string) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	delete(s.taskCancels, taskRuntimeKey(tenantID, taskID))
}

func (s *TenantStore) cancelTaskRuntime(tenantID string, taskID string) {
	s.taskMu.Lock()
	cancel := s.taskCancels[taskRuntimeKey(tenantID, taskID)]
	s.taskMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func taskCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}
