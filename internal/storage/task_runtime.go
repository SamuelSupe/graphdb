package storage

import (
	"context"
	"errors"
	"time"
)

const taskCancelPollInterval = 500 * time.Millisecond

func taskProgressTotal(taskType string) int {
	switch taskType {
	case TaskTypeCompact:
		return 4
	case TaskTypeGC:
		return 2
	case TaskTypeRepair:
		return 6
	case TaskTypeExportSnapshot:
		return 3
	case TaskTypeReplayDeadLetter:
		return 1
	case TaskTypeIndexRebuild:
		return 2
	case TaskTypeTenantBackup:
		return 5
	case TaskTypeTenantRestore:
		return 7
	case TaskTypeTenantRestoreDrill:
		return 7
	default:
		return 1
	}
}

func (s *TenantStore) updateTaskProgress(ctx context.Context, task Task, phase string, completed int, total int, checkpoint map[string]any) error {
	if _, err := s.prepareTenantWrite(ctx, task.TenantID); err != nil {
		return err
	}
	writeCtx := context.WithoutCancel(ctx)
	update := func(current *Task) error {
		if taskTerminal(current.Status) {
			return context.Canceled
		}
		progressTotal := total
		if progressTotal <= 0 {
			progressTotal = current.ProgressTotal
		}
		if progressTotal <= 0 {
			progressTotal = taskProgressTotal(task.Type)
		}
		progressCompleted := completed
		if progressCompleted < 0 {
			progressCompleted = 0
		}
		if progressCompleted > progressTotal {
			progressCompleted = progressTotal
		}
		current.Status = TaskStatusRunning
		current.Phase = phase
		current.ProgressCompleted = progressCompleted
		current.ProgressTotal = progressTotal
		current.UpdatedAt = time.Now().UTC()
		if checkpoint != nil {
			current.Checkpoint = mergeTaskMap(current.Checkpoint, checkpoint)
		}
		return nil
	}
	_, err := s.mutateTask(writeCtx, task.TenantID, task.ID, update)
	if errors.Is(err, ErrNotFound) {
		if err := s.saveTask(writeCtx, task); err != nil {
			return err
		}
		_, err = s.mutateTask(writeCtx, task.TenantID, task.ID, update)
	}
	return err
}

func (s *TenantStore) taskStateOrLocal(ctx context.Context, task Task) Task {
	current, err := s.GetTask(ctx, task.TenantID, task.ID)
	if err != nil {
		return task
	}
	if current.ProgressTotal == 0 {
		current.ProgressTotal = task.ProgressTotal
	}
	if current.OwnerID == "" {
		current.OwnerID = task.OwnerID
	}
	return current
}

func (s *TenantStore) taskCancelRequested(ctx context.Context, task Task) bool {
	current, err := s.GetTask(ctx, task.TenantID, task.ID)
	return err == nil && current.Status == TaskStatusCanceled
}

func (s *TenantStore) watchTaskCancellation(task Task, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(taskCancelPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if s.taskCancelRequested(context.Background(), task) {
					cancel()
					return
				}
			}
		}
	}()
	return func() { close(done) }
}

func mergeTaskMap(base map[string]any, update map[string]any) map[string]any {
	if len(base) == 0 && len(update) == 0 {
		return nil
	}
	out := make(map[string]any, len(base)+len(update))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range update {
		out[key] = value
	}
	return out
}

func verificationStatus(report *IntegrityAuditReport) string {
	if report == nil {
		return ""
	}
	return report.Status
}
