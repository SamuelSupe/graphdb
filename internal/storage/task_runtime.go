package storage

import (
	"context"
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
	current := s.taskStateOrLocal(ctx, task)
	if current.Status == TaskStatusCanceled {
		return context.Canceled
	}
	if taskTerminal(current.Status) && current.Status != TaskStatusQueued && current.Status != TaskStatusRunning {
		return nil
	}
	if total <= 0 {
		total = current.ProgressTotal
	}
	if total <= 0 {
		total = taskProgressTotal(task.Type)
	}
	if completed < 0 {
		completed = 0
	}
	if completed > total {
		completed = total
	}
	current.Status = TaskStatusRunning
	current.Phase = phase
	current.ProgressCompleted = completed
	current.ProgressTotal = total
	current.UpdatedAt = time.Now().UTC()
	if checkpoint != nil {
		current.Checkpoint = mergeTaskMap(current.Checkpoint, checkpoint)
	}
	return s.saveTask(context.Background(), current)
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
