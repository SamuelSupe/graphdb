package storage

import (
	"context"
	"time"
)

type IndexTask struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	Type              string    `json:"type"`
	Status            string    `json:"status"`
	Phase             string    `json:"phase,omitempty"`
	ProgressCompleted int       `json:"progress_completed,omitempty"`
	ProgressTotal     int       `json:"progress_total,omitempty"`
	OwnerID           string    `json:"owner_id,omitempty"`
	CatalogVersion    int64     `json:"catalog_version,omitempty"`
	Error             string    `json:"error,omitempty"`
	StartedAt         time.Time `json:"started_at"`
	UpdatedAt         time.Time `json:"updated_at,omitempty"`
	FinishedAt        time.Time `json:"finished_at,omitempty"`
}

func (s *TenantStore) StartIndexRebuild(ctx context.Context, tenantID string) (IndexTask, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexTask{}, err
	}
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return IndexTask{}, err
	}
	defer unlock()
	return s.startIndexRebuild(ctx, tenantID, true)
}

func (s *TenantStore) startIndexRebuildAfterDefinitionChangeLocked(ctx context.Context, tenantID string) (IndexTask, error) {
	return s.startIndexRebuild(ctx, tenantID, false)
}

func (s *TenantStore) startIndexRebuild(ctx context.Context, tenantID string, reuse bool) (IndexTask, error) {
	if reuse {
		if task, ok, err := s.findRunningTask(ctx, tenantID, TaskTypeIndexRebuild); err != nil {
			return IndexTask{}, err
		} else if ok {
			return indexTaskFromTask(task), nil
		}

	}
	task, err := s.startTaskLocked(ctx, tenantID, TaskTypeIndexRebuild, nil, !reuse)
	return indexTaskFromTask(task), err
}

func indexTaskFromTask(task Task) IndexTask {
	status := task.Status
	if status == TaskStatusQueued {
		status = TaskStatusRunning
	}
	version := taskCheckpointNumber(task.Result["version"])
	if version == 0 {
		version = taskCheckpointNumber(task.Checkpoint["version"])
	}
	errorText := task.Error
	if errorText == "" {
		errorText, _ = task.Result["cleanup_warning"].(string)
	}
	return IndexTask{ID: task.ID, TenantID: task.TenantID, Type: "rebuild", Status: status,
		Phase: task.Phase, ProgressCompleted: task.ProgressCompleted, ProgressTotal: task.ProgressTotal,
		OwnerID: task.OwnerID, CatalogVersion: version,
		Error: errorText, StartedAt: task.StartedAt, UpdatedAt: task.UpdatedAt, FinishedAt: task.FinishedAt}
}

func (s *TenantStore) finishIndexTaskCleanup(ctx context.Context, task Task, catalog IndexCatalog) (map[string]any, string, error) {
	if err := s.updateTaskProgress(ctx, task, "cleanup", 1, 2, map[string]any{"version": catalog.Version}); err != nil {
		return nil, "", err
	}
	// GC reacquires execution capacity per batch so compact can run between them.
	if admission, ok := ctx.Value(taskIngestAdmissionKey{}).(*taskExecutionAdmission); ok {
		admission.release()
	}
	report, err := s.RunGC(ctx, task.TenantID, GCOptions{KeepSnapshots: 2, CleanupIndexOrphans: true, SkipEntityRecordCleanup: true})
	result := taskResult(catalog)
	if ctx.Err() != nil {
		return nil, "", ctx.Err()
	}
	if err != nil {
		result["cleanup_warning"] = err.Error()
	} else if report.IndexCleanupError != "" {
		result["cleanup_warning"] = report.IndexCleanupError
	} else if report.IndexCleanupSkippedReason != "" {
		result["cleanup_warning"] = report.IndexCleanupSkippedReason
	}
	return result, "", nil
}
