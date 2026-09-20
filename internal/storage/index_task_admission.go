package storage

import (
	"context"
	"time"
)

const indexTaskStoppedBeforeExecution = "index task stopped before execution"

func (s *TenantStore) runIndexTaskAdmitted(
	ctx context.Context,
	tenantID string,
	task IndexTask,
) {
	defer s.releaseQueuedTask()
	admission := &taskExecutionAdmission{tenant: s.taskTenantSlot(tenantID), execution: s.taskExecutionSlots}
	if !admission.acquire(ctx) {
		s.failQueuedIndexTask(ctx, task)
		return
	}
	defer admission.release()
	ctx = context.WithValue(ctx, taskIngestAdmissionKey{}, admission)
	s.runIndexRebuildTaskWithRelease(ctx, tenantID, task, admission.release)
}

func (s *TenantStore) failQueuedIndexTask(
	ctx context.Context,
	task IndexTask,
) {
	writeCtx, cancel := s.taskFinalizationContext(ctx)
	defer cancel()
	if current, err := s.GetIndexTask(
		writeCtx,
		task.TenantID,
		task.ID,
	); err == nil {
		if !indexTaskStillActive(current) {
			return
		}
		task = current
	}
	now := time.Now().UTC()
	task.Status = TaskStatusFailed
	task.Phase = TaskStatusFailed
	task.Error = indexTaskStoppedBeforeExecution
	task.UpdatedAt = now
	task.FinishedAt = now
	s.finishIndexRebuildTask(writeCtx, task)
}
