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
	tenantSlot := s.taskTenantSlot(tenantID)
	if !acquireTaskSlot(ctx, tenantSlot) {
		s.failQueuedIndexTask(ctx, task)
		return
	}
	if !acquireTaskSlot(ctx, s.taskExecutionSlots) {
		releaseTaskSlot(tenantSlot)
		s.failQueuedIndexTask(ctx, task)
		return
	}
	released := false
	releaseExecution := func() {
		if released {
			return
		}
		releaseTaskSlot(s.taskExecutionSlots)
		releaseTaskSlot(tenantSlot)
		released = true
	}
	defer releaseExecution()
	s.runIndexRebuildTaskWithRelease(ctx, tenantID, task, releaseExecution)
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
