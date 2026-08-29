package storage

import "context"

func (s *TenantStore) finishIndexRebuildTask(ctx context.Context, task IndexTask) {
	ctx, cancel := s.taskFinalizationContext(ctx)
	defer cancel()
	data, err := marshalParquetIndexTask(ctx, task)
	if err != nil {
		return
	}
	startSlot := s.indexTaskStartSlot(task.TenantID)
	if !acquireTaskSlot(ctx, startSlot) {
		return
	}
	defer releaseTaskSlot(startSlot)

	s.taskMu.Lock()
	current, exists := s.indexTasks[task.TenantID]
	isCurrent := exists && current.ID == task.ID
	s.taskMu.Unlock()
	if !isCurrent {
		_ = s.putIndexTaskBytesWithRetry(
			ctx,
			task.TenantID,
			s.indexTaskKey(task.TenantID, task.ID),
			data,
		)
		return
	}
	if !s.putIndexTaskBytesWithRetry(ctx, task.TenantID, s.indexRebuildRunningTaskKey(task.TenantID), data) {
		return
	}
	if !s.putIndexTaskBytesWithRetry(ctx, task.TenantID, s.indexTaskKey(task.TenantID, task.ID), data) {
		return
	}
	_ = s.clearIndexRebuildRunningMarker(ctx, task.TenantID, task.ID)
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if current, ok := s.indexTasks[task.TenantID]; ok && current.ID == task.ID {
		delete(s.indexTasks, task.TenantID)
	}
}

func (s *TenantStore) putIndexTaskBytesWithRetry(ctx context.Context, tenantID string, key string, data []byte) bool {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		if err := s.putTenantGenerationObject(ctx, tenantID, key, data); err == nil {
			return true
		}
		if attempt+1 < s.retryCount() {
			_ = retryDelay(ctx, attempt)
		}
	}
	return false
}
