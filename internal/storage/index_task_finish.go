package storage

import "context"

func (s *TenantStore) finishIndexRebuildTask(task IndexTask) {
	data, err := marshalParquetIndexTask(context.Background(), task)
	if err != nil {
		return
	}
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if !s.putIndexTaskBytesWithRetry(s.indexRebuildRunningTaskKey(task.TenantID), data) {
		return
	}
	if !s.putIndexTaskBytesWithRetry(s.indexTaskKey(task.TenantID, task.ID), data) {
		return
	}
	_ = s.clearIndexRebuildRunningMarker(context.Background(), task.TenantID, task.ID)
	if current, ok := s.indexTasks[task.TenantID]; ok && current.ID == task.ID {
		delete(s.indexTasks, task.TenantID)
	}
}

func (s *TenantStore) putIndexTaskBytesWithRetry(key string, data []byte) bool {
	ctx := context.Background()
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		if err := s.Objects.Put(ctx, key, data); err == nil {
			return true
		}
		if attempt+1 < s.retryCount() {
			_ = retryDelay(ctx, attempt)
		}
	}
	return false
}
