package storage

import "context"

func (s *TenantStore) findRunningGCTask(ctx context.Context, tenantID string) (Task, bool, error) {
	s.taskMu.Lock()
	task, ok := s.taskActive[taskActiveKey(tenantID, TaskTypeGC)]
	s.taskMu.Unlock()
	return task, ok && taskStillActive(task), nil
}
