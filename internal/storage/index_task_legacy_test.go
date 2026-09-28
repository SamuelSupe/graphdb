package storage

import "context"

// Seed records from the separate index executor used by earlier 2.0 builds.
func (s *TenantStore) saveIndexTask(ctx context.Context, task IndexTask) error {
	data, err := marshalParquetIndexTask(ctx, task)
	if err != nil {
		return err
	}
	if err := s.putTenantGenerationObject(ctx, task.TenantID, s.indexTaskKey(task.TenantID, task.ID), data); err != nil {
		return err
	}
	if task.Type == "rebuild" && indexTaskStillActive(task) {
		return s.putTenantGenerationObject(ctx, task.TenantID, s.indexRebuildRunningTaskKey(task.TenantID), data)
	}
	return nil
}
