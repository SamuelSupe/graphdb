package storage

import "context"

func (s *TenantStore) planExpiredTaskGroup(
	ctx context.Context,
	tenantID string,
	task Task,
	taskKey string,
	checkpoint *gcCheckpointRunner,
) error {
	keys := make([]string, 0, 3)
	if task.Type == TaskTypeBulkImport {
		sourceKey := stringTaskParam(task.Params, "source_key")
		if sourceKey != "" {
			if err := s.validateImportSourceKey(tenantID, sourceKey); err != nil {
				return err
			}
			keys = append(keys, sourceKey)
		}
	}
	if task.ResultKey != "" {
		if err := s.validateTenantObjectKey(tenantID, task.ResultKey); err != nil {
			return err
		}
		keys = append(keys, task.ResultKey)
	}
	keys = append(keys, taskKey)
	for _, key := range keys {
		if err := checkpoint.planExistingKeyIgnoringLimit(
			ctx, s.Objects, key,
		); err != nil {
			return err
		}
	}
	// A task group contains at most two attachments. Planning it atomically
	// keeps dry-run cursors monotonic while bounding any budget overshoot.
	if checkpoint.limitReached() {
		checkpoint.pauseAt(taskKey)
	}
	return nil
}
