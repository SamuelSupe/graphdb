package storage

import (
	"context"
	"errors"
	"fmt"
)

func (s *TenantStore) mutateTask(ctx context.Context, tenantID string, taskID string, mutate func(*Task) error) (Task, error) {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		current, meta, err := s.getTaskObjectWithMeta(ctx, tenantID, taskID)
		if err != nil {
			return Task{}, err
		}
		if err := mutate(&current); err != nil {
			return current, err
		}
		data, err := marshalParquetTask(ctx, current)
		if err != nil {
			return Task{}, err
		}
		if _, err := s.putTenantGenerationConditional(ctx, tenantID, s.taskKey(tenantID, taskID), data, PutCondition{IfMatch: meta.ETag}); err == nil {
			if err := s.syncGCRunningMarker(ctx, current); err != nil {
				return Task{}, err
			}
			return current, nil
		} else if !errors.Is(err, ErrConflict) {
			return Task{}, err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return Task{}, err
		}
	}
	return Task{}, fmt.Errorf("%w: task %q changed while updating", ErrConflict, taskID)
}
