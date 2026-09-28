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
			s.rememberTaskState(current)
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

func (s *TenantStore) rememberTaskState(task Task) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	key := taskActiveKey(task.TenantID, task.Type)
	if active, ok := s.taskActive[key]; ok && active.ID == task.ID {
		// Durable updates can reach this lock out of order. Cancellation and
		// newer progress must not be replaced by an earlier save.
		if taskTerminal(active.Status) || active.UpdatedAt.After(task.UpdatedAt) {
			return
		}
		s.taskActive[key] = task
	}
}
