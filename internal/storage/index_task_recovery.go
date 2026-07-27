package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *TenantStore) reconcileInactiveIndexTask(
	ctx context.Context,
	task IndexTask,
) IndexTask {
	if !indexTaskStillActive(task) || task.OwnerID == "" {
		return task
	}
	active, err := s.indexTaskActive(
		ctx,
		task.TenantID,
		task,
		time.Now().UTC(),
	)
	if err != nil || active {
		return task
	}
	updated, err := s.mutateIndexTask(
		ctx,
		task.TenantID,
		task.ID,
		func(current *IndexTask) error {
			if current.Status != task.Status ||
				current.OwnerID != task.OwnerID ||
				!current.UpdatedAt.Equal(task.UpdatedAt) {
				return errTaskRecoveryStateChanged
			}
			now := time.Now().UTC()
			current.Status = TaskStatusFailed
			current.Phase = TaskStatusFailed
			current.Error = inactiveTaskError
			current.UpdatedAt = now
			current.FinishedAt = now
			return nil
		},
	)
	if err == nil || errors.Is(err, errTaskRecoveryStateChanged) {
		return updated
	}
	return task
}

func (s *TenantStore) mutateIndexTask(
	ctx context.Context,
	tenantID string,
	taskID string,
	mutate func(*IndexTask) error,
) (IndexTask, error) {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		current, meta, err := s.getIndexTaskObjectWithMeta(
			ctx,
			tenantID,
			taskID,
		)
		if err != nil {
			return IndexTask{}, err
		}
		if err := mutate(&current); err != nil {
			return current, err
		}
		data, err := marshalParquetIndexTask(ctx, current)
		if err != nil {
			return IndexTask{}, err
		}
		_, err = s.putTenantGenerationConditional(
			ctx,
			tenantID,
			s.indexTaskKey(tenantID, taskID),
			data,
			PutCondition{IfMatch: meta.ETag},
		)
		if err == nil {
			if current.Type == "rebuild" &&
				!indexTaskStillActive(current) {
				_ = s.clearIndexRebuildRunningMarker(
					ctx,
					tenantID,
					taskID,
				)
			}
			return current, nil
		}
		if !errors.Is(err, ErrConflict) {
			return IndexTask{}, err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return IndexTask{}, err
		}
	}
	return IndexTask{}, fmt.Errorf(
		"%w: index task %q changed while updating",
		ErrConflict,
		taskID,
	)
}
