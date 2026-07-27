package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"
)

func (s *TenantStore) gcRunningTaskKey(tenantID string) string {
	return path.Join(s.taskPrefix(tenantID), "_active", "gc.parquet")
}

func (s *TenantStore) claimGCRunningMarker(ctx context.Context, task Task) (Task, bool, error) {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		marker, meta, err := s.getGCRunningMarker(ctx, task.TenantID)
		if errors.Is(err, ErrNotFound) {
			task.UpdatedAt = time.Now().UTC()
			if _, err := s.putActiveGCRunningMarker(ctx, task, ObjectMeta{Key: s.gcRunningTaskKey(task.TenantID)}); err == nil {
				return Task{}, false, nil
			} else if !errors.Is(err, ErrConflict) {
				return Task{}, false, err
			}
		} else if err != nil {
			return Task{}, false, err
		} else if s.gcMarkerActive(marker, time.Now().UTC()) {
			return marker, true, nil
		} else {
			if taskStillActive(marker) {
				if err := s.failStaleGCTask(ctx, marker); err != nil {
					return Task{}, false, err
				}
			}
			task.UpdatedAt = time.Now().UTC()
			if _, err := s.putActiveGCRunningMarker(ctx, task, meta); err == nil {
				return Task{}, false, nil
			} else if !errors.Is(err, ErrConflict) {
				return Task{}, false, err
			}
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return Task{}, false, err
		}
	}
	return Task{}, false, fmt.Errorf("%w: gc marker for tenant %q changed while claiming", ErrConflict, task.TenantID)
}

func (s *TenantStore) findRunningGCTask(ctx context.Context, tenantID string) (Task, bool, error) {
	marker, _, err := s.getGCRunningMarker(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, err
	}
	if s.gcMarkerActive(marker, time.Now().UTC()) {
		return marker, true, nil
	}
	if taskStillActive(marker) {
		if err := s.failStaleGCTask(ctx, marker); err != nil {
			return Task{}, false, err
		}
	}
	return Task{}, false, nil
}

func (s *TenantStore) failStaleGCTask(ctx context.Context, marker Task) error {
	_, err := s.mutateTask(ctx, marker.TenantID, marker.ID, func(task *Task) error {
		if !taskStillActive(*task) {
			return nil
		}
		now := time.Now().UTC()
		task.Status = TaskStatusFailed
		task.Phase = TaskStatusFailed
		task.Error = "task owner heartbeat expired"
		task.UpdatedAt = now
		task.FinishedAt = now
		return nil
	})
	if errors.Is(err, ErrNotFound) {
		failed := marker
		now := time.Now().UTC()
		failed.Status = TaskStatusFailed
		failed.Phase = TaskStatusFailed
		failed.Error = "task owner heartbeat expired"
		failed.UpdatedAt = now
		failed.FinishedAt = now
		return s.saveTask(ctx, failed)
	}
	return err
}

func (s *TenantStore) syncGCRunningMarker(ctx context.Context, task Task) error {
	if task.Type != TaskTypeGC {
		return nil
	}
	if taskStillActive(task) {
		return s.refreshGCRunningMarker(ctx, task)
	}
	return s.finishGCRunningMarker(ctx, task)
}

func (s *TenantStore) refreshGCRunningMarker(ctx context.Context, task Task) error {
	if _, err := s.prepareTenantWrite(ctx, task.TenantID); err != nil {
		return err
	}
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		marker, meta, err := s.getGCRunningMarker(ctx, task.TenantID)
		if errors.Is(err, ErrNotFound) {
			if _, err := s.putActiveGCRunningMarker(ctx, task, ObjectMeta{Key: s.gcRunningTaskKey(task.TenantID)}); err == nil {
				return nil
			} else if !errors.Is(err, ErrConflict) {
				return err
			}
		} else if err != nil {
			return err
		} else {
			if marker.ID != task.ID {
				if s.gcMarkerActive(marker, time.Now().UTC()) {
					return fmt.Errorf("%w: gc task %q owns tenant %q marker", ErrConflict, marker.ID, task.TenantID)
				}
				return fmt.Errorf("%w: gc marker ownership changed", ErrConflict)
			}
			if !taskStillActive(marker) {
				return context.Canceled
			}
			marker.UpdatedAt = task.UpdatedAt
			if _, err := s.putActiveGCRunningMarker(ctx, marker, meta); err == nil {
				return nil
			} else if !errors.Is(err, ErrConflict) {
				return err
			}
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: gc marker changed while refreshing", ErrConflict)
}

func (s *TenantStore) finishGCRunningMarker(ctx context.Context, task Task) error {
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		marker, meta, err := s.getGCRunningMarker(ctx, task.TenantID)
		if errors.Is(err, ErrNotFound) || (err == nil && marker.ID != task.ID) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := s.putGCRunningMarker(ctx, task, meta); err == nil {
			return nil
		} else if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: gc marker changed while finishing", ErrConflict)
}

func (s *TenantStore) abandonGCRunningMarker(ctx context.Context, task Task) {
	now := time.Now().UTC()
	task.Status = TaskStatusFailed
	task.Phase = TaskStatusFailed
	task.Error = "task persistence failed"
	task.UpdatedAt = now
	task.FinishedAt = now
	if _, err := s.mutateTask(ctx, task.TenantID, task.ID, func(current *Task) error {
		*current = task
		return nil
	}); err != nil {
		_ = s.finishGCRunningMarker(ctx, task)
	}
}

func (s *TenantStore) getGCRunningMarker(ctx context.Context, tenantID string) (Task, ObjectMeta, error) {
	key := s.gcRunningTaskKey(tenantID)
	s.clearWriterObjectKey(key)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return Task{}, meta, err
	}
	if !isParquetBytes(data) {
		return Task{}, meta, fmt.Errorf("unsupported gc running marker")
	}
	task, err := decodeParquetTask(ctx, data)
	if err != nil {
		return Task{}, meta, err
	}
	if task.TenantID != tenantID || task.Type != TaskTypeGC || task.ID == "" {
		return Task{}, meta, fmt.Errorf("gc running marker mismatch for tenant %q", tenantID)
	}
	return task, meta, nil
}

func (s *TenantStore) putGCRunningMarker(ctx context.Context, task Task, meta ObjectMeta) (ObjectMeta, error) {
	data, err := marshalParquetTask(ctx, task)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{IfNoneMatch: !meta.Exists}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	}
	return s.putTenantGenerationConditional(ctx, task.TenantID, s.gcRunningTaskKey(task.TenantID), data, condition)
}

func (s *TenantStore) putActiveGCRunningMarker(ctx context.Context, task Task, meta ObjectMeta) (ObjectMeta, error) {
	data, err := marshalParquetTask(ctx, task)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{IfNoneMatch: !meta.Exists}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	}
	return s.putTenantConditional(ctx, task.TenantID, s.gcRunningTaskKey(task.TenantID), data, condition)
}

func (s *TenantStore) gcMarkerActive(task Task, now time.Time) bool {
	if !taskStillActive(task) {
		return false
	}
	updatedAt := task.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = task.StartedAt
	}
	return !updatedAt.IsZero() && now.Before(updatedAt.Add(s.taskMarkerTTL()))
}

func (s *TenantStore) taskMarkerTTL() time.Duration {
	if s.TaskMarkerTTL <= 0 {
		return 30 * time.Second
	}
	return s.TaskMarkerTTL
}

func (s *TenantStore) startGCTaskHeartbeat(
	ctx context.Context,
	task Task,
	cancelTask context.CancelFunc,
) func() {
	if task.Type != TaskTypeGC {
		return func() {}
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	interval := s.taskMarkerTTL() / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				task.UpdatedAt = time.Now().UTC()
				if err := s.refreshGCRunningMarker(heartbeatCtx, task); err != nil {
					cancelTask()
					return
				}
			}
		}
	}()
	return stopHeartbeat
}
