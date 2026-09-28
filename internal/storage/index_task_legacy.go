package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"
)

var errInvalidIndexTask = errors.New("invalid index task")

func (s *TenantStore) indexTaskActive(ctx context.Context, tenantID string, task IndexTask, now time.Time) (bool, error) {
	return s.taskRuntimeActive(tenantID, task.ID), nil
}

func indexTaskIDFromKey(key string) (string, bool) {
	name := path.Base(key)
	if !strings.HasSuffix(name, ".parquet") {
		return "", false
	}
	id, err := url.PathUnescape(strings.TrimSuffix(name, ".parquet"))
	if err != nil || id == "" {
		return "", false
	}
	return id, true
}

func (s *TenantStore) GetIndexTask(ctx context.Context, tenantID string, taskID string) (IndexTask, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexTask{}, err
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return IndexTask{}, fmt.Errorf("index task id is required")
	}
	if task, err := s.getTaskObject(ctx, tenantID, taskID); err == nil {
		if task.Type != TaskTypeIndexRebuild {
			return IndexTask{}, fmt.Errorf("task %q is not an index rebuild", taskID)
		}
		task = s.reconcileInactiveTask(ctx, task)
		s.rememberTaskState(task)
		return indexTaskFromTask(task), nil
	} else if !errors.Is(err, ErrNotFound) {
		return IndexTask{}, err
	}
	task, _, err := s.getIndexTaskObjectWithMeta(ctx, tenantID, taskID)
	if err != nil {
		return IndexTask{}, err
	}
	return s.reconcileInactiveIndexTask(ctx, task), nil
}

func (s *TenantStore) getIndexTaskObjectWithMeta(
	ctx context.Context,
	tenantID string,
	taskID string,
) (IndexTask, ObjectMeta, error) {
	key := s.indexTaskKey(tenantID, taskID)
	s.clearWriterObjectKey(key)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return IndexTask{}, meta, err
	}
	if !isParquetBytes(data) {
		return IndexTask{}, meta, fmt.Errorf("%w: only parquet index tasks are readable", errInvalidIndexTask)
	}
	task, err := decodeParquetIndexTask(ctx, data)
	if err != nil {
		return IndexTask{}, meta, fmt.Errorf("%w: %v", errInvalidIndexTask, err)
	}
	if task.TenantID == "" || task.ID == "" {
		return IndexTask{}, meta, fmt.Errorf("%w: task metadata is required", errInvalidIndexTask)
	}
	if task.TenantID != tenantID {
		return IndexTask{}, meta, fmt.Errorf("%w: index task tenant mismatch: path tenant %q contains tenant %q", errInvalidIndexTask, tenantID, task.TenantID)
	}
	if task.ID != taskID {
		return IndexTask{}, meta, fmt.Errorf("%w: index task id mismatch: path task %q contains task %q", errInvalidIndexTask, taskID, task.ID)
	}
	return task, meta, nil
}

func (s *TenantStore) getIndexRebuildRunningMarker(ctx context.Context, tenantID string) (IndexTask, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexTask{}, err
	}
	key := s.indexRebuildRunningTaskKey(tenantID)
	s.clearWriterObjectKey(key)
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return IndexTask{}, err
	}
	if !isParquetBytes(data) {
		return IndexTask{}, fmt.Errorf("%w: only parquet index rebuild running markers are readable", errInvalidIndexTask)
	}
	task, err := decodeParquetIndexTask(ctx, data)
	if err != nil {
		return IndexTask{}, fmt.Errorf("%w: %v", errInvalidIndexTask, err)
	}
	if task.TenantID == "" || task.ID == "" {
		return IndexTask{}, fmt.Errorf("%w: running marker task metadata is required", errInvalidIndexTask)
	}
	if task.TenantID != tenantID {
		return IndexTask{}, fmt.Errorf("%w: running marker tenant mismatch: path tenant %q contains tenant %q", errInvalidIndexTask, tenantID, task.TenantID)
	}
	if task.Type != "rebuild" {
		return IndexTask{}, fmt.Errorf("%w: running marker type %q is not rebuild", errInvalidIndexTask, task.Type)
	}
	return task, nil
}

func (s *TenantStore) saveIndexRebuildRunningMarker(ctx context.Context, task IndexTask) error {
	data, err := marshalParquetIndexTask(ctx, task)
	if err != nil {
		return err
	}
	return s.putTenantGenerationObject(ctx, task.TenantID, s.indexRebuildRunningTaskKey(task.TenantID), data)
}

func (s *TenantStore) clearIndexRebuildRunningMarker(ctx context.Context, tenantID string, taskID string) error {
	task, err := s.getIndexRebuildRunningMarker(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if errors.Is(err, errInvalidIndexTask) {
		return s.deleteTenantGenerationObject(ctx, tenantID, s.indexRebuildRunningTaskKey(tenantID))
	}
	if err != nil {
		return err
	}
	if task.ID != taskID {
		return nil
	}
	return s.deleteTenantGenerationObject(ctx, tenantID, s.indexRebuildRunningTaskKey(tenantID))
}
