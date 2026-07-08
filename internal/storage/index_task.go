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

type IndexTask struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	Type              string    `json:"type"`
	Status            string    `json:"status"`
	Phase             string    `json:"phase,omitempty"`
	ProgressCompleted int       `json:"progress_completed,omitempty"`
	ProgressTotal     int       `json:"progress_total,omitempty"`
	OwnerID           string    `json:"owner_id,omitempty"`
	CatalogVersion    int64     `json:"catalog_version,omitempty"`
	Error             string    `json:"error,omitempty"`
	StartedAt         time.Time `json:"started_at"`
	UpdatedAt         time.Time `json:"updated_at,omitempty"`
	FinishedAt        time.Time `json:"finished_at,omitempty"`
}

func (s *TenantStore) StartIndexRebuild(ctx context.Context, tenantID string) (IndexTask, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexTask{}, err
	}
	return s.startIndexRebuildLocked(ctx, tenantID)
}

func (s *TenantStore) startIndexRebuildLocked(ctx context.Context, tenantID string) (IndexTask, error) {
	return s.startIndexRebuild(ctx, tenantID, true)
}

func (s *TenantStore) startIndexRebuildAfterDefinitionChangeLocked(ctx context.Context, tenantID string) (IndexTask, error) {
	return s.startIndexRebuild(ctx, tenantID, false)
}

func (s *TenantStore) startIndexRebuild(ctx context.Context, tenantID string, reuseRunning bool) (IndexTask, error) {
	s.taskMu.Lock()
	if s.indexTasks == nil {
		s.indexTasks = map[string]IndexTask{}
	}
	if reuseRunning {
		if task, ok := s.indexTasks[tenantID]; ok && task.Status == "running" {
			s.taskMu.Unlock()
			return task, nil
		}
		if task, ok, err := s.findRunningIndexRebuildTask(ctx, tenantID); err != nil {
			s.taskMu.Unlock()
			return IndexTask{}, err
		} else if ok {
			s.indexTasks[tenantID] = task
			s.taskMu.Unlock()
			return task, nil
		}
	}
	id, err := newCommitID()
	if err != nil {
		s.taskMu.Unlock()
		return IndexTask{}, err
	}
	now := time.Now().UTC()
	task := IndexTask{ID: id, TenantID: tenantID, Type: "rebuild", Status: "running", Phase: "queued", ProgressTotal: 1, OwnerID: s.InstanceID, StartedAt: now, UpdatedAt: now}
	if err := s.saveIndexTask(ctx, task); err != nil {
		s.taskMu.Unlock()
		return IndexTask{}, err
	}
	s.indexTasks[tenantID] = task
	s.taskMu.Unlock()
	go s.runIndexRebuildTask(tenantID, task)
	return task, nil
}

func (s *TenantStore) findRunningIndexRebuildTask(ctx context.Context, tenantID string) (IndexTask, bool, error) {
	objects, err := s.Objects.List(ctx, s.indexTaskPrefix(tenantID))
	if err != nil {
		return IndexTask{}, false, err
	}
	var running IndexTask
	for _, object := range objects {
		taskID, ok := indexTaskIDFromKey(object.Key)
		if !ok {
			continue
		}
		task, err := s.GetIndexTask(ctx, tenantID, taskID)
		if errors.Is(err, ErrNotFound) || errors.Is(err, errInvalidIndexTask) {
			continue
		}
		if err != nil {
			return IndexTask{}, false, err
		}
		if task.TenantID != tenantID || task.Type != "rebuild" || task.Status != "running" {
			continue
		}
		active, err := s.indexTaskActive(ctx, tenantID, task, time.Now().UTC())
		if err != nil {
			return IndexTask{}, false, err
		}
		if !active {
			continue
		}
		if running.ID == "" || task.StartedAt.Before(running.StartedAt) {
			running = task
		}
	}
	return running, running.ID != "", nil
}

func (s *TenantStore) indexTaskActive(ctx context.Context, tenantID string, task IndexTask, now time.Time) (bool, error) {
	lease, err := s.GetWriterLease(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return now.Before(task.StartedAt.Add(s.leaseTTL())), nil
	}
	if err != nil {
		return false, err
	}
	if task.OwnerID == "" {
		return now.Before(task.StartedAt.Add(s.leaseTTL())), nil
	}
	return lease.OwnerID == task.OwnerID && lease.ExpiresAt.After(now), nil
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
	data, err := s.Objects.Get(ctx, s.indexTaskKey(tenantID, taskID))
	if err != nil {
		return IndexTask{}, err
	}
	if !isParquetBytes(data) {
		return IndexTask{}, fmt.Errorf("%w: only parquet index tasks are readable", errInvalidIndexTask)
	}
	task, err := decodeParquetIndexTask(ctx, data)
	if err != nil {
		return IndexTask{}, fmt.Errorf("%w: %v", errInvalidIndexTask, err)
	}
	if task.TenantID == "" || task.ID == "" {
		return IndexTask{}, fmt.Errorf("%w: task metadata is required", errInvalidIndexTask)
	}
	if task.TenantID != tenantID {
		return IndexTask{}, fmt.Errorf("%w: index task tenant mismatch: path tenant %q contains tenant %q", errInvalidIndexTask, tenantID, task.TenantID)
	}
	if task.ID != taskID {
		return IndexTask{}, fmt.Errorf("%w: index task id mismatch: path task %q contains task %q", errInvalidIndexTask, taskID, task.ID)
	}
	return task, nil
}

func (s *TenantStore) runIndexRebuildTask(tenantID string, task IndexTask) {
	defer s.clearIndexRebuildTask(tenantID, task.ID)
	defer func() {
		if recovered := recover(); recovered != nil {
			task.FinishedAt = time.Now().UTC()
			task.UpdatedAt = task.FinishedAt
			task.Status = "failed"
			task.Phase = "failed"
			task.Error = fmt.Sprintf("panic: %v", recovered)
			s.trySaveIndexTask(context.Background(), task)
		}
	}()
	task.Phase = "backfill"
	task.ProgressCompleted = 0
	task.ProgressTotal = 1
	task.UpdatedAt = time.Now().UTC()
	s.trySaveIndexTask(context.Background(), task)
	catalog, err := s.RebuildIndexes(context.Background(), tenantID)
	task.FinishedAt = time.Now().UTC()
	task.UpdatedAt = task.FinishedAt
	if err != nil {
		task.Status = "failed"
		task.Phase = "failed"
		task.Error = err.Error()
		s.trySaveIndexTask(context.Background(), task)
		return
	}
	task.Phase = "cleanup"
	task.CatalogVersion = catalog.Version
	task.UpdatedAt = time.Now().UTC()
	s.trySaveIndexTask(context.Background(), task)
	gcReport, cleanupErr := s.RunGC(context.Background(), tenantID, GCOptions{KeepSnapshots: 2, CleanupIndexOrphans: true, SkipEntityRecordCleanup: true})
	task.Status = "succeeded"
	task.Phase = "done"
	task.ProgressCompleted = 1
	task.ProgressTotal = 1
	task.CatalogVersion = catalog.Version
	if cleanupErr != nil {
		task.Error = "index cleanup failed: " + cleanupErr.Error()
	} else if gcReport.IndexCleanupError != "" {
		task.Error = "index cleanup failed: " + gcReport.IndexCleanupError
	} else if gcReport.IndexCleanupSkippedReason != "" {
		task.Error = "index cleanup skipped: " + gcReport.IndexCleanupSkippedReason
	}
	s.trySaveIndexTask(context.Background(), task)
}

func (s *TenantStore) clearIndexRebuildTask(tenantID string, taskID string) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if s.indexTasks == nil {
		return
	}
	if task, ok := s.indexTasks[tenantID]; ok && task.ID == taskID {
		delete(s.indexTasks, tenantID)
	}
}

func (s *TenantStore) saveIndexTask(ctx context.Context, task IndexTask) error {
	data, err := marshalParquetIndexTask(ctx, task)
	if err != nil {
		return err
	}
	return s.Objects.Put(ctx, s.indexTaskKey(task.TenantID, task.ID), data)
}

func (s *TenantStore) trySaveIndexTask(ctx context.Context, task IndexTask) {
	defer func() {
		_ = recover()
	}()
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		if err := s.saveIndexTask(ctx, task); err == nil {
			return
		}
		if attempt+1 < s.retryCount() {
			_ = retryDelay(ctx, attempt)
		}
	}
}
