package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	TaskTypeCompact            = "compact"
	TaskTypeGC                 = "gc"
	TaskTypeRepair             = "repair"
	TaskTypeExportSnapshot     = "export_snapshot"
	TaskTypeReplayDeadLetter   = "replay_deadletters"
	TaskTypeIndexRebuild       = "index_rebuild"
	TaskTypeTenantBackup       = "tenant_backup"
	TaskTypeTenantRestore      = "tenant_restore"
	TaskTypeTenantRestoreDrill = "tenant_restore_drill"
)

type Task struct {
	ID                string         `json:"id"`
	TenantID          string         `json:"tenant_id"`
	Type              string         `json:"type"`
	Status            string         `json:"status"`
	Phase             string         `json:"phase,omitempty"`
	ProgressCompleted int            `json:"progress_completed,omitempty"`
	ProgressTotal     int            `json:"progress_total,omitempty"`
	OwnerID           string         `json:"owner_id,omitempty"`
	Params            map[string]any `json:"params,omitempty"`
	Checkpoint        map[string]any `json:"checkpoint,omitempty"`
	Result            map[string]any `json:"result,omitempty"`
	ResultKey         string         `json:"result_key,omitempty"`
	Error             string         `json:"error,omitempty"`
	StartedAt         time.Time      `json:"started_at"`
	UpdatedAt         time.Time      `json:"updated_at,omitempty"`
	FinishedAt        time.Time      `json:"finished_at,omitempty"`
}

type TaskListOptions struct {
	Type   string
	Status string
	Limit  int
}

func (s *TenantStore) StartTask(ctx context.Context, tenantID string, taskType string, params map[string]any) (Task, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Task{}, err
	}
	taskType, err := normalizeTaskType(taskType)
	if err != nil {
		return Task{}, err
	}
	params = cloneTaskParams(params)
	if err := validateTaskParams(taskType, params); err != nil {
		return Task{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return Task{}, err
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return Task{}, err
	}
	checkpoint := taskInitialCheckpoint(params)
	id, err := newCommitID()
	if err != nil {
		return Task{}, err
	}
	now := time.Now().UTC()
	task := Task{
		ID:            id,
		TenantID:      tenantID,
		Type:          taskType,
		Status:        "queued",
		Phase:         "queued",
		ProgressTotal: 1,
		OwnerID:       s.InstanceID,
		Params:        params,
		Checkpoint:    checkpoint,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if active, reused, err := s.admitTask(task); err != nil {
		return Task{}, err
	} else if reused {
		return active, nil
	}
	if err := s.saveTask(ctx, task); err != nil {
		s.releaseTaskAdmission(task)
		return Task{}, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	s.registerTaskCancel(tenantID, id, cancel)
	go s.runTaskAdmitted(runCtx, cancel, task)
	return task, nil
}

func (s *TenantStore) GetTask(ctx context.Context, tenantID string, taskID string) (Task, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Task{}, err
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return Task{}, fmt.Errorf("task id is required")
	}
	task, err := s.getTaskObject(ctx, tenantID, taskID)
	if errors.Is(err, ErrNotFound) {
		indexTask, indexErr := s.GetIndexTask(ctx, tenantID, taskID)
		if indexErr != nil {
			return Task{}, err
		}
		return taskFromIndexTask(indexTask), nil
	}
	if err != nil {
		return Task{}, err
	}
	if task.TenantID != tenantID {
		return Task{}, fmt.Errorf("task tenant mismatch: path tenant %q contains tenant %q", tenantID, task.TenantID)
	}
	if task.ID != taskID {
		return Task{}, fmt.Errorf("task id mismatch: path task %q contains task %q", taskID, task.ID)
	}
	return task, nil
}

func (s *TenantStore) ListTasks(ctx context.Context, tenantID string, options TaskListOptions) ([]Task, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	tasks, err := s.listStoredTasks(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	indexTasks, err := s.listIndexTasksAsTasks(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, indexTasks...)
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].StartedAt.Equal(tasks[j].StartedAt) {
			return tasks[i].ID > tasks[j].ID
		}
		return tasks[i].StartedAt.After(tasks[j].StartedAt)
	})
	filtered := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		if options.Type != "" && task.Type != options.Type {
			continue
		}
		if options.Status != "" && task.Status != options.Status {
			continue
		}
		filtered = append(filtered, task)
		if options.Limit > 0 && len(filtered) >= options.Limit {
			break
		}
	}
	return filtered, nil
}

func (s *TenantStore) runTask(ctx context.Context, cancel context.CancelFunc, task Task) {
	defer s.unregisterTaskCancel(task.TenantID, task.ID)
	stopWatch := s.watchTaskCancellation(task, cancel)
	defer stopWatch()
	defer func() {
		if recovered := recover(); recovered != nil {
			task.Status = "failed"
			task.Phase = "failed"
			task.Error = fmt.Sprintf("panic: %v", recovered)
			task.FinishedAt = time.Now().UTC()
			task.UpdatedAt = task.FinishedAt
			s.trySaveTask(context.Background(), task)
		}
	}()
	if s.taskCancelRequested(context.Background(), task) {
		return
	}
	task.Status = "running"
	task.Phase = "running"
	task.ProgressTotal = taskProgressTotal(task.Type)
	task.UpdatedAt = time.Now().UTC()
	s.trySaveTask(context.Background(), task)
	result, resultKey, err := s.runTaskOperation(ctx, task)
	task = s.taskStateOrLocal(context.Background(), task)
	task.FinishedAt = time.Now().UTC()
	task.UpdatedAt = task.FinishedAt
	if ctx.Err() != nil || taskCanceled(err) || task.Status == TaskStatusCanceled {
		task.Status = "canceled"
		task.Phase = "canceled"
		task.Error = "canceled"
		s.trySaveTask(context.Background(), task)
		return
	}
	if err != nil {
		task.Status = "failed"
		task.Phase = "failed"
		task.Error = err.Error()
		s.trySaveTask(context.Background(), task)
		return
	}
	task.Status = "succeeded"
	task.Phase = "done"
	task.ProgressTotal = max(1, task.ProgressTotal)
	task.ProgressCompleted = task.ProgressTotal
	task.Result = result
	task.ResultKey = resultKey
	if task.Checkpoint == nil {
		task.Checkpoint = map[string]any{}
	}
	task.Checkpoint["completed"] = true
	s.trySaveTask(context.Background(), task)
}

func (s *TenantStore) runTaskOperation(ctx context.Context, task Task) (map[string]any, string, error) {
	total := taskProgressTotal(task.Type)
	if err := s.updateTaskProgress(ctx, task, "starting", 0, total, map[string]any{"phase": "starting"}); err != nil {
		return nil, "", err
	}
	switch task.Type {
	case TaskTypeCompact:
		return s.compactTask(ctx, task)
	case TaskTypeGC:
		if err := s.updateTaskProgress(ctx, task, "gc", 1, total, map[string]any{"phase": "gc", "cursor": stringTaskParam(task.Params, "cursor")}); err != nil {
			return nil, "", err
		}
		report, err := s.RunGC(ctx, task.TenantID, gcOptionsFromTask(task.Params))
		if err == nil {
			_ = s.updateTaskProgress(ctx, task, "gc_done", total, total, taskResult(report.Checkpoint))
		}
		return taskResult(report), "", err
	case TaskTypeRepair:
		if err := s.updateTaskProgress(ctx, task, "repair", 1, total, map[string]any{"phase": "repair", "apply": boolTaskParam(task.Params, "apply")}); err != nil {
			return nil, "", err
		}
		progress := func(ctx context.Context, action RepairAction, completed int, actionTotal int) error {
			taskTotal := max(total, actionTotal+2)
			checkpoint := map[string]any{
				"phase":                   "repair_" + action.Type,
				"repair_action":           action.Type,
				"repair_action_status":    action.Status,
				"repair_action_completed": completed,
				"repair_action_total":     actionTotal,
			}
			if action.Message != "" {
				checkpoint["repair_action_message"] = action.Message
			}
			return s.updateTaskProgress(ctx, task, "repair_"+action.Type, min(1+completed, taskTotal-1), taskTotal, checkpoint)
		}
		report, err := s.RepairTenant(ctx, task.TenantID, RepairOptions{Apply: boolTaskParam(task.Params, "apply"), Progress: progress})
		if err == nil {
			taskTotal := max(total, len(report.Plan)+2)
			_ = s.updateTaskProgress(ctx, task, "repair_done", taskTotal, taskTotal, map[string]any{"phase": "repair_done", "status": report.Status, "actions": len(report.Actions), "remaining_issues": len(report.RemainingIssues), "verification_status": verificationStatus(report.Verification)})
		}
		return taskResult(report), "", err
	case TaskTypeExportSnapshot:
		return s.exportSnapshotTask(ctx, task)
	case TaskTypeReplayDeadLetter:
		report, err := s.replayDeadLettersTask(ctx, task)
		return taskResult(report), "", err
	case TaskTypeIndexRebuild:
		if err := s.updateTaskProgress(ctx, task, "index_rebuild", 1, total, map[string]any{"phase": "index_rebuild"}); err != nil {
			return nil, "", err
		}
		catalog, err := s.RebuildIndexes(ctx, task.TenantID)
		if err == nil {
			_ = s.updateTaskProgress(ctx, task, "index_rebuild_done", total, total, map[string]any{"phase": "index_rebuild_done", "version": catalog.Version})
		}
		return taskResult(catalog), "", err
	case TaskTypeTenantBackup:
		return s.tenantBackupTask(ctx, task)
	case TaskTypeTenantRestore:
		report, err := s.tenantRestoreTask(ctx, task)
		return taskResult(report), "", err
	case TaskTypeTenantRestoreDrill:
		report, err := s.tenantRestoreDrillTask(ctx, task)
		return taskResult(report), "", err
	default:
		return nil, "", fmt.Errorf("unsupported task type %q", task.Type)
	}
}

func (s *TenantStore) exportSnapshotTask(ctx context.Context, task Task) (map[string]any, string, error) {
	total := taskProgressTotal(task.Type)
	if resultKey := taskCheckpointString(task, "result_key"); resultKey != "" {
		result, ok, err := s.loadTaskResultByKey(ctx, resultKey)
		if err != nil {
			return nil, "", err
		}
		if ok {
			_ = s.updateTaskActionProgress(ctx, task, "export_done", total, total, taskActionUpdate{
				ID:     "write_result",
				Status: "completed",
				Output: map[string]any{"result_key": resultKey, "resumed": true, "version": result["version"]},
				Verification: map[string]any{
					"result_readable": true,
				},
			}, map[string]any{"phase": "export_done", "result_key": resultKey, "resumed": true})
			return exportTaskSummary(result, resultKey, true), resultKey, nil
		}
	}
	if err := s.updateTaskActionProgress(ctx, task, "export_load_snapshot", 1, total, taskActionUpdate{
		ID:     "load_snapshot",
		Status: "running",
	}, nil); err != nil {
		return nil, "", err
	}
	g, manifest, err := s.Load(ctx, task.TenantID)
	if err != nil {
		_ = s.updateTaskActionProgress(context.Background(), task, "export_load_snapshot", 1, total, taskActionUpdate{ID: "load_snapshot", Err: err}, nil)
		return nil, "", err
	}
	snapshot := g.Snapshot()
	if err := s.updateTaskActionProgress(ctx, task, "export_write_result", 2, total, taskActionUpdate{
		ID:     "load_snapshot",
		Status: "completed",
		Output: map[string]any{"version": manifest.Version, "entities": len(snapshot.Entities), "edges": len(snapshot.Edges)},
	}, map[string]any{"version": manifest.Version}); err != nil {
		return nil, "", err
	}
	resultKey := s.taskResultKey(task.TenantID, task.ID)
	record := map[string]any{"tenant_id": task.TenantID, "version": manifest.Version, "snapshot": snapshot}
	if err := s.updateTaskActionProgress(ctx, task, "export_write_result", 2, total, taskActionUpdate{
		ID:     "write_result",
		Status: "running",
		Input:  map[string]any{"version": manifest.Version, "result_key": resultKey},
	}, map[string]any{"version": manifest.Version, "result_key": resultKey}); err != nil {
		return nil, "", err
	}
	if err := s.putTaskResult(ctx, task.TenantID, task.ID, record); err != nil {
		_ = s.updateTaskActionProgress(context.Background(), task, "export_write_result", 2, total, taskActionUpdate{ID: "write_result", Err: err}, nil)
		return nil, "", err
	}
	_ = s.updateTaskActionProgress(ctx, task, "export_done", total, total, taskActionUpdate{
		ID:     "write_result",
		Status: "completed",
		Output: map[string]any{"version": manifest.Version, "result_key": resultKey},
		Verification: map[string]any{
			"result_readable": true,
		},
	}, map[string]any{"phase": "export_done", "version": manifest.Version, "result_key": resultKey})
	return map[string]any{
		"tenant_id":      task.TenantID,
		"version":        manifest.Version,
		"ci_types":       len(snapshot.CITypes),
		"relation_types": len(snapshot.RelationTypes),
		"entities":       len(snapshot.Entities),
		"edges":          len(snapshot.Edges),
		"result_key":     resultKey,
	}, resultKey, nil
}

func (s *TenantStore) listStoredTasks(ctx context.Context, tenantID string) ([]Task, error) {
	objects, err := s.Objects.List(ctx, s.taskPrefix(tenantID))
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, 0, len(objects))
	prefix := s.taskPrefix(tenantID)
	for _, object := range objects {
		rest := strings.TrimPrefix(object.Key, prefix)
		if strings.Contains(rest, "/") {
			continue
		}
		taskID, ok := taskIDFromKey(object.Key)
		if !ok {
			continue
		}
		task, err := s.GetTask(ctx, tenantID, taskID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (s *TenantStore) listIndexTasksAsTasks(ctx context.Context, tenantID string) ([]Task, error) {
	objects, err := s.Objects.List(ctx, s.indexTaskPrefix(tenantID))
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, 0, len(objects))
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
			return nil, err
		}
		tasks = append(tasks, taskFromIndexTask(task))
	}
	return tasks, nil
}

func (s *TenantStore) saveTask(ctx context.Context, task Task) error {
	data, err := marshalParquetTask(ctx, task)
	if err != nil {
		return err
	}
	return s.Objects.Put(ctx, s.taskKey(task.TenantID, task.ID), data)
}

func (s *TenantStore) trySaveTask(ctx context.Context, task Task) {
	defer func() {
		_ = recover()
	}()
	for attempt := 0; attempt < s.retryCount(); attempt++ {
		if err := s.saveTask(ctx, task); err == nil {
			return
		}
		if attempt+1 < s.retryCount() {
			_ = retryDelay(ctx, attempt)
		}
	}
}

func taskFromIndexTask(task IndexTask) Task {
	result := map[string]any{}
	if task.CatalogVersion > 0 {
		result["catalog_version"] = task.CatalogVersion
	}
	result["legacy_index_task"] = true
	return Task{
		ID:                task.ID,
		TenantID:          task.TenantID,
		Type:              TaskTypeIndexRebuild,
		Status:            task.Status,
		Phase:             task.Phase,
		ProgressCompleted: task.ProgressCompleted,
		ProgressTotal:     task.ProgressTotal,
		OwnerID:           task.OwnerID,
		Result:            result,
		Error:             task.Error,
		StartedAt:         task.StartedAt,
		UpdatedAt:         task.UpdatedAt,
		FinishedAt:        task.FinishedAt,
	}
}

func normalizeTaskType(taskType string) (string, error) {
	taskType = strings.TrimSpace(taskType)
	switch taskType {
	case TaskTypeCompact, TaskTypeGC, TaskTypeRepair, TaskTypeExportSnapshot, TaskTypeReplayDeadLetter, TaskTypeIndexRebuild, TaskTypeTenantBackup, TaskTypeTenantRestore, TaskTypeTenantRestoreDrill:
		return taskType, nil
	default:
		return "", fmt.Errorf("unsupported task type %q", taskType)
	}
}

func validateTaskParams(taskType string, params map[string]any) error {
	switch taskType {
	case TaskTypeReplayDeadLetter:
		if strings.TrimSpace(stringTaskParam(params, "source")) == "" {
			return fmt.Errorf("source is required for replay_deadletters task")
		}
	case TaskTypeIndexRebuild:
		if strings.TrimSpace(stringTaskParam(params, "format")) != "" {
			return fmt.Errorf("index_rebuild task format is fixed to parquet")
		}
	case TaskTypeTenantRestore:
		if strings.TrimSpace(stringTaskParam(params, "backup_key")) == "" {
			return fmt.Errorf("backup_key is required for tenant_restore task")
		}
	case TaskTypeTenantRestoreDrill:
		if targetTenantID := strings.TrimSpace(stringTaskParam(params, "target_tenant_id")); targetTenantID != "" {
			if err := ValidateTenantID(targetTenantID); err != nil {
				return err
			}
		}
	}
	if intTaskParam(params, "limit") < 0 || intTaskParam(params, "keep_snapshots") < 0 || intTaskParam(params, "max_deletes") < 0 || int64TaskParam(params, "deadletter_max_age_seconds") < 0 || int64TaskParam(params, "task_max_age_seconds") < 0 || int64TaskParam(params, "query_timeout_ms") < 0 {
		return fmt.Errorf("task numeric params must be non-negative")
	}
	return nil
}

func gcOptionsFromTask(params map[string]any) GCOptions {
	return GCOptions{
		KeepSnapshots:       intTaskParam(params, "keep_snapshots"),
		DeadLetterMaxAge:    time.Duration(int64TaskParam(params, "deadletter_max_age_seconds")) * time.Second,
		TaskMaxAge:          time.Duration(int64TaskParam(params, "task_max_age_seconds")) * time.Second,
		CleanupIndexOrphans: boolTaskParam(params, "cleanup_index_orphans"),
		CheckpointCursor:    stringTaskParam(params, "cursor"),
		MaxDeletes:          intTaskParam(params, "max_deletes"),
		DryRun:              boolTaskParam(params, "dry_run"),
	}
}

func cloneTaskParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return nil
	}
	copied := make(map[string]any, len(params))
	for key, value := range params {
		copied[key] = value
	}
	return copied
}

func taskResult(value any) map[string]any {
	data, err := json.Marshal(value)
	if err != nil {
		return map[string]any{"value": value}
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return map[string]any{"value": value}
	}
	return result
}

func stringTaskParam(params map[string]any, key string) string {
	value, ok := params[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func boolTaskParam(params map[string]any, key string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return typed == "true" || typed == "1"
	default:
		return false
	}
}

func intTaskParam(params map[string]any, key string) int {
	return int(int64TaskParam(params, key))
}

func int64TaskParam(params map[string]any, key string) int64 {
	value, ok := params[key]
	if !ok {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		n, _ := typed.Int64()
		return n
	default:
		return 0
	}
}

func taskIDFromKey(key string) (string, bool) {
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

func (s *TenantStore) getTaskObject(ctx context.Context, tenantID string, taskID string) (Task, error) {
	data, err := s.Objects.Get(ctx, s.taskKey(tenantID, taskID))
	if err != nil {
		return Task{}, err
	}
	if !isParquetBytes(data) {
		return Task{}, fmt.Errorf("unsupported task object: only parquet tasks are readable")
	}
	return decodeParquetTask(ctx, data)
}

func (s *TenantStore) putTaskResult(ctx context.Context, tenantID string, taskID string, result map[string]any) error {
	data, err := marshalParquetTaskResult(ctx, tenantID, taskID, result)
	if err != nil {
		return err
	}
	return s.Objects.Put(ctx, s.taskResultKey(tenantID, taskID), data)
}
