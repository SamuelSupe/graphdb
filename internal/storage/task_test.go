package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestUnifiedTaskRunsCompactAndExportSnapshot(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	compact, err := store.StartTask(ctx, "tenant-a", TaskTypeCompact, nil)
	if err != nil {
		t.Fatalf("start compact task: %v", err)
	}
	compact = waitForTask(t, ctx, store, "tenant-a", compact.ID)
	if compact.Status != "succeeded" || compact.Result["version"] == nil {
		t.Fatalf("compact task = %#v", compact)
	}
	compactData, err := store.Objects.Get(ctx, store.taskKey("tenant-a", compact.ID))
	if err != nil {
		t.Fatalf("compact task object: %v", err)
	}
	if !isParquetBytes(compactData) {
		t.Fatalf("compact task object is not parquet")
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(manifest.CommitKeys) != 0 || manifest.SnapshotKey == "" {
		t.Fatalf("manifest after compact = %#v", manifest)
	}
	assertTaskActionCompleted(t, compact, "load_current")
	assertTaskActionCompleted(t, compact, "write_snapshot_catalog")
	assertTaskActionCompleted(t, compact, "write_snapshot_record")
	assertTaskActionCompleted(t, compact, "publish_manifest")

	exportTask, err := store.StartTask(ctx, "tenant-a", TaskTypeExportSnapshot, nil)
	if err != nil {
		t.Fatalf("start export task: %v", err)
	}
	exportTask = waitForTask(t, ctx, store, "tenant-a", exportTask.ID)
	if exportTask.Status != "succeeded" || exportTask.ResultKey == "" {
		t.Fatalf("export task = %#v", exportTask)
	}
	if exportTask.ProgressCompleted != exportTask.ProgressTotal || exportTask.Checkpoint["completed"] != true || exportTask.Checkpoint["result_key"] != exportTask.ResultKey {
		t.Fatalf("export progress/checkpoint = %#v", exportTask)
	}
	assertTaskActionCompleted(t, exportTask, "load_snapshot")
	assertTaskActionCompleted(t, exportTask, "write_result")
	if _, err := store.Objects.Get(ctx, exportTask.ResultKey); err != nil {
		t.Fatalf("export result object: %v", err)
	}
	resultData, err := store.Objects.Get(ctx, exportTask.ResultKey)
	if err != nil {
		t.Fatalf("export result object: %v", err)
	}
	if !isParquetBytes(resultData) {
		t.Fatalf("export result object is not parquet")
	}
	result, err := decodeParquetTaskResult(ctx, resultData, "tenant-a", exportTask.ID)
	if err != nil {
		t.Fatalf("decode export result: %v", err)
	}
	if result["tenant_id"] != "tenant-a" {
		t.Fatalf("export result = %#v", result)
	}
}

func TestUnifiedTaskListIncludesLegacyIndexTasks(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	indexTask, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start index task: %v", err)
	}
	waitForIndexTaskStatus(t, ctx, store, "tenant-a", indexTask.ID)
	tasks, err := store.ListTasks(ctx, "tenant-a", TaskListOptions{Type: TaskTypeIndexRebuild})
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != indexTask.ID || tasks[0].Type != TaskTypeIndexRebuild {
		t.Fatalf("tasks = %#v", tasks)
	}
	if _, ok := tasks[0].Result["legacy_index_task"]; !ok {
		t.Fatalf("legacy index task marker missing: %#v", tasks[0])
	}
}

func TestTaskCancelAndRetry(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	now := time.Now().UTC()
	task := Task{
		ID:            "task-cancel",
		TenantID:      "tenant-a",
		Type:          TaskTypeExportSnapshot,
		Status:        TaskStatusQueued,
		Phase:         TaskStatusQueued,
		ProgressTotal: 1,
		OwnerID:       store.InstanceID,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	canceled, err := store.CancelTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("cancel task: %v", err)
	}
	if canceled.Status != TaskStatusCanceled || canceled.FinishedAt.IsZero() {
		t.Fatalf("canceled task = %#v", canceled)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("retry task: %v", err)
	}
	if retry.ID == task.ID || retry.Type != task.Type || retry.Params["retry_of"] != task.ID {
		t.Fatalf("retry task = %#v", retry)
	}
	retry = waitForTask(t, ctx, store, "tenant-a", retry.ID)
	if retry.Status != TaskStatusSucceeded || retry.ResultKey == "" {
		t.Fatalf("completed retry = %#v", retry)
	}
}

func TestRetryExportTaskReusesCheckpointResult(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	exportTask, err := store.StartTask(ctx, "tenant-a", TaskTypeExportSnapshot, nil)
	if err != nil {
		t.Fatalf("start export: %v", err)
	}
	exportTask = waitForTask(t, ctx, store, "tenant-a", exportTask.ID)
	if exportTask.Status != TaskStatusSucceeded || exportTask.ResultKey == "" {
		t.Fatalf("export task = %#v", exportTask)
	}
	now := time.Now().UTC()
	failed := Task{
		ID:         "task-export-failed",
		TenantID:   "tenant-a",
		Type:       TaskTypeExportSnapshot,
		Status:     TaskStatusFailed,
		Phase:      TaskStatusFailed,
		Checkpoint: map[string]any{"result_key": exportTask.ResultKey},
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, failed); err != nil {
		t.Fatalf("save failed task: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", failed.ID)
	if err != nil {
		t.Fatalf("retry export: %v", err)
	}
	retry = waitForTask(t, ctx, store, "tenant-a", retry.ID)
	if retry.Status != TaskStatusSucceeded || retry.ResultKey != exportTask.ResultKey || retry.Result["resumed"] != true {
		t.Fatalf("retry export task = %#v", retry)
	}
}

func TestRetryRunningTaskRejected(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:        "task-running",
		TenantID:  "tenant-a",
		Type:      TaskTypeCompact,
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	if _, err := store.RetryTask(ctx, "tenant-a", task.ID); err == nil {
		t.Fatal("retry running task succeeded")
	}
}

func TestRetryGCTaskUsesCheckpointCursor(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:         "task-gc",
		TenantID:   "tenant-a",
		Type:       TaskTypeGC,
		Status:     TaskStatusFailed,
		Phase:      TaskStatusFailed,
		Params:     map[string]any{"max_deletes": float64(10)},
		Result:     map[string]any{"checkpoint": map[string]any{"next_cursor": "key-10"}},
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("retry task: %v", err)
	}
	if retry.Params["cursor"] != "key-10" || retry.Params["retry_of"] != task.ID {
		t.Fatalf("retry params = %#v", retry.Params)
	}
}

func TestRetryReplayTaskUsesCheckpointCursor(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:         "task-replay",
		TenantID:   "tenant-a",
		Type:       TaskTypeReplayDeadLetter,
		Status:     TaskStatusFailed,
		Phase:      TaskStatusFailed,
		Params:     map[string]any{"source": "agent"},
		Checkpoint: map[string]any{"next_cursor": "deadletter-key-10"},
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("retry task: %v", err)
	}
	if retry.Params["cursor"] != "deadletter-key-10" || retry.Params["retry_of"] != task.ID {
		t.Fatalf("retry params = %#v", retry.Params)
	}
}

func TestTaskCheckpointRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:         "task-checkpoint",
		TenantID:   "tenant-a",
		Type:       TaskTypeGC,
		Status:     TaskStatusRunning,
		Phase:      "gc",
		Checkpoint: map[string]any{"next_cursor": "key-1", "scanned": 2},
		StartedAt:  now,
		UpdatedAt:  now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	loaded, err := store.GetTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Checkpoint["next_cursor"] != "key-1" || loaded.Checkpoint["scanned"] != float64(2) {
		t.Fatalf("checkpoint = %#v", loaded.Checkpoint)
	}
}

func TestTaskRunnerDoesNotOverwritePersistedCancel(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:            "task-canceled-before-run",
		TenantID:      "tenant-a",
		Type:          TaskTypeExportSnapshot,
		Status:        TaskStatusQueued,
		Phase:         TaskStatusQueued,
		ProgressTotal: 1,
		OwnerID:       store.InstanceID,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	if _, err := store.CancelTask(ctx, "tenant-a", task.ID); err != nil {
		t.Fatalf("cancel task: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.runTask(runCtx, cancel, task)
	loaded, err := store.GetTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != TaskStatusCanceled || loaded.Phase != TaskStatusCanceled {
		t.Fatalf("loaded task = %#v", loaded)
	}
}

func TestUpdateTaskProgressSeesPersistedCancel(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:        "task-progress-canceled",
		TenantID:  "tenant-a",
		Type:      TaskTypeCompact,
		Status:    TaskStatusCanceled,
		Phase:     TaskStatusCanceled,
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	if err := store.updateTaskProgress(ctx, task, "compact", 1, 2, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("progress err = %v, want context.Canceled", err)
	}
}

func waitForTask(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, taskID string) Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(ctx, tenantID, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if task.Status != "queued" && task.Status != "running" {
			if task.Status == "failed" {
				t.Fatalf("task failed: %#v", task)
			}
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish", taskID)
	return Task{}
}

func assertTaskActionCompleted(t *testing.T, task Task, actionID string) {
	t.Helper()
	if !taskActionCompleted(task, actionID) {
		t.Fatalf("task %s action %q not completed: checkpoint=%#v", task.ID, actionID, task.Checkpoint)
	}
}

func waitForIndexTaskStatus(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, taskID string) IndexTask {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetIndexTask(ctx, tenantID, taskID)
		if err != nil {
			t.Fatalf("get index task: %v", err)
		}
		if task.Status != "running" {
			if task.Status != "succeeded" {
				t.Fatalf("index task = %#v", task)
			}
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("index task %s did not finish", taskID)
	return IndexTask{}
}
