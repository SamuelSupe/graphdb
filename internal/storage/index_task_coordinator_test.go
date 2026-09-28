package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCanceledIndexTaskAdmissionPersistsTerminalStatus(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := Task{
		ID:            "canceled-before-execution",
		TenantID:      "tenant-a",
		Type:          TaskTypeIndexRebuild,
		Status:        TaskStatusRunning,
		Phase:         TaskStatusQueued,
		ProgressTotal: 1,
		OwnerID:       store.InstanceID,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("publish queued index task: %v", err)
	}
	store.taskMu.Lock()
	store.taskActive[taskActiveKey(task.TenantID, task.Type)] = task
	store.taskMu.Unlock()
	store.taskQueueSlots <- struct{}{}
	tenantSlot := store.taskTenantSlot(task.TenantID)
	tenantSlot <- struct{}{}

	runCtx, cancel := context.WithCancel(ctx)
	cancel()
	store.runTaskAdmitted(runCtx, cancel, task)
	<-tenantSlot

	loaded, err := store.GetIndexTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get index task: %v", err)
	}
	if loaded.Status != TaskStatusCanceled ||
		loaded.Phase != TaskStatusCanceled ||
		loaded.FinishedAt.IsZero() {
		t.Fatalf("canceled queued index task = %#v", loaded)
	}

}

func TestGetIndexTaskFailsAfterOwnerLeaseExpires(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	old := time.Now().UTC().Add(-time.Minute)
	task := IndexTask{
		ID:        "stale-index-task",
		TenantID:  "tenant-a",
		Type:      "rebuild",
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   "stopped-writer",
		StartedAt: old,
		UpdatedAt: old,
	}
	if err := store.saveIndexTask(ctx, task); err != nil {
		t.Fatalf("save index task: %v", err)
	}

	loaded, err := store.GetIndexTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get index task: %v", err)
	}
	if loaded.Status != TaskStatusFailed ||
		loaded.Phase != TaskStatusFailed ||
		loaded.Error != inactiveTaskError ||
		loaded.FinishedAt.IsZero() {
		t.Fatalf("recovered index task = %#v", loaded)
	}
	if _, err := store.getIndexRebuildRunningMarker(
		ctx,
		task.TenantID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get running marker err = %v, want ErrNotFound", err)
	}
}

func TestIndexRebuildUsesTaskCancellationAndRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	slot := store.taskTenantSlot("tenant-a")
	slot <- struct{}{}
	index, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		<-slot
		t.Fatal(err)
	}
	canceled, err := store.CancelTask(ctx, "tenant-a", index.ID)
	<-slot
	if err != nil || canceled.Status != TaskStatusCanceled {
		t.Fatalf("cancel: %+v, %v", canceled, err)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", index.ID)
	if err != nil {
		t.Fatal(err)
	}
	finished := waitForTask(t, ctx, store, "tenant-a", retry.ID)
	if finished.Status != TaskStatusSucceeded {
		t.Fatalf("retry: %+v", finished)
	}
	compat, err := store.GetIndexTask(ctx, "tenant-a", retry.ID)
	if err != nil || compat.Status != finished.Status {
		t.Fatalf("index status: %+v, %v", compat, err)
	}
}
