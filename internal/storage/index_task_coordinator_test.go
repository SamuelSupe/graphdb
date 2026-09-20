package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIndexTaskStartDoesNotReuseStaleProcessCache(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	old := time.Now().UTC().Add(-time.Hour)
	stale := IndexTask{
		ID:         "stale-cached-task",
		TenantID:   "tenant-a",
		Type:       "rebuild",
		Status:     TaskStatusSucceeded,
		Phase:      "done",
		OwnerID:    "remote-writer",
		StartedAt:  old,
		UpdatedAt:  old,
		FinishedAt: old,
	}
	if err := store.saveIndexTask(ctx, stale); err != nil {
		t.Fatalf("save completed index task: %v", err)
	}
	stale.Status = TaskStatusRunning
	stale.Phase = TaskStatusRunning
	stale.FinishedAt = time.Time{}
	store.taskMu.Lock()
	store.indexTasks[stale.TenantID] = stale
	store.taskMu.Unlock()
	for range defaultTaskExecutionLimit {
		store.taskExecutionSlots <- struct{}{}
	}
	defer func() {
		for range defaultTaskExecutionLimit {
			<-store.taskExecutionSlots
		}
	}()

	started, err := store.StartIndexRebuild(ctx, stale.TenantID)
	if err != nil {
		t.Fatalf("start index task: %v", err)
	}
	if started.ID == stale.ID {
		t.Fatalf("reused stale process-cached index task %q", stale.ID)
	}
}

func TestCanceledIndexTaskAdmissionPersistsTerminalStatus(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := IndexTask{
		ID:            "canceled-before-execution",
		TenantID:      "tenant-a",
		Type:          "rebuild",
		Status:        TaskStatusRunning,
		Phase:         TaskStatusQueued,
		ProgressTotal: 1,
		OwnerID:       store.InstanceID,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := store.publishQueuedIndexTask(ctx, task); err != nil {
		t.Fatalf("publish queued index task: %v", err)
	}
	store.taskMu.Lock()
	store.indexTasks[task.TenantID] = task
	store.taskMu.Unlock()
	if !store.reserveQueuedTask() {
		t.Fatal("reserve queued task")
	}
	tenantSlot := store.taskTenantSlot(task.TenantID)
	tenantSlot <- struct{}{}

	runCtx, cancel := context.WithCancel(ctx)
	cancel()
	store.runIndexTaskAdmitted(runCtx, task.TenantID, task)
	<-tenantSlot

	loaded, err := store.GetIndexTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get index task: %v", err)
	}
	if loaded.Status != TaskStatusFailed ||
		loaded.Phase != TaskStatusFailed ||
		loaded.FinishedAt.IsZero() {
		t.Fatalf("canceled queued index task = %#v", loaded)
	}
	if _, err := store.getIndexRebuildRunningMarker(
		ctx,
		task.TenantID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get running marker err = %v, want ErrNotFound", err)
	}
}

func TestGetIndexTaskFailsAfterOwnerLeaseExpires(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Millisecond
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
