package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGetTaskFailsInactiveLocalOwnerAfterRecoveryGrace(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.TaskMarkerTTL = time.Millisecond
	now := time.Now().UTC().Add(-time.Minute)
	task := Task{
		ID:        "stale-local-task",
		TenantID:  "tenant-a",
		Type:      TaskTypeCompact,
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   store.InstanceID,
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	loaded, err := store.GetTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != TaskStatusFailed ||
		loaded.Error != inactiveTaskError ||
		loaded.FinishedAt.IsZero() {
		t.Fatalf("recovered task = %#v", loaded)
	}
}

func TestGetTaskKeepsActivePostgresQueueOwner(t *testing.T) {
	ctx := context.Background()
	coordinator := newTaskLeaseTestCoordinator()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	store.TaskMarkerTTL = time.Millisecond
	now := time.Now().UTC().Add(-time.Minute)
	task := Task{
		ID:        "active-queued-task",
		TenantID:  "tenant-a",
		Type:      TaskTypeCompact,
		Status:    TaskStatusQueued,
		Phase:     TaskStatusQueued,
		OwnerID:   "remote-writer",
		StartedAt: now,
		UpdatedAt: now,
	}
	if _, acquired, err := coordinator.AcquireTaskLease(
		ctx,
		task.TenantID,
		coordinatorQueuedTaskLeaseType(task),
		task.OwnerID+"/"+task.ID,
		time.Minute,
	); err != nil || !acquired {
		t.Fatalf("acquire queue lease: acquired=%v err=%v", acquired, err)
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	loaded, err := store.GetTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != TaskStatusQueued {
		t.Fatalf("active queued task status = %q, want queued", loaded.Status)
	}
}

func TestGetTaskFailsPostgresTaskAfterOwnerLeasesExpire(t *testing.T) {
	ctx := context.Background()
	coordinator := newTaskLeaseTestCoordinator()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	store.TaskMarkerTTL = time.Millisecond
	now := time.Now().UTC().Add(-time.Minute)
	task := Task{
		ID:        "stale-postgres-task",
		TenantID:  "tenant-a",
		Type:      TaskTypeCompact,
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   "dead-writer",
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	loaded, err := store.GetTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != TaskStatusFailed ||
		loaded.Error != inactiveTaskError {
		t.Fatalf("recovered task = %#v", loaded)
	}
}

func TestRunGCCleansExpiredTaskAfterOwnerStops(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.TaskMarkerTTL = time.Millisecond
	old := time.Now().UTC().Add(-2 * time.Hour)
	task := Task{
		ID:        "expired-orphan-task",
		TenantID:  "tenant-a",
		Type:      TaskTypeExportSnapshot,
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   "stopped-writer",
		ResultKey: store.taskResultKey("tenant-a", "expired-orphan-task"),
		StartedAt: old,
		UpdatedAt: old,
	}
	if err := store.putTaskResult(
		ctx,
		task.TenantID,
		task.ID,
		map[string]any{"orphaned": true},
	); err != nil {
		t.Fatalf("put task result: %v", err)
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	indexTask := IndexTask{
		ID:        "expired-orphan-index-task",
		TenantID:  task.TenantID,
		Type:      "rebuild",
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   "stopped-writer",
		StartedAt: old,
		UpdatedAt: old,
	}
	if err := store.saveIndexTask(ctx, indexTask); err != nil {
		t.Fatalf("save index task: %v", err)
	}

	report, err := store.RunGC(
		ctx,
		task.TenantID,
		GCOptions{TaskMaxAge: time.Hour},
	)
	if err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if report.DeletedTasks != 1 ||
		report.DeletedTaskResults != 1 ||
		report.DeletedIndexTasks != 1 {
		t.Fatalf(
			"gc report = %#v, want orphaned task, result, and index task deleted",
			report,
		)
	}
	if _, err := store.Objects.Get(
		ctx,
		store.taskKey(task.TenantID, task.ID),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted task err = %v, want ErrNotFound", err)
	}
	if _, err := store.Objects.Get(
		ctx,
		store.indexTaskKey(task.TenantID, indexTask.ID),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted index task err = %v, want ErrNotFound", err)
	}
}
