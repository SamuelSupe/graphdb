package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type blockingTaskPersistenceStore struct {
	ObjectStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingTaskPersistenceStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return nil, ObjectMeta{Key: key}, ctx.Err()
	case <-s.release:
		return s.ObjectStore.GetWithMeta(ctx, key)
	}
}

func TestQueuedTaskCancellationPersistenceHasDeadline(t *testing.T) {
	objects := &blockingTaskPersistenceStore{
		ObjectStore: NewMemoryStore(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	defer close(objects.release)
	store := NewTenantStore(objects, "test")
	store.TaskPersistenceTimeout = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		store.persistQueuedTaskCancellation(ctx, Task{
			ID:       "task-a",
			TenantID: "tenant-a",
			Type:     TaskTypeCompact,
			Status:   TaskStatusQueued,
		})
		close(done)
	}()
	select {
	case <-objects.started:
	case <-time.After(time.Second):
		t.Fatal("task cancellation persistence did not reach object storage")
	}
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task cancellation persistence exceeded its configured deadline")
	}
}

func TestStartTaskLockAcquisitionHonorsContext(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	unlock := store.lockTenant("tenant-a")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.StartTask(
			ctx,
			"tenant-a",
			TaskTypeExportSnapshot,
			nil,
		)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		unlock()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("start task err = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		unlock()
		<-done
		t.Fatal("task start ignored cancellation while waiting for tenant lock")
	}
}

func TestShutdownTasksCancelsQueuedWorkAndRejectsNewTasks(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	for range defaultTaskExecutionLimit {
		store.taskExecutionSlots <- struct{}{}
	}
	task, err := store.StartTask(
		ctx,
		"tenant-a",
		TaskTypeExportSnapshot,
		nil,
	)
	if err != nil {
		t.Fatalf("start queued task: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !store.taskRuntimeActive(task.TenantID, task.ID) {
		if time.Now().After(deadline) {
			t.Fatal("queued task runtime was not registered")
		}
		time.Sleep(time.Millisecond)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := store.ShutdownTasks(shutdownCtx); err != nil {
		t.Fatalf("shutdown tasks: %v", err)
	}
	loaded, err := store.GetTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get canceled task: %v", err)
	}
	if loaded.Status != TaskStatusCanceled {
		t.Fatalf("task status = %q, want canceled", loaded.Status)
	}
	if _, err := store.StartTask(
		ctx,
		"tenant-a",
		TaskTypeCompact,
		nil,
	); !errors.Is(err, ErrTaskServiceClosed) {
		t.Fatalf("start after shutdown err = %v, want ErrTaskServiceClosed", err)
	}
}

func TestShutdownTasksCancelsLegacyIndexRebuild(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	for range defaultTaskExecutionLimit {
		store.taskExecutionSlots <- struct{}{}
	}
	task, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start queued index rebuild: %v", err)
	}
	if !store.taskRuntimeActive(task.TenantID, task.ID) {
		t.Fatal("queued index task runtime was not registered")
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := store.ShutdownTasks(shutdownCtx); err != nil {
		t.Fatalf("shutdown tasks: %v", err)
	}
	loaded, err := store.GetIndexTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get stopped index task: %v", err)
	}
	if loaded.Status != TaskStatusFailed || loaded.FinishedAt.IsZero() {
		t.Fatalf("index task = %#v, want terminal failed state", loaded)
	}
	if _, err := store.StartIndexRebuild(ctx, "tenant-a"); !errors.Is(err, ErrTaskServiceClosed) {
		t.Fatalf("start index rebuild after shutdown err = %v, want ErrTaskServiceClosed", err)
	}
}

func TestTryAcquireMaintenanceBoundsTenantAndGlobalConcurrency(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	releaseTenant, err := store.TryAcquireMaintenance("tenant-a")
	if err != nil {
		t.Fatalf("acquire tenant-a: %v", err)
	}
	if _, err := store.TryAcquireMaintenance("tenant-a"); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatalf("second tenant-a acquire err = %v, want ErrMaintenanceBusy", err)
	}
	releaseTenant()

	tenantIDs := make([]string, 0, defaultTaskExecutionLimit+1)
	usedStripes := map[int]struct{}{}
	for i := 0; len(tenantIDs) < defaultTaskExecutionLimit+1; i++ {
		tenantID := fmt.Sprintf("tenant-%d", i)
		stripe := taskTenantStripe(tenantID, len(store.taskTenantSlots))
		if _, exists := usedStripes[stripe]; exists {
			continue
		}
		usedStripes[stripe] = struct{}{}
		tenantIDs = append(tenantIDs, tenantID)
	}
	releases := make([]func(), 0, defaultTaskExecutionLimit)
	for _, tenantID := range tenantIDs[:defaultTaskExecutionLimit] {
		release, err := store.TryAcquireMaintenance(tenantID)
		if err != nil {
			t.Fatalf("acquire %s: %v", tenantID, err)
		}
		releases = append(releases, release)
	}
	if _, err := store.TryAcquireMaintenance(tenantIDs[defaultTaskExecutionLimit]); !errors.Is(err, ErrMaintenanceBusy) {
		t.Fatalf("global overflow acquire err = %v, want ErrMaintenanceBusy", err)
	}
	for _, release := range releases {
		release()
	}
	if release, err := store.TryAcquireMaintenance(tenantIDs[defaultTaskExecutionLimit]); err != nil {
		t.Fatalf("acquire after release: %v", err)
	} else {
		release()
	}
}

func waitForTaskStatus(
	t *testing.T,
	store *TenantStore,
	tenantID string,
	taskID string,
	status string,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(context.Background(), tenantID, taskID)
		if err == nil && task.Status == status {
			return
		}
		time.Sleep(time.Millisecond)
	}
	task, err := store.GetTask(context.Background(), tenantID, taskID)
	t.Fatalf("task status = %q err=%v, want %q", task.Status, err, status)
}
