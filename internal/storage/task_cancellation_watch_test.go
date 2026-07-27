package storage

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingTaskCancellationStore struct {
	ObjectStore
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (s *blockingTaskCancellationStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if !strings.Contains(key, "/tasks/") {
		return s.ObjectStore.GetWithMeta(ctx, key)
	}
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	close(s.exited)
	return nil, ObjectMeta{}, ctx.Err()
}

func TestTaskCancellationWatchStopsBlockedObjectRead(t *testing.T) {
	objects := &blockingTaskCancellationStore{
		ObjectStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		exited:      make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	stop := store.watchTaskCancellation(
		Task{ID: "task-a", TenantID: "tenant-a"},
		func() {},
	)
	select {
	case <-objects.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("task cancellation watch did not start object read")
	}

	stop()
	select {
	case <-objects.exited:
	case <-time.After(time.Second):
		t.Fatal("stopped task cancellation watch left object read blocked")
	}
}

func TestTaskRunnerInitialCancelCheckStopsBlockedObjectRead(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	seed := NewTenantStore(base, "test")
	now := time.Now().UTC()
	task := Task{
		ID:        "task-blocked-initial-cancel-check",
		TenantID:  "tenant-a",
		Type:      TaskTypeExportSnapshot,
		Status:    TaskStatusQueued,
		Phase:     TaskStatusQueued,
		OwnerID:   seed.InstanceID,
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := seed.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}

	objects := &blockingTaskCancellationStore{
		ObjectStore: base,
		entered:     make(chan struct{}),
		exited:      make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		store.runTask(runCtx, cancel, task)
		close(done)
	}()

	select {
	case <-objects.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("task runner did not start initial cancellation read")
	}
	cancel()
	select {
	case <-objects.exited:
	case <-time.After(time.Second):
		t.Fatal("task runner initial cancellation read ignored cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task runner remained blocked after cancellation")
	}
}
