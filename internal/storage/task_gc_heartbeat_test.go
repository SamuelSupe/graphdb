package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

type blockingGCMarkerStore struct {
	ObjectStore
	key     string
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (s *blockingGCMarkerStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if key != s.key {
		return s.ObjectStore.GetWithMeta(ctx, key)
	}
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	close(s.exited)
	return nil, ObjectMeta{Key: key}, ctx.Err()
}

func TestGCTaskHeartbeatReadStopsWhenTaskIsCanceled(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.TaskMarkerTTL = 3 * time.Millisecond
	now := time.Now().UTC()
	task := Task{
		ID:        "gc-heartbeat-cancel",
		TenantID:  "tenant-a",
		Type:      TaskTypeGC,
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   store.InstanceID,
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save gc task: %v", err)
	}
	objects := &blockingGCMarkerStore{
		ObjectStore: base,
		key:         store.gcRunningTaskKey(task.TenantID),
		entered:     make(chan struct{}),
		exited:      make(chan struct{}),
	}
	store.Objects = objects
	runCtx, cancel := context.WithCancel(ctx)
	stop := store.startGCTaskHeartbeat(runCtx, task, func() {})
	defer stop()

	select {
	case <-objects.entered:
	case <-time.After(time.Second):
		t.Fatal("gc heartbeat did not start marker read")
	}
	cancel()
	select {
	case <-objects.exited:
	case <-time.After(time.Second):
		t.Fatal("gc heartbeat marker read ignored task cancellation")
	}
}
