package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestStaleGCRunningMarkerDoesNotBlockWrites(t *testing.T) {
	ctx := context.Background()
	objects := newCountingListStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	store.TaskMarkerTTL = 5 * time.Millisecond
	store.Backpressure = NewWritePressure(BackpressureConfig{})
	old := time.Now().UTC().Add(-time.Hour)
	task := Task{
		ID: "stale-gc", TenantID: "tenant-a", Type: TaskTypeGC,
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		OwnerID: "crashed-owner", StartedAt: old, UpdatedAt: old,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save stale task: %v", err)
	}

	objects.reset()
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit after stale gc: %v", err)
	}
	if got := objects.count(store.taskPrefix("tenant-a")); got != 0 {
		t.Fatalf("historical task list count = %d, want 0", got)
	}
	failed, err := store.GetTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get stale task: %v", err)
	}
	if failed.Status != TaskStatusFailed || !strings.Contains(failed.Error, "heartbeat expired") {
		t.Fatalf("stale task = %#v", failed)
	}
}

func TestStaleGCMarkerRecoversMissingTaskHistory(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.TaskMarkerTTL = 5 * time.Millisecond
	old := time.Now().UTC().Add(-time.Hour)
	marker := Task{
		ID: "orphaned-gc", TenantID: "tenant-a", Type: TaskTypeGC,
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		OwnerID: "crashed-owner", StartedAt: old, UpdatedAt: old,
	}
	if _, err := store.putGCRunningMarker(ctx, marker, ObjectMeta{Key: store.gcRunningTaskKey("tenant-a")}); err != nil {
		t.Fatalf("write orphaned marker: %v", err)
	}
	if _, found, err := store.findRunningGCTask(ctx, "tenant-a"); err != nil || found {
		t.Fatalf("find stale marker found=%v err=%v", found, err)
	}
	recovered, err := store.GetTask(ctx, "tenant-a", marker.ID)
	if err != nil {
		t.Fatalf("get recovered task: %v", err)
	}
	if recovered.Status != TaskStatusFailed || !strings.Contains(recovered.Error, "heartbeat expired") {
		t.Fatalf("recovered task = %#v", recovered)
	}
}

func TestTaskProgressCannotOverwriteConcurrentCancel(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newBlockingTaskPutStore(base)
	runner := NewTenantStore(objects, "test")
	now := time.Now().UTC()
	task := Task{
		ID: "task-race", TenantID: "tenant-a", Type: TaskTypeCompact,
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		StartedAt: now, UpdatedAt: now,
	}
	if err := runner.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}

	entered, release := objects.blockNextPut(runner.taskKey("tenant-a", task.ID))
	progressDone := make(chan error, 1)
	go func() {
		progressDone <- runner.updateTaskProgress(ctx, task, "compact", 1, 2, map[string]any{"step": "snapshot"})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("progress update did not reach conditional put")
	}

	canceller := NewTenantStore(base, "test")
	if _, err := canceller.CancelTask(ctx, "tenant-a", task.ID); err != nil {
		t.Fatalf("cancel task: %v", err)
	}
	close(release)
	if err := <-progressDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("progress err = %v, want context.Canceled", err)
	}
	current, err := canceller.GetTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get canceled task: %v", err)
	}
	if current.Status != TaskStatusCanceled || current.Phase != TaskStatusCanceled {
		t.Fatalf("task after race = %#v", current)
	}
}

type blockingTaskPutStore struct {
	ObjectStore
	mu      sync.Mutex
	key     string
	block   bool
	entered chan struct{}
	release chan struct{}
}

func newBlockingTaskPutStore(inner ObjectStore) *blockingTaskPutStore {
	return &blockingTaskPutStore{ObjectStore: inner}
}

func (s *blockingTaskPutStore) blockNextPut(key string) (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.key = key
	s.block = true
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
	return s.entered, s.release
}

func (s *blockingTaskPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	s.mu.Lock()
	block := s.block && key == s.key
	if block {
		s.block = false
	}
	entered, release := s.entered, s.release
	s.mu.Unlock()
	if block {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ObjectMeta{}, ctx.Err()
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}
