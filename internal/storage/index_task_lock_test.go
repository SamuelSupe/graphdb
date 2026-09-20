package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

var errIndexTaskMarkerProbe = errors.New("index task marker probe failed")

type blockingIndexTaskMarkerStore struct {
	ObjectStore
	key     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingIndexTaskMarkerStore) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	if key != s.key {
		return s.ObjectStore.Get(ctx, key)
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return nil, errIndexTaskMarkerProbe
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestIndexTaskMarkerIODoesNotBlockOtherTaskAdmission(t *testing.T) {
	objects := &blockingIndexTaskMarkerStore{
		ObjectStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	objects.key = store.indexRebuildRunningTaskKey("tenant-a")
	startDone := make(chan error, 1)
	go func() {
		_, err := store.startIndexRebuild(
			context.Background(),
			"tenant-a",
			true,
		)
		startDone <- err
	}()
	select {
	case <-objects.entered:
	case <-time.After(time.Second):
		t.Fatal("index task marker read did not start")
	}

	admissionDone := make(chan error, 1)
	task := Task{
		ID:       "task-b",
		TenantID: "tenant-b",
		Type:     TaskTypeCompact,
	}
	go func() {
		_, _, err := store.admitTask(context.Background(), task)
		admissionDone <- err
	}()
	select {
	case err := <-admissionDone:
		if err != nil {
			t.Fatalf("admit unrelated task: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated task admission blocked behind marker I/O")
	}
	store.releaseTaskAdmission(task)
	close(objects.release)
	if err := <-startDone; !errors.Is(err, errIndexTaskMarkerProbe) {
		t.Fatalf("start index task err = %v, want marker probe error", err)
	}
}

func TestIndexTaskStartSlotHonorsContext(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	slot := store.indexTaskStartSlot("tenant-a")
	slot <- struct{}{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.startIndexRebuild(ctx, "tenant-a", true)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		<-slot
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("start index task err = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		<-slot
		<-done
		t.Fatal("index task start ignored cancellation while waiting for its slot")
	}
}

func TestIndexCleanupReleasesMaintenanceExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := newBlockingTenantDeleteStore(files, "")
	store := NewTenantStore(objects, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	objects.prefix = store.reverseIndexPrefix("tenant-a")
	if err := files.Put(ctx, objects.prefix+"orphan.parquet", []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	entered, resume := objects.blockNextDelete()
	resumeCleanup := sync.OnceFunc(func() { close(resume) })
	defer resumeCleanup()
	task, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	release, err := store.TryAcquireMaintenance("tenant-a")
	if err != nil {
		t.Fatalf("index cleanup blocked other maintenance: %v", err)
	}
	release()
	resumeCleanup()
	finished := waitForIndexTaskStatus(t, ctx, store, "tenant-a", task.ID)
	if finished.Status != TaskStatusSucceeded {
		t.Fatalf("index task = %+v", finished)
	}
}
