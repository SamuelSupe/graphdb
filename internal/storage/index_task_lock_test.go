package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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
		_, _, err := store.admitTask(task)
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
