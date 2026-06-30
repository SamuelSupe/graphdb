package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"graphdb/internal/graph"
)

func TestConcurrentDirectCommitsAreSerializedAndAllVisible(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	const writers = 64
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{
					ID:     fmt.Sprintf("host:%03d", i),
					Kind:   "host",
					Fields: graph.Fields{"seq": i},
				}},
			}, CommitOptions{})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent commit failed: %v", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != writers {
		t.Fatalf("manifest version = %d, want %d", manifest.Version, writers)
	}
	for i := 0; i < writers; i++ {
		id := fmt.Sprintf("host:%03d", i)
		if _, ok := g.GetEntity(id); !ok {
			t.Fatalf("missing entity %q after concurrent commits", id)
		}
	}
}

func TestConcurrentIdempotentCommitsApplyOnce(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	const callers = 32
	mutations := graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:idempotent", Kind: "host", Fields: graph.Fields{"name": "same"}}},
	}
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{IdempotencyKey: "same-body"}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("idempotent commit failed: %v", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:idempotent"); !ok {
		t.Fatal("idempotent entity missing")
	}
}

func TestReaderLoadsRemainConsistentDuringConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	reader := NewTenantStore(objects, "test")
	const commits = 40
	errs := make(chan error, commits+1)
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, _, err := reader.Load(context.Background(), "tenant-a"); err != nil {
				errs <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	for i := 0; i < commits; i++ {
		if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: fmt.Sprintf("host:%02d", i), Kind: "host"}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	close(stop)
	<-done
	close(errs)
	for err := range errs {
		t.Fatalf("reader load failed during writes: %v", err)
	}
	g, manifest, err := reader.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("final reader load: %v", err)
	}
	if manifest.Version != commits {
		t.Fatalf("final reader version = %d, want %d", manifest.Version, commits)
	}
	for i := 0; i < commits; i++ {
		if _, ok := g.GetEntity(fmt.Sprintf("host:%02d", i)); !ok {
			t.Fatalf("missing host:%02d in final reader view", i)
		}
	}
}
