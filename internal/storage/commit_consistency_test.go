package storage

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"graphdb/internal/graph"
)

func TestCommitDoesNotExposeUnpublishedGraphThroughWriteCache(t *testing.T) {
	ctx := context.Background()
	objects := newBlockingManifestStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	entered, release := objects.blockNextManifest()
	errCh := make(chan error, 1)
	go func() {
		_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
		}, CommitOptions{})
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("commit did not reach blocked manifest publish")
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load while publish blocked: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want old version 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("unpublished entity leaked through write cache")
	}
	close(release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("commit did not finish")
	}
	g, manifest, err = store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load after publish: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("published entity missing after manifest publish")
	}
}

func TestCommitObjectWriteUsesConditionalCreateAndRetriesCollision(t *testing.T) {
	ctx := context.Background()
	objects := &conflictOnceCommitObjectStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	manifest, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !objects.Triggered() {
		t.Fatal("commit object was not written with If-None-Match")
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	g, loaded, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Version != 1 {
		t.Fatalf("loaded version = %d, want 1", loaded.Version)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("entity missing after commit retry")
	}
}

type conflictOnceCommitObjectStore struct {
	ObjectStore
	mu        sync.Mutex
	triggered bool
}

func (s *conflictOnceCommitObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.Contains(key, "/commits/") && condition.IfNoneMatch && s.markTriggered() {
		return ObjectMeta{Key: key}, ErrConflict
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *conflictOnceCommitObjectStore) markTriggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *conflictOnceCommitObjectStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

type blockingManifestStore struct {
	ObjectStore
	mu      sync.Mutex
	block   bool
	entered chan struct{}
	release chan struct{}
}

func newBlockingManifestStore(base ObjectStore) *blockingManifestStore {
	return &blockingManifestStore{ObjectStore: base}
}

func (s *blockingManifestStore) blockNextManifest() (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = true
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
	return s.entered, s.release
}

func (s *blockingManifestStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	entered, release, shouldBlock := s.takeBlock(key)
	if shouldBlock {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ObjectMeta{}, ctx.Err()
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *blockingManifestStore) takeBlock(key string) (chan struct{}, chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.block || !strings.HasSuffix(key, "/manifest.parquet") {
		return nil, nil, false
	}
	s.block = false
	return s.entered, s.release, true
}
