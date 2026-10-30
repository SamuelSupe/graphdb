package storage

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestIdentityEncodingPreservesLegacyReplayAndNewCommits(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	a := graph.Entity{ID: "first", Kind: "host", Fields: graph.Fields{"a": "foo|b=s:bar", "b": "baz"}}
	b := graph.Entity{ID: "second", Kind: "host", Fields: graph.Fields{"a": "foo", "b": "bar|b=s:baz"}}
	types := []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"a": {Type: "string"}, "b": {Type: "string"}}, IdentityKeys: []graph.IdentityKey{{Name: "pair", Fields: []string{"a", "b"}, Strategy: "merge"}}}}
	for _, tenant := range []string{"legacy", "current"} {
		writeCtx, encoding, want := ctx, graph.IdentityEncodingTuple, 2
		if tenant == "legacy" {
			writeCtx, encoding, want = LegacyIdentityContext(ctx), 0, 1
		}
		for i, entity := range []graph.Entity{a, b} {
			mutations := graph.Mutations{UpsertEntities: []graph.Entity{entity}}
			if i == 0 {
				mutations.UpsertCITypes = types
			}
			manifest, err := store.Commit(writeCtx, tenant, mutations, CommitOptions{})
			if err != nil {
				t.Fatal(err)
			}
			commit, err := store.getCommitObject(ctx, manifest.CommitKeys[len(manifest.CommitKeys)-1])
			if err != nil || commit.IdentityEncoding != encoding {
				t.Fatalf("%s stored encoding=%d err=%v", tenant, commit.IdentityEncoding, err)
			}
		}
		cold := NewTenantStore(objects, "test")
		g, _, err := cold.Load(ctx, tenant)
		if err != nil {
			t.Fatal(err)
		}
		if g.Entities.Len() != want {
			t.Fatalf("%s cold replay entities=%d want=%d", tenant, g.Entities.Len(), want)
		}
		// A new commit changes matching rules without changing old committed state.
		if _, err := cold.Commit(ctx, tenant, graph.Mutations{UpsertEntities: []graph.Entity{{ID: "third", Kind: "host", Fields: graph.Fields{"a": "third", "b": "third"}}}}, CommitOptions{}); err != nil {
			t.Fatal(err)
		}
		g, _, err = NewTenantStore(objects, "test").Load(ctx, tenant)
		if err != nil || g.Entities.Len() != want+1 {
			t.Fatalf("%s mixed replay err=%v", tenant, err)
		}
		if _, err := cold.Compact(ctx, tenant); err != nil {
			t.Fatal(err)
		}
		g, _, err = NewTenantStore(objects, "test").Load(ctx, tenant)
		if err != nil || g.Entities.Len() != want+1 {
			t.Fatalf("%s compacted replay err=%v", tenant, err)
		}
	}
}

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
