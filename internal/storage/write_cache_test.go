package storage

import (
	"context"
	"strings"
	"sync"
	"testing"

	"graphdb/internal/graph"
)

func TestWriteCacheEvictsLeastRecentlyUsedTenant(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	store.MaxWriteCacheTenants = 2
	store.setWriteCache("tenant-a", cachedGraph(1))
	store.setWriteCache("tenant-b", cachedGraph(1))
	if _, ok := store.getWriteCache("tenant-a"); !ok {
		t.Fatal("tenant-a missing before LRU refresh")
	}
	store.setWriteCache("tenant-c", cachedGraph(1))

	if _, ok := store.getWriteCache("tenant-b"); ok {
		t.Fatal("least recently used tenant-b was not evicted")
	}
	for _, tenantID := range []string{"tenant-a", "tenant-c"} {
		if _, ok := store.getWriteCache(tenantID); !ok {
			t.Fatalf("recent tenant %q was evicted", tenantID)
		}
	}
}

func TestDeleteWriteCacheRemovesLRUState(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	store.MaxWriteCacheTenants = 1
	store.setWriteCache("tenant-a", cachedGraph(1))
	store.deleteWriteCache("tenant-a")
	store.setWriteCache("tenant-b", cachedGraph(1))

	if len(store.writeCacheOrder) != 1 || store.writeCacheOrder[0] != "tenant-b" {
		t.Fatalf("write cache order = %#v, want only tenant-b", store.writeCacheOrder)
	}
	if _, ok := store.getWriteCache("tenant-a"); ok {
		t.Fatal("deleted tenant remains cached")
	}
}

func TestWriteCacheEvictsLeastRecentlyUsedTenantByMemory(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	store.MaxWriteCacheTenants = 10
	store.MaxWriteCacheBytes = 100
	first := cachedGraph(1)
	first.CacheBytes = 60
	second := cachedGraph(1)
	second.CacheBytes = 60
	store.setWriteCache("tenant-a", first)
	store.setWriteCache("tenant-b", second)

	if _, ok := store.getWriteCache("tenant-a"); ok {
		t.Fatal("least recently used tenant-a was not evicted by byte limit")
	}
	if _, ok := store.getWriteCache("tenant-b"); !ok {
		t.Fatal("tenant-b missing after byte-limit eviction")
	}
	if store.writeCacheBytes != 60 {
		t.Fatalf("write cache bytes = %d, want 60", store.writeCacheBytes)
	}
}

func TestWriteCacheRejectsOversizedGraphAndAccountsReplacement(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	store.MaxWriteCacheTenants = 10
	store.MaxWriteCacheBytes = 100
	loaded := cachedGraph(1)
	loaded.CacheBytes = 80
	store.setWriteCache("tenant-a", loaded)
	loaded.CacheBytes = 30
	store.setWriteCache("tenant-a", loaded)
	if store.writeCacheBytes != 30 {
		t.Fatalf("replacement cache bytes = %d, want 30", store.writeCacheBytes)
	}

	oversized := cachedGraph(2)
	oversized.CacheBytes = 101
	store.setWriteCache("tenant-b", oversized)
	if _, ok := store.getWriteCache("tenant-b"); ok {
		t.Fatal("oversized graph should not be retained")
	}
	store.deleteWriteCache("tenant-a")
	if store.writeCacheBytes != 0 {
		t.Fatalf("cache bytes after delete = %d, want 0", store.writeCacheBytes)
	}
}

func TestExpectedVersionMismatchDoesNotLoadCommitHistory(t *testing.T) {
	ctx := context.Background()
	objects := newPathCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	for version := 1; version <= 4; version++ {
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:a", Kind: "host", Fields: graph.Fields{"version": version},
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", version, err)
		}
	}
	store.deleteWriteCache("tenant-a")
	objects.reset()

	expected := int64(0)
	_, err := store.commitOnceLocked(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:b", Kind: "host",
	}}}, CommitOptions{ExpectedVersion: &expected})
	if err == nil || !strings.Contains(err.Error(), "expected version 0, current version 4") {
		t.Fatalf("error = %v, want expected-version mismatch", err)
	}
	if got := objects.countContains("/commits/"); got != 0 {
		t.Fatalf("commit history reads = %d, want 0", got)
	}
	if got := objects.countContains("/manifest.parquet"); got != 1 {
		t.Fatalf("manifest reads = %d, want 1", got)
	}
}

func TestWriteCacheRetainsComputedContentHash(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"state": "ready"},
	}}}
	first, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	cached, ok := store.getWriteCache("tenant-a")
	if !ok || cached.DataMD5 == "" || cached.DataMD5 != first.DataMD5 {
		t.Fatalf("cached hash = %q, result hash = %q", cached.DataMD5, first.DataMD5)
	}
	if cached.CacheBytes < minimumWriteCacheBytes {
		t.Fatalf("cached memory weight = %d, want at least %d", cached.CacheBytes, minimumWriteCacheBytes)
	}
	second, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("skipped commit: %v", err)
	}
	if !second.Skipped || second.DataMD5 != first.DataMD5 {
		t.Fatalf("second result = %#v, want skipped with retained hash", second)
	}
}

func cachedGraph(version int64) loadedGraph {
	g := graph.New()
	g.Version = version
	return loadedGraph{Graph: g, Manifest: Manifest{Version: version}}
}

type pathCountingStore struct {
	ObjectStore
	mu    sync.Mutex
	reads []string
}

func newPathCountingStore(objects ObjectStore) *pathCountingStore {
	return &pathCountingStore{ObjectStore: objects}
}

func (s *pathCountingStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.record(key)
	return s.ObjectStore.Get(ctx, key)
}

func (s *pathCountingStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.record(key)
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *pathCountingStore) record(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, key)
}

func (s *pathCountingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = nil
}

func (s *pathCountingStore) countContains(fragment string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.reads {
		if strings.Contains(key, fragment) {
			count++
		}
	}
	return count
}
