package storage

import (
	"context"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestReaderCacheObserverRecordsHitMissAndVisibleVersion(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	observer := &testCacheObserver{}
	cache := NewReaderCache(store, time.Hour)
	cache.Observer = observer
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("first load: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("second load: %v", err)
	}
	if observer.cache["tenant-a\x00miss"] != 1 || observer.cache["tenant-a\x00hit"] != 1 {
		t.Fatalf("cache events = %#v", observer.cache)
	}
	if observer.visible["tenant-a"] != 1 {
		t.Fatalf("visible versions = %#v", observer.visible)
	}
}

func TestIndexObjectCacheObserverRecordsHitMiss(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})
	store.CacheObserver = &testCacheObserver{}
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if _, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"}); err != nil || !ok {
		t.Fatalf("first lookup ok=%v err=%v", ok, err)
	}
	if _, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-02"}); err != nil || !ok {
		t.Fatalf("second lookup ok=%v err=%v", ok, err)
	}
	observer := store.CacheObserver.(*testCacheObserver)
	if observer.cache["tenant-a\x00secondary_index_miss"] < 1 || observer.cache["tenant-a\x00secondary_index_hit"] < 1 {
		t.Fatalf("index cache events = %#v", observer.cache)
	}
}

type testCacheObserver struct {
	cache   map[string]int
	visible map[string]int64
}

func (o *testCacheObserver) RecordReaderCache(tenantID string, status string) {
	if o.cache == nil {
		o.cache = map[string]int{}
	}
	o.cache[tenantID+"\x00"+status]++
}

func (o *testCacheObserver) RecordReaderVisibleVersion(tenantID string, version int64) {
	if o.visible == nil {
		o.visible = map[string]int64{}
	}
	o.visible[tenantID] = version
}
