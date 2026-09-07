package storage

import (
	"context"
	"strconv"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestReaderCacheReadOnlyViewReusesSnapshotAndKeepsLoadIsolated(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	cache := NewReaderCache(store, time.Minute)
	var first *graph.Graph
	if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", 1, func(g *graph.Graph, _ Manifest) error {
		first = g
		return nil
	}); err != nil {
		t.Fatalf("first view: %v", err)
	}
	if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", 1, func(g *graph.Graph, _ Manifest) error {
		if g != first {
			t.Fatal("hot read-only view cloned the cached graph")
		}
		return nil
	}); err != nil {
		t.Fatalf("second view: %v", err)
	}
	owned, _, err := cache.LoadAtLeast(ctx, "tenant-a", 1)
	if err != nil {
		t.Fatalf("owned load: %v", err)
	}
	if owned == first {
		t.Fatal("public Load returned the shared cache snapshot")
	}
	delete(owned.Entities, "person:alice")
	if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", 1, func(g *graph.Graph, _ Manifest) error {
		if _, ok := g.GetEntity("person:alice"); !ok {
			t.Fatal("mutation of public Load result reached cache snapshot")
		}
		return nil
	}); err != nil {
		t.Fatalf("view after owned mutation: %v", err)
	}
}

func TestReaderCacheCachedReadOnlyGraphMissDoesNotLoad(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	cache := NewReaderCache(store, time.Minute)

	called := false
	used, err := cache.WithCachedReadOnlyGraph(ctx, "tenant-a", 1, func(*graph.Graph, Manifest) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("cold cached view: %v", err)
	}
	if used || called {
		t.Fatalf("cold cached view used=%v callback=%v, want miss without callback", used, called)
	}
	if reads := objects.CountContains(""); reads != 0 {
		t.Fatalf("cold cached view performed %d object reads, want none", reads)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance manifest: %v", err)
	}
	objects.Reset()
	used, err = cache.WithCachedReadOnlyGraph(ctx, "tenant-a", 2, func(*graph.Graph, Manifest) error {
		t.Fatal("stale cached view invoked callback")
		return nil
	})
	if err != nil {
		t.Fatalf("stale cached view: %v", err)
	}
	if used {
		t.Fatal("stale cached view reported a hit")
	}
	if reads := objects.CountContains(""); reads != 0 {
		t.Fatalf("stale cached view performed %d object reads, want none", reads)
	}
}

func TestReaderCacheReusesExactWriteCacheForCatchupAndKeepsPublicResultsIsolated(t *testing.T) {
	ctx := context.Background()
	objects, store, cache, _ := newReaderCacheWriteReuseFixture(t)
	objects.Reset()

	loaded, manifest, err := cache.LoadAtLeast(ctx, "tenant-a", 2)
	if err != nil {
		t.Fatalf("load v2: %v", err)
	}
	if manifest.Version != 2 || loaded.Version != 2 {
		t.Fatalf("loaded graph/manifest = %d/%d, want 2/2", loaded.Version, manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:b"); !ok {
		t.Fatal("loaded v2 graph is missing host:b")
	}
	if got := objects.CountContains("/commits/") + objects.CountContains("/snapshots/"); got != 0 {
		t.Fatalf("exact write-cache catch-up read %d commit/snapshot objects, want none", got)
	}
	if got := objects.CountContains("manifest.parquet"); got == 0 {
		t.Fatal("exact write-cache catch-up skipped the authoritative manifest read")
	}
	delete(loaded.Entities, "host:b")
	assertReaderCacheEntityPresent(t, ctx, cache, store, "host:b", 2)

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v3: %v", err)
	}
	objects.Reset()
	refreshed, manifest, err := cache.Refresh(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("refresh v3: %v", err)
	}
	if manifest.Version != 3 || refreshed.Version != 3 {
		t.Fatalf("refreshed graph/manifest = %d/%d, want 3/3", refreshed.Version, manifest.Version)
	}
	if _, ok := refreshed.GetEntity("host:c"); !ok {
		t.Fatal("refreshed v3 graph is missing host:c")
	}
	if got := objects.CountContains("/commits/") + objects.CountContains("/snapshots/"); got != 0 {
		t.Fatalf("exact write-cache refresh read %d commit/snapshot objects, want none", got)
	}
	if got := objects.CountContains("manifest.parquet"); got == 0 {
		t.Fatal("exact write-cache refresh skipped the authoritative manifest read")
	}
	delete(refreshed.Entities, "host:c")
	assertReaderCacheEntityPresent(t, ctx, cache, store, "host:c", 3)
}

func TestReaderCacheWriteCacheReuseRequiresExactManifest(t *testing.T) {
	for _, tc := range []struct {
		name string
		prep func(context.Context, *TenantStore, loadedGraph) error
	}{
		{
			name: "old-version",
			prep: func(_ context.Context, store *TenantStore, old loadedGraph) error {
				store.deleteWriteCache("tenant-a")
				store.setWriteCache("tenant-a", old)
				return nil
			},
		},
		{
			name: "etag-mismatch",
			prep: func(ctx context.Context, store *TenantStore, _ loadedGraph) error {
				manifest, meta, err := store.getManifest(ctx, "tenant-a")
				if err != nil {
					return err
				}
				manifest.UpdatedAt = manifest.UpdatedAt.Add(time.Second)
				_, err = store.putManifestMeta(ctx, "tenant-a", manifest, meta)
				return err
			},
		},
		{
			name: "missing-write-cache",
			prep: func(_ context.Context, store *TenantStore, _ loadedGraph) error {
				store.deleteWriteCache("tenant-a")
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			objects, store, cache, old := newReaderCacheWriteReuseFixture(t)
			if err := tc.prep(ctx, store, old); err != nil {
				t.Fatalf("prepare %s: %v", tc.name, err)
			}
			objects.Reset()
			loaded, manifest, err := cache.LoadAtLeast(ctx, "tenant-a", 2)
			if err != nil {
				t.Fatalf("load after %s: %v", tc.name, err)
			}
			if manifest.Version != 2 || loaded.Version != 2 {
				t.Fatalf("loaded graph/manifest = %d/%d, want 2/2", loaded.Version, manifest.Version)
			}
			if _, ok := loaded.GetEntity("host:b"); !ok {
				t.Fatalf("fallback after %s lost host:b", tc.name)
			}
			if got := objects.CountContains("/commits/") + objects.CountContains("/snapshots/"); got == 0 {
				t.Fatalf("%s bypassed persistent validation reads", tc.name)
			}
			if got := objects.CountContains("manifest.parquet"); got == 0 {
				t.Fatalf("%s skipped the authoritative manifest read", tc.name)
			}
		})
	}
}

func newReaderCacheWriteReuseFixture(t *testing.T) (*countingReadStore, *TenantStore, *ReaderCache, loadedGraph) {
	t.Helper()
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	cache := NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, _, err := cache.LoadAtLeast(ctx, "tenant-a", 1); err != nil {
		t.Fatalf("warm v1 cache: %v", err)
	}
	old, ok := store.getWriteCache("tenant-a")
	if !ok || old.Manifest.Version != 1 {
		t.Fatalf("v1 write cache = %#v, want version 1", old)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	return objects, store, cache, old
}

func assertReaderCacheEntityPresent(t *testing.T, ctx context.Context, cache *ReaderCache, store *TenantStore, entityID string, minVersion int64) {
	t.Helper()
	if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", minVersion, func(g *graph.Graph, _ Manifest) error {
		if _, ok := g.GetEntity(entityID); !ok {
			t.Fatalf("shared reader cache lost %s", entityID)
		}
		return nil
	}); err != nil {
		t.Fatalf("shared reader cache read: %v", err)
	}
	loaded, manifest, err := store.LoadAtLeast(ctx, "tenant-a", minVersion)
	if err != nil {
		t.Fatalf("write cache read: %v", err)
	}
	if manifest.Version < minVersion {
		t.Fatalf("write cache manifest version = %d, want >= %d", manifest.Version, minVersion)
	}
	if _, ok := loaded.GetEntity(entityID); !ok {
		t.Fatalf("write cache graph lost %s", entityID)
	}
}

func BenchmarkReaderCacheHotRead(b *testing.B) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	entities := make([]graph.Entity, 2000)
	for i := range entities {
		entities[i] = graph.Entity{ID: "host:" + strconv.Itoa(i), Kind: "host", Fields: graph.Fields{"region": "ap-southeast-1"}}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		b.Fatalf("commit: %v", err)
	}
	cache := NewReaderCache(store, time.Hour)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		b.Fatalf("warm cache: %v", err)
	}
	b.Run("isolated-load", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("read-only-view", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := cache.WithReadOnlyGraphAtLeast(ctx, "tenant-a", 0, func(*graph.Graph, Manifest) error { return nil }); err != nil {
				b.Fatal(err)
			}
		}
	})
}
