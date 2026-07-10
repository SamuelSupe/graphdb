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
