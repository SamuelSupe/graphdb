package storage

import (
	"context"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIndexHealthUsesBoundedObjectPages(t *testing.T) {
	ctx := context.Background()
	paged := &pagingOnlyStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(paged, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:app", Kind: "host",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if _, err := store.IndexHealth(ctx, "tenant-a"); err != nil {
		t.Fatalf("index health: %v", err)
	}
	if paged.listCalls != 0 {
		t.Fatalf("unbounded list calls=%d, want 0", paged.listCalls)
	}
	if paged.pageCalls == 0 {
		t.Fatal("index health did not use bounded object pages")
	}
}

func BenchmarkIndexHealth10K(b *testing.B) {
	ctx := context.Background()
	files, err := OpenFileStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "bench")
	store.WriteEntityRecords = false
	entities := make([]graph.Entity, 10_000)
	edges := make([]graph.Edge, 5_000)
	for i := range entities {
		id := fmt.Sprintf("host:%05d", i)
		entities[i] = graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{
			"hostname": id, "region": "east", "sequence": i,
			"meta": map[string]any{"tags": []any{"active", i}},
		}}
	}
	for i := range edges {
		edges[i] = graph.Edge{Type: "links", From: entities[i*2].ID, To: entities[i*2+1].ID,
			Fields: graph.Fields{"weight": i}}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{
			"region": {Type: "string", Indexed: true},
		}}},
		UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
		UpsertEntities:      entities, UpsertEdges: edges,
	}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		b.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		b.Fatal(err)
	}
	if health, err := store.IndexHealth(ctx, "tenant-a"); err != nil || health.Status != "ready" {
		b.Fatalf("initial health = %+v, err = %v", health, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if health, err := store.IndexHealth(ctx, "tenant-a"); err != nil || health.Status != "ready" {
			b.Fatalf("health = %+v, err = %v", health, err)
		}
	}
}
