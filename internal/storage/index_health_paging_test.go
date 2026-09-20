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

func TestHealthBucketsPreserveCurrentAndLegacyShards(t *testing.T) {
	id := "host:legacy"
	for i := 0; hashedIndexShardID(id) == legacyIndexShardID(id); i++ {
		id = fmt.Sprintf("host:legacy-%d", i)
	}
	g := graph.New()
	g.Entities[id] = graph.Entity{ID: id, Kind: "host"}
	g.Edges["edge:a"] = graph.Edge{ID: "edge:a", Type: "links", From: id, To: "host:other"}
	current, legacy := hashedIndexShardID(id), legacyIndexShardID(id)
	pages := expectedEntityPages(g, []EntityPageSpec{{Shard: current}, {Shard: legacy}, {Shard: "missing"}})
	edges := expectedEdgeShards(g, []EdgeShard{{RelationType: "links", Shard: current}, {RelationType: "links", Shard: legacy}, {RelationType: "other", Shard: legacy}})
	for _, shard := range []string{current, legacy} {
		if got := pages[shard]; len(got) != 1 || got[0].ID != id {
			t.Fatalf("page %s = %v", shard, got)
		}
		if got := edges["links\x00"+shard]; len(got) != 1 || got[0].ID != "edge:a" {
			t.Fatalf("edge shard %s = %v", shard, got)
		}
	}
	if len(pages["missing"]) != 0 || len(edges["other\x00"+legacy]) != 0 {
		t.Fatal("bucket leaked an unrelated entity or relation")
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
