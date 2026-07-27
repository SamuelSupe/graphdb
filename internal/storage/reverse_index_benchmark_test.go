package storage

import (
	"context"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkSingleEntityCommitWithLargeReverseIndex(b *testing.B) {
	const (
		entityCount = 5000
		edgeCount   = 20000
	)
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "bench")
	entities := make([]graph.Entity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, graph.Entity{
			ID:     fmt.Sprintf("node:%05d", i),
			Kind:   "node",
			Fields: graph.Fields{"state": "ready"},
		})
	}
	edges := make([]graph.Edge, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		edges = append(edges, graph.Edge{
			Type: "link",
			From: fmt.Sprintf("node:%05d", i%entityCount),
			To: fmt.Sprintf(
				"node:%05d",
				(i*17+i/entityCount+1)%entityCount,
			),
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "node",
			Fields: map[string]graph.FieldSpec{
				"state": {Type: "string", Indexed: true},
			},
		}},
		UpsertRelationTypes: []graph.RelationType{{
			Name:        "link",
			FromKind:    "node",
			ToKind:      "node",
			Cardinality: graph.ManyToMany,
		}},
		UpsertEntities: entities,
		UpsertEdges:    edges,
	}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: "node:00000", Kind: "node",
				Fields: graph.Fields{"state": fmt.Sprintf("updated-%d", i)},
			}},
		}, CommitOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
