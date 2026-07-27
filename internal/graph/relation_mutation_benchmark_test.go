package graph

import (
	"fmt"
	"testing"
)

func BenchmarkBatchUpdateRelationTypes(b *testing.B) {
	const (
		relationCount = 64
		edgesPerType  = 128
	)
	relationTypes := make([]RelationType, 0, relationCount)
	entities := make([]Entity, 0, edgesPerType*2)
	edges := make([]Edge, 0, relationCount*edgesPerType)
	for i := 0; i < edgesPerType; i++ {
		entities = append(
			entities,
			Entity{ID: fmt.Sprintf("node:from:%03d", i), Kind: "node"},
			Entity{ID: fmt.Sprintf("node:to:%03d", i), Kind: "node"},
		)
	}
	for relation := 0; relation < relationCount; relation++ {
		name := fmt.Sprintf("relation_%03d", relation)
		relationTypes = append(relationTypes, RelationType{
			Name: name, FromKind: "node", ToKind: "node", Directed: true,
		})
		for edge := 0; edge < edgesPerType; edge++ {
			edges = append(edges, Edge{
				Type: name,
				From: fmt.Sprintf("node:from:%03d", edge),
				To:   fmt.Sprintf("node:to:%03d", edge),
			})
		}
	}
	g, err := FromSnapshot(Snapshot{
		Version:       1,
		Entities:      entities,
		RelationTypes: relationTypes,
		Edges:         edges,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("update-relations-%d", i),
			Version: 2,
			Mutations: Mutations{
				UpsertRelationTypes: relationTypes,
			},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateHighDegreeCardinality(b *testing.B) {
	const edgeCount = 4096
	g := New()
	g.Entities["node:hub"] = Entity{ID: "node:hub", Kind: "node"}
	for i := 0; i < edgeCount; i++ {
		suffix := fmt.Sprintf("%04d", i)
		relationName := "relation_" + suffix
		entityID := "node:to:" + suffix
		edgeID := "edge:" + suffix
		g.Entities[entityID] = Entity{ID: entityID, Kind: "node"}
		g.RelationTypes[relationName] = RelationType{
			Name:        relationName,
			FromKind:    "node",
			ToKind:      "node",
			Directed:    true,
			Cardinality: ManyToOne,
		}
		g.Edges[edgeID] = Edge{
			ID:   edgeID,
			Type: relationName,
			From: "node:hub",
			To:   entityID,
		}
	}
	g.rebuildIndexes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := g.validateAllEdges(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddUnusedRelationTypeToLargeGraph(b *testing.B) {
	g := largeIsolatedMutationGraph(b, 10000, 50000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("unused-relation-%d", i),
			Version: 1,
			Mutations: Mutations{UpsertRelationTypes: []RelationType{{
				Name:           "unused",
				AllowCrossKind: true,
			}}},
		}, ApplyOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
