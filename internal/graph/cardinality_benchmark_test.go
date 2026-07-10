package graph

import (
	"fmt"
	"testing"
)

func BenchmarkApplyManyToManyEdges(b *testing.B) {
	const edgeCount = 2000
	for i := 0; i < b.N; i++ {
		g := New()
		entities := make([]Entity, 0, edgeCount*2)
		edges := make([]Edge, 0, edgeCount)
		for n := 0; n < edgeCount; n++ {
			from := fmt.Sprintf("service:%04d", n)
			to := fmt.Sprintf("host:%04d", n)
			entities = append(entities,
				Entity{ID: from, Kind: "service"},
				Entity{ID: to, Kind: "host"},
			)
			edges = append(edges, Edge{Type: "runs_on", From: from, To: to})
		}
		b.StartTimer()
		err := g.ApplyCommit(Commit{
			ID:      fmt.Sprintf("bench-%d", i),
			Version: 1,
			Mutations: Mutations{
				UpsertEntities: entities,
				UpsertEdges:    edges,
			},
		})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkApplySingleEntityStorageCopy(b *testing.B) {
	const entityCount = 10000
	g := New()
	entities := make([]Entity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, Entity{ID: fmt.Sprintf("host:%05d", i), Kind: "host", Fields: Fields{"state": "ready"}})
	}
	if err := g.ApplyCommit(Commit{ID: "seed", Version: 1, Mutations: Mutations{UpsertEntities: entities}}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("update-%d", i),
			Version: 2,
			Mutations: Mutations{UpsertEntities: []Entity{{
				ID: "host:00000", Kind: "host", Fields: Fields{"state": "updated"},
			}}},
		}, ApplyOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}
