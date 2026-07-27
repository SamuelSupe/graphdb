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

func BenchmarkDeleteStaleEntitiesWithSparseEdges(b *testing.B) {
	const staleEntities = 1000
	const unrelatedEdges = 2000
	g := New()
	entities := make([]Entity, 0, staleEntities+unrelatedEdges*2)
	edges := make([]Edge, 0, unrelatedEdges)
	for i := 0; i < staleEntities; i++ {
		id := fmt.Sprintf("host:stale:%04d", i)
		entities = append(entities, Entity{
			ID: id, Kind: "host", Source: "agent", ExternalID: id,
		})
	}
	for i := 0; i < unrelatedEdges; i++ {
		from := fmt.Sprintf("service:active:%04d", i)
		to := fmt.Sprintf("host:active:%04d", i)
		entities = append(entities,
			Entity{ID: from, Kind: "service", Source: "manual", ExternalID: from},
			Entity{ID: to, Kind: "host", Source: "manual", ExternalID: to},
		)
		edges = append(edges, Edge{Type: "runs_on", From: from, To: to})
	}
	if err := g.ApplyCommit(Commit{
		ID:      "seed-stale-delete",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: entities,
			UpsertEdges:    edges,
		},
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID:      fmt.Sprintf("delete-stale-%d", i),
			Version: 2,
			Mutations: Mutations{MarkSourceStale: []SourceStaleRequest{{
				Source: "agent",
				Action: "delete",
			}}},
		}, ApplyOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}
