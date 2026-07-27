package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkMaterializedKindPage(b *testing.B) {
	const entityCount = 100000
	g := graph.New()
	g.Version = 1
	for i := 0; i < entityCount; i++ {
		id := fmt.Sprintf("host:%06d", i)
		g.Entities[id] = graph.Entity{ID: id, Kind: "host"}
	}
	request := Request{
		Op:        "match",
		Kind:      "host",
		Limit:     10,
		CostLimit: entityCount + 1,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := Execute(g, request)
		if err != nil {
			b.Fatal(err)
		}
		if len(response.Results) != request.Limit {
			b.Fatalf("results = %d, want %d", len(response.Results), request.Limit)
		}
	}
}

func BenchmarkMaterializedFieldIndexRangePage(b *testing.B) {
	const entityCount = 50000
	g := graph.New()
	entities := make([]graph.Entity, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		entities = append(entities, graph.Entity{
			ID:   fmt.Sprintf("node:%05d", i),
			Kind: "node",
			Fields: graph.Fields{
				"score": i,
			},
		})
	}
	if err := g.ApplyCommit(graph.Commit{
		ID:      "seed",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: entities,
		},
	}); err != nil {
		b.Fatal(err)
	}
	request := Request{
		Op:        "match",
		Kind:      "node",
		Limit:     5,
		CostLimit: entityCount + 1,
		Where: []Filter{{
			Field: "score",
			Op:    "gte",
			Value: entityCount - 10,
		}},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := Execute(g, request)
		if err != nil {
			b.Fatal(err)
		}
		if len(response.Results) != request.Limit {
			b.Fatalf("results = %d, want %d", len(response.Results), request.Limit)
		}
	}
}
