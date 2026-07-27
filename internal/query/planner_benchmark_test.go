package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkPlanRuntimeIndexRangeLargeGraph(b *testing.B) {
	const entityCount = 50_000
	g := graph.New()
	entities := make([]graph.Entity, 0, entityCount)
	for index := 0; index < entityCount; index++ {
		entities = append(entities, graph.Entity{
			ID:   fmt.Sprintf("node:%05d", index),
			Kind: "node",
			Fields: graph.Fields{
				"score": index,
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
		Op:   "match",
		Kind: "node",
		Where: []Filter{{
			Field: "score",
			Op:    "gte",
			Value: 0,
		}},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		plan := PlanQuery(g, request)
		if plan.Strategy != "field-index-scan" {
			b.Fatalf("strategy = %q", plan.Strategy)
		}
	}
}
