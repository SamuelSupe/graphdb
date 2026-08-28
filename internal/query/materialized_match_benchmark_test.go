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

func BenchmarkMaterializedQueryOperations(b *testing.B) {
	const entityCount = 2000
	g := graph.New()
	entities := make([]graph.Entity, 0, entityCount)
	edges := make([]graph.Edge, 0, entityCount-1)
	for i := 0; i < entityCount; i++ {
		id := fmt.Sprintf("node:%04d", i)
		entities = append(entities, graph.Entity{
			ID: id, Kind: "node",
			Fields: graph.Fields{"bucket": i % 20, "score": i},
		})
		if i > 0 {
			edges = append(edges, graph.Edge{
				ID: fmt.Sprintf("edge:%04d", i), Type: "depends_on",
				From: fmt.Sprintf("node:%04d", i-1), To: id,
			})
		}
	}
	if err := g.ApplyCommit(graph.Commit{
		ID: "seed", Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{{
				Name: "node",
				Fields: map[string]graph.FieldSpec{
					"bucket": {Type: "number", Indexed: true},
					"score":  {Type: "number", Indexed: true},
				},
			}},
			UpsertEntities: entities,
			UpsertEdges:    edges,
		},
	}); err != nil {
		b.Fatal(err)
	}
	requests := map[string]Request{
		"match-kind-page": {
			Op: "match", Kind: "node", Limit: 20,
		},
		"match-field-index": {
			Op: "match", Kind: "node", Limit: 20,
			Where: []Filter{{Field: "bucket", Op: "eq", Value: 7}},
		},
		"neighbors": {
			Op: "neighbors", ID: "node:0100", Direction: "both", Limit: 20,
		},
		"traverse": {
			Op: "traverse", ID: "node:0100", Direction: "out", Depth: 4, Limit: 20,
		},
		"impact": {
			Op: "impact", ID: "node:0100", Direction: "out", Depth: 4, Limit: 20,
		},
		"shortest-path": {
			Op: "shortest_path", ID: "node:0100", TargetID: "node:0108", Direction: "out", Depth: 8,
		},
		"pattern": {
			Op: "pattern", Kind: "node",
			Where: []Filter{{Field: "id", Op: "eq", Value: "node:0100"}},
			Path: PathFilter{Steps: []PathStep{
				{Direction: "out", RelationTypes: []string{"depends_on"}},
				{Direction: "out", RelationTypes: []string{"depends_on"}},
			}},
			Limit: 20,
		},
	}
	for name, request := range requests {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				response, err := Execute(g, request)
				if err != nil {
					b.Fatal(err)
				}
				if response.Version != g.Version {
					b.Fatalf("version = %d, want %d", response.Version, g.Version)
				}
			}
		})
	}
}
