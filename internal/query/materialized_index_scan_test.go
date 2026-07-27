package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestMaterializedFieldIndexScanReturnsOnlyMatchingCandidates(t *testing.T) {
	g := graph.New()
	entities := make([]graph.Entity, 0, 100)
	for i := 0; i < 100; i++ {
		entities = append(entities, graph.Entity{
			ID:   fmt.Sprintf("node:%03d", i),
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
		t.Fatal(err)
	}

	response, err := Execute(g, Request{
		Op:      "match",
		Kind:    "node",
		Limit:   10,
		Profile: true,
		Where: []Filter{{
			Field: "score",
			Op:    "gte",
			Value: 98,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Plan == nil || response.Plan.Strategy != "field-index-scan" {
		t.Fatalf("plan = %#v", response.Plan)
	}
	if got := resultEntityIDs(response.Results); len(got) != 2 ||
		got[0] != "node:098" || got[1] != "node:099" {
		t.Fatalf("results = %#v", got)
	}
	if rows := profileRows(response.Profile, "index-scan"); rows != 2 {
		t.Fatalf("index-scan rows = %d, want 2", rows)
	}
}

func TestExistsOnIndexedAnyFieldIncludesNonScalarValues(t *testing.T) {
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "seed",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{{
				Name: "node",
				Fields: map[string]graph.FieldSpec{
					"metadata": {Type: "any", Indexed: true},
				},
			}},
			UpsertEntities: []graph.Entity{
				{
					ID:   "node:object",
					Kind: "node",
					Fields: graph.Fields{
						"metadata": map[string]any{"region": "sg"},
					},
				},
				{
					ID:     "node:scalar",
					Kind:   "node",
					Fields: graph.Fields{"metadata": "ready"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	request := Request{
		Op:    "match",
		Kind:  "node",
		Where: []Filter{{Field: "metadata", Op: "exists", Value: true}},
	}
	if plan := PlanQuery(g, request); plan.Strategy != "kind-scan" {
		t.Fatalf("strategy = %q, want kind-scan", plan.Strategy)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatal(err)
	}
	if got := resultEntityIDs(response.Results); len(got) != 2 ||
		got[0] != "node:object" || got[1] != "node:scalar" {
		t.Fatalf("results = %#v", got)
	}
}
