package query

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestMatchNeighborsAndTraverse(t *testing.T) {
	g := seedGraph(t)

	match, err := Execute(g, Request{
		Op:      "match",
		Kind:    "person",
		Filters: graph.Fields{"name": "Alice"},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(match.Results) != 1 || match.Results[0].Entity.ID != "person:alice" {
		t.Fatalf("unexpected match results: %#v", match.Results)
	}

	neighbors, err := Execute(g, Request{
		Op:           "neighbors",
		ID:           "person:alice",
		Direction:    "out",
		RelationType: "works_at",
	})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(neighbors.Results) != 1 || neighbors.Results[0].Entity.ID != "company:acme" {
		t.Fatalf("unexpected neighbors: %#v", neighbors.Results)
	}

	traverse, err := Execute(g, Request{
		Op:        "traverse",
		ID:        "person:alice",
		Direction: "out",
		Depth:     2,
	})
	if err != nil {
		t.Fatalf("traverse: %v", err)
	}
	if len(traverse.Results) != 1 {
		t.Fatalf("traverse result count = %d, want 1", len(traverse.Results))
	}
	path := traverse.Results[0].Path
	if len(path.Entities) != 2 || path.Entities[1].ID != "company:acme" {
		t.Fatalf("unexpected path: %#v", path)
	}
}

func TestNeighborsApplyLegacyFilters(t *testing.T) {
	g := seedGraph(t)
	err := g.ApplyCommit(graph.Commit{
		ID:      "second-company",
		Version: 2,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name:        "works_at",
				FromKind:    "person",
				ToKind:      "company",
				Directed:    true,
				Cardinality: graph.ManyToMany,
			}},
			UpsertEntities: []graph.Entity{{ID: "company:globex", Kind: "company", Fields: graph.Fields{"name": "Globex"}}},
			UpsertEdges: []graph.Edge{{
				ID:   "edge:alice-globex",
				Type: "works_at",
				From: "person:alice",
				To:   "company:globex",
			}},
		},
	})
	if err != nil {
		t.Fatalf("add second neighbor: %v", err)
	}
	response, err := Execute(g, Request{
		Op:           "neighbors",
		ID:           "person:alice",
		Direction:    "out",
		RelationType: "works_at",
		Filters:      graph.Fields{"name": "ACME"},
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "company:acme" {
		t.Fatalf("neighbors results = %#v, want only ACME", response.Results)
	}
}

func TestCursorPagination(t *testing.T) {
	g := seedGraph(t)
	response, err := Execute(g, Request{Op: "match", Kind: "person", Limit: 1})
	if err != nil {
		t.Fatalf("match page 1: %v", err)
	}
	if response.NextCursor == "" {
		t.Fatal("expected next cursor")
	}
	next, err := Execute(g, Request{Op: "match", Kind: "person", Limit: 1, Cursor: response.NextCursor})
	if err != nil {
		t.Fatalf("match page 2: %v", err)
	}
	if len(next.Results) != 1 || next.Results[0].Entity.ID == response.Results[0].Entity.ID {
		t.Fatalf("unexpected second page: %#v", next.Results)
	}
}

func seedGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g := graph.New()
	err := g.ApplyCommit(graph.Commit{
		ID:      "seed",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertRelationTypes: []graph.RelationType{{
				Name:        "works_at",
				FromKind:    "person",
				ToKind:      "company",
				Directed:    true,
				Cardinality: graph.ManyToOne,
			}},
			UpsertEntities: []graph.Entity{
				{ID: "person:alice", Kind: "person", Fields: graph.Fields{"name": "Alice"}},
				{ID: "person:bob", Kind: "person", Fields: graph.Fields{"name": "Bob"}},
				{ID: "company:acme", Kind: "company", Fields: graph.Fields{"name": "ACME"}},
			},
			UpsertEdges: []graph.Edge{{
				ID:   "edge:alice-acme",
				Type: "works_at",
				From: "person:alice",
				To:   "company:acme",
			}},
		},
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	return g
}
