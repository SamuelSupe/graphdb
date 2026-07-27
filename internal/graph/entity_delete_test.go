package graph

import "testing"

func TestSourceStaleDeleteRemovesOnlyIncidentEdges(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed-delete-edges",
		Version: 1,
		Mutations: Mutations{
			UpsertEntities: []Entity{
				{ID: "host:stale", Kind: "host", Source: "agent", ExternalID: "stale"},
				{ID: "host:a", Kind: "host"},
				{ID: "host:b", Kind: "host"},
			},
			UpsertEdges: []Edge{
				{Type: "depends_on", From: "host:stale", To: "host:a"},
				{Type: "depends_on", From: "host:a", To: "host:stale"},
				{Type: "depends_on", From: "host:stale", To: "host:stale"},
				{Type: "depends_on", From: "host:a", To: "host:b"},
			},
		},
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	if err := g.ApplyCommit(Commit{
		ID:      "delete-stale",
		Version: 2,
		Mutations: Mutations{MarkSourceStale: []SourceStaleRequest{{
			Source: "agent",
			Action: "delete",
		}}},
	}); err != nil {
		t.Fatalf("delete stale entity: %v", err)
	}
	if _, ok := g.Entities["host:stale"]; ok {
		t.Fatal("stale entity was not deleted")
	}
	if len(g.Edges) != 1 {
		t.Fatalf("edges after delete = %#v, want one unrelated edge", g.Edges)
	}
	for _, edge := range g.Edges {
		if edge.From != "host:a" || edge.To != "host:b" {
			t.Fatalf("unexpected surviving edge: %#v", edge)
		}
	}
}
