package graph

import (
	"fmt"
	"sort"
	"testing"
)

func TestBatchDeleteRelationTypesRemovesOnlyTheirEdges(t *testing.T) {
	g, err := FromSnapshot(Snapshot{
		Version: 1,
		Entities: []Entity{
			{ID: "node:a", Kind: "node"},
			{ID: "node:b", Kind: "node"},
		},
		RelationTypes: []RelationType{
			{Name: "first", AllowCrossKind: true, Directed: true},
			{Name: "second", AllowCrossKind: true, Directed: true},
			{Name: "kept", AllowCrossKind: true, Directed: true},
		},
		Edges: []Edge{
			{ID: "edge:first", Type: "first", From: "node:a", To: "node:b"},
			{ID: "edge:second", Type: "second", From: "node:a", To: "node:b"},
			{ID: "edge:kept", Type: "kept", From: "node:a", To: "node:b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantAffected := make([]string, 0, 2)
	for id, edge := range g.Edges {
		if edge.Type == "first" || edge.Type == "second" {
			wantAffected = append(wantAffected, id)
		}
	}
	sort.Strings(wantAffected)
	next, report, err := g.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "delete-relations",
		Version: 2,
		Mutations: Mutations{
			DeleteRelationTypes: []string{"first", "second"},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.RelationTypes["first"]; ok {
		t.Fatal("first relation type still exists")
	}
	if _, ok := next.RelationTypes["second"]; ok {
		t.Fatal("second relation type still exists")
	}
	if _, ok := next.RelationTypes["kept"]; !ok {
		t.Fatal("unrelated relation type was removed")
	}
	if len(next.Edges) != 1 {
		t.Fatalf("edges = %#v, want only kept edge", next.Edges)
	}
	for _, edge := range next.Edges {
		if edge.Type != "kept" {
			t.Fatalf("remaining edge = %#v, want kept relation", edge)
		}
	}
	sort.Strings(report.AffectedEdgeIDs)
	if got, want := fmt.Sprint(report.AffectedEdgeIDs), fmt.Sprint(wantAffected); got != want {
		t.Fatalf("affected edges = %s, want %s", got, want)
	}
}

func TestBatchUpdateRelationTypesStillRevalidatesExistingEdges(t *testing.T) {
	g, err := FromSnapshot(Snapshot{
		Version: 1,
		Entities: []Entity{
			{ID: "node:a", Kind: "node"},
			{ID: "node:b", Kind: "node"},
		},
		RelationTypes: []RelationType{
			{Name: "links", AllowCrossKind: true, Directed: true},
			{Name: "other", AllowCrossKind: true, Directed: true},
		},
		Edges: []Edge{{
			ID: "edge:links", Type: "links",
			From: "node:a", To: "node:b",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = g.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "update-relations",
		Version: 2,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{
				{Name: "other", AllowCrossKind: true, Directed: true},
				{Name: "links", FromKind: "service", ToKind: "host", Directed: true},
			},
		},
	}, ApplyOptions{})
	if err == nil {
		t.Fatal("batch relation update accepted an incompatible existing edge")
	}
}

func TestRelationTypeUpdateRevalidatesAllCardinalities(t *testing.T) {
	tests := []struct {
		name        string
		cardinality string
		edges       []Edge
	}{
		{
			name:        "many to one",
			cardinality: ManyToOne,
			edges: []Edge{
				{ID: "edge:first", Type: "links", From: "node:a", To: "node:b"},
				{ID: "edge:second", Type: "links", From: "node:a", To: "node:c"},
			},
		},
		{
			name:        "one to many",
			cardinality: OneToMany,
			edges: []Edge{
				{ID: "edge:first", Type: "links", From: "node:a", To: "node:c"},
				{ID: "edge:second", Type: "links", From: "node:b", To: "node:c"},
			},
		},
		{
			name:        "one to one",
			cardinality: OneToOne,
			edges: []Edge{
				{ID: "edge:first", Type: "links", From: "node:a", To: "node:c"},
				{ID: "edge:second", Type: "links", From: "node:b", To: "node:c"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g, err := FromSnapshot(Snapshot{
				Version: 1,
				Entities: []Entity{
					{ID: "node:a", Kind: "node"},
					{ID: "node:b", Kind: "node"},
					{ID: "node:c", Kind: "node"},
				},
				RelationTypes: []RelationType{{
					Name: "links", FromKind: "node", ToKind: "node", Directed: true,
				}},
				Edges: test.edges,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = g.ApplyCommitStorageCopyWithOptions(Commit{
				ID:      "restrict-cardinality",
				Version: 2,
				Mutations: Mutations{
					UpsertRelationTypes: []RelationType{{
						Name:        "links",
						FromKind:    "node",
						ToKind:      "node",
						Directed:    true,
						Cardinality: test.cardinality,
					}},
				},
			}, ApplyOptions{})
			if err == nil {
				t.Fatalf("relation update accepted %s cardinality violation", test.cardinality)
			}
		})
	}
}
