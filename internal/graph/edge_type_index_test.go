package graph

import "testing"

func TestEdgeTypeIndexCopyOnWriteIsolation(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{
			UpsertRelationTypes: []RelationType{
				{Name: "first", AllowCrossKind: true},
				{Name: "second", AllowCrossKind: true},
			},
			UpsertEntities: []Entity{
				{ID: "node:a", Kind: "node"},
				{ID: "node:b", Kind: "node"},
			},
			UpsertEdges: []Edge{
				{ID: "edge:first", Type: "first", From: "node:a", To: "node:b"},
				{ID: "edge:second", Type: "second", From: "node:a", To: "node:b"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	next, _, err := g.ApplyCommitStorageCopyWithOptions(Commit{
		ID:      "delete-first",
		Version: 2,
		Mutations: Mutations{
			DeleteRelationTypes: []string{"first"},
		},
	}, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if next.edgeTypeIndex.At("first").Len() != 0 {
		t.Fatalf("next first index = %#v", next.edgeTypeIndex.At("first"))
	}
	if next.edgeTypeIndex.At("second").Len() != 1 {
		t.Fatalf("next second index = %#v", next.edgeTypeIndex.At("second"))
	}
	if g.edgeTypeIndex.At("first").Len() != 1 {
		t.Fatalf("source first index changed: %#v", g.edgeTypeIndex.At("first"))
	}
	if g.edgeTypeIndex.At("second").Len() != 1 {
		t.Fatalf("source second index changed: %#v", g.edgeTypeIndex.At("second"))
	}
}
