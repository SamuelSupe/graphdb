package codec

import (
	"reflect"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestCommitBuilderRetainsSparseOrdinalsAndCopiesDecodedValues(t *testing.T) {
	var builder CommitBuilder
	for _, ordinal := range []int{4, 1} {
		rows, err := EntityMutationRows(CommitRowUpsertEntity, ordinal, 0, graph.Entity{
			ID: "host:a", Fields: graph.Fields{"name": "original"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if err := builder.Apply(row); err != nil {
				t.Fatal(err)
			}
		}
		rows, err = EdgeMutationRows(CommitRowUpsertEdge, ordinal, graph.Edge{
			ID: "edge:a", Fields: graph.Fields{"name": "original"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if err := builder.Apply(row); err != nil {
				t.Fatal(err)
			}
		}
	}
	var ordinals []int
	for ordinal, entity := range builder.Entities() {
		ordinals = append(ordinals, ordinal)
		entity.Fields["name"] = "changed by caller"
	}
	if !reflect.DeepEqual(ordinals, []int{1, 4}) {
		t.Fatalf("decoded entity ordinals = %v, want [1 4]", ordinals)
	}
	ordinals = nil
	for ordinal, edge := range builder.Edges() {
		ordinals = append(ordinals, ordinal)
		edge.Fields["name"] = "changed by caller"
	}
	if !reflect.DeepEqual(ordinals, []int{1, 4}) {
		t.Fatalf("decoded edge ordinals = %v, want [1 4]", ordinals)
	}
	for _, entity := range builder.Entities() {
		if entity.Fields["name"] != "original" {
			t.Fatal("caller mutated retained entity")
		}
	}
	for _, edge := range builder.Edges() {
		if edge.Fields["name"] != "original" {
			t.Fatal("caller mutated retained edge")
		}
	}
}
