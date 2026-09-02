package graph

import (
	"fmt"
	"testing"
)

func TestMatchEntityIDsAndFieldIndexIDsAreSorted(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID:      "seed",
		Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:b", Kind: "host", Fields: Fields{"region": "us-east-1"}},
			{ID: "service:a", Kind: "service", Fields: Fields{"region": "us-east-1"}},
			{ID: "host:a", Kind: "host", Fields: Fields{"region": "us-west-2"}},
			{ID: "host:c", Kind: "host", Fields: Fields{"region": "us-east-1"}},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, []string{"host:a", "host:b", "host:c"}) {
		t.Fatalf("host ids = %#v", ids)
	}
	if ids := g.MatchFieldIndexIDs("host", "region", []any{"us-east-1", "us-east-1"}); !sameStrings(ids, []string{"host:b", "host:c"}) {
		t.Fatalf("field index ids = %#v", ids)
	}
	values := []any{"us-east-1", "us-west-2", "us-east-1"}
	if ids := g.MatchFieldIndexIDs("host", "region", values); !sameStrings(ids, []string{"host:a", "host:b", "host:c"}) {
		t.Fatalf("multi-value field index ids = %#v", ids)
	}
	if count := g.FieldIndexCount("host", "region", values); count != 3 {
		t.Fatalf("multi-value field index count = %d, want 3", count)
	}
}

func TestReadOrderCachesInvalidateAfterMutation(t *testing.T) {
	g := New()
	eastIDs := make([]string, 0, minCachedFieldIndexOrder)
	seedEntities := make([]Entity, 0, minCachedFieldIndexOrder)
	for i := 0; i < minCachedFieldIndexOrder; i++ {
		id := fmt.Sprintf("host:east-%02d", i)
		eastIDs = append(eastIDs, id)
		seedEntities = append(seedEntities, Entity{
			ID: id, Kind: "host", Fields: Fields{"region": "us-east-1"},
		})
	}
	if err := g.ApplyCommit(Commit{
		ID: "seed", Version: 1,
		Mutations: Mutations{UpsertEntities: seedEntities},
	}); err != nil {
		t.Fatal(err)
	}
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, eastIDs) {
		t.Fatalf("initial ids = %#v", ids)
	}
	visited := make([]string, 0, len(eastIDs))
	count, err := g.VisitFieldIndexIDs("host", "region", []any{"us-east-1"}, func(id string) error {
		visited = append(visited, id)
		return nil
	})
	if err != nil || count != len(eastIDs) || !sameStrings(visited, eastIDs) {
		t.Fatalf("initial field index ids = %#v, err = %v", visited, err)
	}
	updatedEastIDs := append([]string(nil), eastIDs[1:]...)
	updatedEastIDs = append(updatedEastIDs, "host:east-new")
	updatedIDs := append([]string{"host:east-00"}, updatedEastIDs...)
	if err := g.ApplyCommit(Commit{
		ID: "update", Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:east-00", Kind: "host", Fields: Fields{"region": "us-west-2"}},
			{ID: "host:east-new", Kind: "host", Fields: Fields{"region": "us-east-1"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, updatedIDs) {
		t.Fatalf("updated ids = %#v", ids)
	}
	visited = visited[:0]
	count, err = g.VisitFieldIndexIDs("host", "region", []any{"us-east-1", "us-west-2"}, func(id string) error {
		visited = append(visited, id)
		return nil
	})
	if err != nil || count != len(updatedIDs) || !sameStrings(visited, updatedIDs) {
		t.Fatalf("updated field index ids = %#v, err = %v", visited, err)
	}
}

func sameStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
