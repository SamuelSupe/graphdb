package graph

import "testing"

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
}

func TestMatchEntityIDsOrderCacheInvalidatesAfterMutation(t *testing.T) {
	g := New()
	if err := g.ApplyCommit(Commit{
		ID: "seed", Version: 1,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:b", Kind: "host"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, []string{"host:b"}) {
		t.Fatalf("initial ids = %#v", ids)
	}
	if err := g.ApplyCommit(Commit{
		ID: "update", Version: 2,
		Mutations: Mutations{UpsertEntities: []Entity{
			{ID: "host:a", Kind: "host"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if ids := g.MatchEntityIDs("host"); !sameStrings(ids, []string{"host:a", "host:b"}) {
		t.Fatalf("updated ids = %#v", ids)
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
