package query

import "testing"

func TestLazyReadAcceptsAuthoritativeEmptyEdgeIndexes(t *testing.T) {
	stats := PlannerStats{
		Version:                   3,
		ForwardEdgeIndexAvailable: true,
		ReverseEdgeIndexAvailable: true,
		EntityPages: []PlannerEntityPageStat{{
			Shard:       "00",
			EntityCount: 1000000,
		}},
	}
	for _, request := range []Request{
		{Op: "neighbors", ID: "host:app", Direction: "out"},
		{Op: "neighbors", ID: "host:app", Direction: "in"},
		{Op: "neighbors", ID: "host:app", Direction: "both"},
		{Op: "traverse", ID: "host:app", Direction: "out", Depth: 2},
	} {
		if !SupportsLazyRead(request, stats) {
			t.Fatalf("lazy read rejected %#v with complete empty edge indexes", request)
		}
	}

	stats.ReverseEdgeIndexAvailable = false
	if SupportsLazyRead(
		Request{Op: "neighbors", ID: "host:app", Direction: "in"},
		stats,
	) {
		t.Fatal("lazy read accepted incoming query without a reverse index")
	}
	if !SupportsLazyRead(
		Request{Op: "neighbors", ID: "host:app", Direction: "out"},
		stats,
	) {
		t.Fatal("lazy read rejected outgoing query with a forward index")
	}
}
