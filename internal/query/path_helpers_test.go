package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestMaxPathResultsCapsExplicitMaxPaths(t *testing.T) {
	got := maxPathResults(Request{Path: PathFilter{MaxPaths: 1_000_000}})
	if got != maxQueryLimit+1 {
		t.Fatalf("maxPathResults = %d, want %d", got, maxQueryLimit+1)
	}
}

func TestMaxPathResultsKeepsSmallExplicitMaxPaths(t *testing.T) {
	got := maxPathResults(Request{Limit: 100, Path: PathFilter{MaxPaths: 7}})
	if got != 7 {
		t.Fatalf("maxPathResults = %d, want explicit small cap", got)
	}
}

func TestMaxPathResultsUsesStableCursorOffset(t *testing.T) {
	request := Request{Op: "traverse", Limit: 1000}
	request.Cursor = encodeCursor(cursorState{
		Version: 1,
		After:   "path:edge:0999",
		Offset:  1000,
		Query:   cursorQueryHash(request),
	})
	if got := maxPathResults(request); got != 2001 {
		t.Fatalf("maxPathResults = %d, want cursor window 2001", got)
	}
}

func TestMaxPathResultsUsesCostLimitForGlobalOperations(t *testing.T) {
	request := Request{
		Op:        "traverse",
		Sort:      []SortSpec{{Field: "end_id"}},
		CostLimit: 2500,
	}
	if got := maxPathResults(request); got != 2501 {
		t.Fatalf("maxPathResults = %d, want cost-bounded 2501", got)
	}
}

func TestCompletePathCollectorKeepsOnlyPageWindow(t *testing.T) {
	request := Request{
		Op:        "traverse",
		Sort:      []SortSpec{{Field: "end_id", Desc: true}},
		Aggregate: []Aggregation{{Op: "count"}},
		Limit:     1,
	}
	collector := newPathResultCollector(request)
	for index := 0; index < 5000; index++ {
		id := fmt.Sprintf("host:%04d", index)
		if err := collector.add(graph.Path{
			Entities: []graph.Entity{
				{ID: "service:start", Kind: "service"},
				{ID: id, Kind: "host"},
			},
			Edges: []graph.Edge{{ID: "edge:" + id}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if collector.count != 5000 || collector.sorted.Len() != 2 {
		t.Fatalf(
			"collector count=%d retained=%d, want 5000/2",
			collector.count, collector.sorted.Len(),
		)
	}
	if collector.aggregates()["count"] != 5000 {
		t.Fatalf("aggregates = %#v", collector.aggregates())
	}
	results := collector.results()
	if len(results) != 2 ||
		pathEnd(*results[0].Path).ID != "host:4999" {
		t.Fatalf("results = %#v", results)
	}
}
