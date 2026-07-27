package query

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestMatchSortedSecondPageKeepsBoundedTopK(t *testing.T) {
	g := graph.New()
	g.Version = 1
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("host:%02d", i)
		g.Entities[id] = graph.Entity{
			ID:     id,
			Kind:   "host",
			Fields: graph.Fields{"score": float64(i)},
		}
	}

	request := Request{
		Op:      "match",
		Kind:    "host",
		Limit:   2,
		Sort:    []SortSpec{{Field: "score", Desc: true}},
		Profile: true,
		Aggregate: []Aggregation{{
			Name: "total",
			Op:   "count",
		}},
	}
	first, err := Execute(g, request)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("first page is missing a cursor")
	}

	request.Cursor = first.NextCursor
	second, err := Execute(g, request)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second.Results) != 2 ||
		second.Results[0].Entity.ID != "host:47" ||
		second.Results[1].Entity.ID != "host:46" {
		t.Fatalf("second page results = %#v", second.Results)
	}
	if second.Aggregates["total"] != 50 {
		t.Fatalf("second page total = %#v, want 50", second.Aggregates["total"])
	}
	if rows := profileRows(second.Profile, "filter-project"); rows != 5 {
		t.Fatalf("second page retained rows = %d, want offset + limit + lookahead = 5", rows)
	}
}

func profileRows(profile []OperatorStat, name string) int {
	for _, stat := range profile {
		if stat.Name == name {
			return stat.Rows
		}
	}
	return -1
}
