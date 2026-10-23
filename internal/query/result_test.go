package query

import (
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"math"
	"testing"
	"time"
)

func TestPathResultFieldsUseEndpoint(t *testing.T) {
	result := Result{Path: &graph.Path{Entities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"cost": 1}}, {ID: "host:b", Kind: "host", Fields: graph.Fields{"cost": 7, "name": "endpoint"}}}}}
	if value := resultValue(result, "cost"); value != 7 {
		t.Fatalf("path aggregation lost endpoint cost: %v", value)
	}
	if value := resultValue(result, "name"); value != "endpoint" {
		t.Fatalf("path sorting lost endpoint name: %v", value)
	}
	if value := resultValue(result, "kind"); value != "host" {
		t.Fatalf("path grouping lost endpoint kind: %v", value)
	}
}

func TestQueryTimeoutCannotOverflowDuration(t *testing.T) {
	if err := validateRequestControls(Request{TimeoutMS: int(math.MaxInt64/int64(time.Millisecond)) + 1}); err == nil {
		t.Fatal("overflowed timeout accepted")
	}
	if err := validateRequestControls(Request{TimeoutMS: 0}); err != nil {
		t.Fatal(err)
	}
}

func TestPathEndpointSortAndAggregateKeepDefaultCursorOrder(t *testing.T) {
	g := traversalPagingGraph(t)
	request := Request{Op: "traverse", ID: "service:start", Direction: "out", Depth: 1, Limit: 10, Sort: []SortSpec{{Field: "rank", Desc: true}}, Aggregate: []Aggregation{{Op: "sum", Field: "rank"}}, GroupBy: []string{"kind"}}
	result, err := Execute(g, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 3 || pathEnd(*result.Results[0].Path).ID != "host:c" {
		t.Fatalf("endpoint sort: %+v", result.Results)
	}
	if len(result.Groups) != 1 {
		t.Fatalf("endpoint grouping: %+v", result.Groups)
	}
}
