package query

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestGroupByKeepsStringAndNumberValuesSeparate(t *testing.T) {
	stringValue := graph.Entity{
		ID: "entity:string", Kind: "item",
		Fields: graph.Fields{"bucket": "1"},
	}
	numberValue := graph.Entity{
		ID: "entity:number", Kind: "item",
		Fields: graph.Fields{"bucket": float64(1)},
	}
	groups, err := aggregateGroups(
		[]Result{{Entity: &stringValue}, {Entity: &numberValue}},
		[]string{"bucket"},
		[]Aggregation{{Op: "count"}},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("aggregateGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want distinct string and number groups", groups)
	}
	for _, group := range groups {
		if group.Aggregates["count"] != 1 {
			t.Fatalf("group = %#v, want count 1", group)
		}
	}
}

func TestAggregateCardinalityIsBounded(t *testing.T) {
	entities := make([]graph.Entity, maxAggregateBuckets+1)
	results := make([]Result, len(entities))
	for i := range entities {
		entities[i] = graph.Entity{ID: fmt.Sprintf("entity:%d", i), Kind: "item", Fields: graph.Fields{"bucket": i}}
		results[i] = Result{Entity: &entities[i]}
	}
	if _, err := aggregateResults(results, []Aggregation{{Op: "count_by", Field: "bucket"}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("count_by err = %v, want ErrLimitExceeded", err)
	}
	if _, err := aggregateGroups(results, []string{"bucket"}, nil, nil, nil); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("group_by err = %v, want ErrLimitExceeded", err)
	}
}

func TestAggregateFiniteExtremes(t *testing.T) {
	for _, values := range [][]float64{{1e308, 1e308}, {math.MaxFloat64, math.MaxFloat64}, {math.MaxFloat64, -math.MaxFloat64}} {
		results := make([]Result, len(values))
		for i, value := range values {
			results[i].Entity = &graph.Entity{Fields: graph.Fields{"value": value, "group": "a"}}
		}
		aggregate, err := aggregateResults(results, []Aggregation{{Op: "avg", Field: "value"}, {Op: "min", Field: "value"}, {Op: "max", Field: "value"}})
		if err != nil {
			t.Fatal(err)
		}
		want := values[0]/2 + values[1]/2
		if aggregate["avg_value"] != want {
			t.Fatalf("avg=%v want %v", aggregate, want)
		}
		groups, err := aggregateGroups(results, []string{"group"}, []Aggregation{{Op: "avg", Field: "value"}}, nil, nil)
		if err != nil || len(groups) != 1 || groups[0].Aggregates["avg_value"] != want {
			t.Fatalf("groups=%+v err=%v", groups, err)
		}
		_, err = aggregateResults(results, []Aggregation{{Op: "sum", Field: "value"}})
		if values[0] == values[1] && !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("overflow error=%v", err)
		}
	}
}
