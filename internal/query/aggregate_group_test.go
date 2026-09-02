package query

import (
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
