package query

import (
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
	groups := aggregateGroups(
		[]Result{{Entity: &stringValue}, {Entity: &numberValue}},
		[]string{"bucket"},
		[]Aggregation{{Op: "count"}},
		nil,
		nil,
	)
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want distinct string and number groups", groups)
	}
	for _, group := range groups {
		if group.Aggregates["count"] != 1 {
			t.Fatalf("group = %#v, want count 1", group)
		}
	}
}
