package query

import (
	"reflect"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestLabelsAliasFiltersAndProjectsReservedField(t *testing.T) {
	article := graph.Entity{ID: "document:article", Kind: "document", Fields: graph.Fields{"title": "GGraphDB"}}
	if err := graph.SetEntityLabels(&article, []string{"knowledge", "article"}); err != nil {
		t.Fatal(err)
	}
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{ID: "seed", Version: 1, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{
		article,
		{ID: "document:legacy", Kind: "document", Fields: graph.Fields{"labels": "legacy"}},
	}}}); err != nil {
		t.Fatal(err)
	}

	request, err := ParseGQL(`FIND document WHERE labels CONTAINS "knowledge" PROJECT id, labels LIMIT 10`)
	if err != nil {
		t.Fatal(err)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != article.ID {
		t.Fatalf("results=%#v", response.Results)
	}
	if got := response.Results[0].Fields["labels"]; !reflect.DeepEqual(got, []any{"article", "knowledge"}) {
		t.Fatalf("projected labels=%#v", got)
	}
	if _, ok := response.Results[0].Entity.Fields[graph.ReservedLabelsField]; !ok {
		t.Fatalf("projected entity lost reserved labels: %#v", response.Results[0].Entity.Fields)
	}

	legacy, err := Execute(g, Request{Op: "match", Kind: "document", Where: []Filter{{Field: "labels", Op: "eq", Value: "legacy"}}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.Results) != 1 || legacy.Results[0].Entity.ID != "document:legacy" {
		t.Fatalf("legacy labels fallback=%#v", legacy.Results)
	}
}

func TestMaterializeFieldsLoadsReservedAndLegacyLabels(t *testing.T) {
	fields := materializeFields(Request{Op: "match", Project: []string{"labels"}})
	want := []string{graph.ReservedLabelsField, "labels"}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields=%#v want=%#v", fields, want)
	}
}
