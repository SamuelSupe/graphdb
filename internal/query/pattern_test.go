package query

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestGQLPatternMatchesExactBoundedPath(t *testing.T) {
	request, err := ParseGQL(`MATCH service WHERE name = "frontend" PATH STEP OUT REL depends_on NODE service WHERE name = "api" STEP OUT REL depends_on NODE database WHERE name = "postgres" LIMIT 10`)
	if err != nil {
		t.Fatalf("parse pattern: %v", err)
	}
	if request.Op != "pattern" || request.Kind != "service" || len(request.Path.Steps) != 2 {
		t.Fatalf("request = %#v", request)
	}
	if request.Path.Steps[0].Direction != "out" || request.Path.Steps[1].RelationTypes[0] != "depends_on" {
		t.Fatalf("steps = %#v", request.Path.Steps)
	}

	response, err := Execute(seedCMDBGraph(t), request)
	if err != nil {
		t.Fatalf("execute pattern: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Path == nil {
		t.Fatalf("results = %#v, want one path", response.Results)
	}
	path := *response.Results[0].Path
	if len(path.Entities) != 3 || len(path.Edges) != 2 || path.Entities[0].ID != "service:frontend" || path.Entities[2].ID != "db:postgres" {
		t.Fatalf("path = %#v", path)
	}
}

func TestGQLKnowledgeGraphPatternExampleParses(t *testing.T) {
	request, err := ParseGQL(`MATCH document WHERE labels CONTAINS "article" PATH STEP OUT REL cites NODE document WHERE status = "published" STEP IN REL authored_by NODE person LIMIT 20`)
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "pattern" || request.Kind != "document" || request.Limit != 20 || len(request.Path.Steps) != 2 {
		t.Fatalf("request = %#v", request)
	}
	if request.Path.Steps[1].Direction != "in" || request.Path.Steps[1].NodeKinds[0] != "person" {
		t.Fatalf("steps = %#v", request.Path.Steps)
	}
}

func TestPatternSupportsDirectionPerStep(t *testing.T) {
	response, err := Execute(seedCMDBGraph(t), Request{
		Op:    "pattern",
		Kind:  "database",
		Where: []Filter{{Field: "id", Op: "eq", Value: "db:postgres"}},
		Path: PathFilter{Steps: []PathStep{
			{Direction: "in", RelationTypes: []string{"depends_on"}, NodeKinds: []string{"service"}, Where: []Filter{{Field: "name", Op: "eq", Value: "api"}}},
			{Direction: "in", RelationTypes: []string{"depends_on"}, NodeKinds: []string{"service"}, Where: []Filter{{Field: "name", Op: "eq", Value: "frontend"}}},
		}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("execute reverse pattern: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Path == nil || pathEnd(*response.Results[0].Path).ID != "service:frontend" {
		t.Fatalf("results = %#v, want reverse path to frontend", response.Results)
	}
}

func TestPatternValidationBoundsSteps(t *testing.T) {
	tests := []Request{
		{Op: "pattern", Kind: "service"},
		{Op: "pattern", Kind: "service", Path: PathFilter{Steps: make([]PathStep, 9)}},
		{Op: "pattern", Kind: "service", Depth: 2, Path: PathFilter{Steps: []PathStep{{}}}},
	}
	for _, request := range tests {
		if _, err := Execute(graph.New(), request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("request %#v error = %v, want ErrInvalid", request, err)
		}
	}
}

func TestPatternSortAndCursorCanReachLaterPaths(t *testing.T) {
	g := graph.New()
	entities := []graph.Entity{{ID: "document:start", Kind: "document"}}
	edges := make([]graph.Edge, 0, 5)
	for i := 5; i >= 1; i-- {
		id := fmt.Sprintf("document:%d", i)
		entities = append(entities, graph.Entity{ID: id, Kind: "document"})
		edges = append(edges, graph.Edge{Type: "cites", From: "document:start", To: id})
	}
	if err := g.ApplyCommit(graph.Commit{ID: "seed", Version: 1, Mutations: graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "cites", FromKind: "document", ToKind: "document", Directed: true}},
		UpsertEntities:      entities,
		UpsertEdges:         edges,
	}}); err != nil {
		t.Fatal(err)
	}

	request := Request{
		Op: "pattern", Kind: "document", Where: []Filter{{Field: "id", Op: "eq", Value: "document:start"}},
		Path: PathFilter{Steps: []PathStep{{Direction: "out", RelationTypes: []string{"cites"}}}},
		Sort: []SortSpec{{Field: "end_id"}}, Limit: 2,
	}
	wantPages := [][]string{{"document:1", "document:2"}, {"document:3", "document:4"}, {"document:5"}}
	for page, want := range wantPages {
		response, err := Execute(g, request)
		if err != nil {
			t.Fatalf("page %d: %v", page+1, err)
		}
		got := make([]string, 0, len(response.Results))
		for _, result := range response.Results {
			got = append(got, pathEnd(*result.Path).ID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("page %d = %#v, want %#v", page+1, got, want)
		}
		request.Cursor = response.NextCursor
	}
	if request.Cursor != "" {
		t.Fatalf("last page returned cursor %q", request.Cursor)
	}
}
