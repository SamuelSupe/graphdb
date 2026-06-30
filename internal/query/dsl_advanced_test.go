package query

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestBooleanWhereExpression(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:   "match",
		Kind: "host",
		WhereExpr: &FilterExpr{Op: "and", Children: []FilterExpr{
			{Op: "or", Children: []FilterExpr{
				{Field: "cpu", Op: "gte", Value: 16},
				{Field: "hostname", Op: "eq", Value: "db-01"},
			}},
			{Op: "not", Children: []FilterExpr{{Field: "owner", Op: "exists", Value: true}}},
		}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("boolean query: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:db-01" {
		t.Fatalf("results = %#v, want host:db-01", response.Results)
	}
}

func TestEdgeWhereFiltersNeighbors(t *testing.T) {
	g := seedEdgeFilterGraph(t)
	response, err := Execute(g, Request{
		Op:        "neighbors",
		ID:        "service:api",
		Direction: "out",
		EdgeWhere: []Filter{{Field: "status", Op: "eq", Value: "active"}},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("neighbors edge filter: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:active" {
		t.Fatalf("results = %#v, want host:active", response.Results)
	}
}

func TestPathStepsConstrainEachHop(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:        "traverse",
		ID:        "service:frontend",
		Direction: "out",
		Depth:     2,
		Path: PathFilter{Steps: []PathStep{
			{RelationTypes: []string{"depends_on"}, NodeKinds: []string{"service"}},
			{RelationTypes: []string{"depends_on"}, NodeKinds: []string{"database"}},
		}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("path steps: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Path == nil || pathEnd(*response.Results[0].Path).ID != "db:postgres" {
		t.Fatalf("results = %#v, want database path", response.Results)
	}

	miss, err := Execute(g, Request{
		Op:        "traverse",
		ID:        "service:frontend",
		Direction: "out",
		Depth:     2,
		Path: PathFilter{Steps: []PathStep{
			{RelationTypes: []string{"depends_on"}, NodeKinds: []string{"service"}},
			{RelationTypes: []string{"runs_on"}, NodeKinds: []string{"database"}},
		}},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("path steps miss: %v", err)
	}
	if len(miss.Results) != 0 {
		t.Fatalf("miss results = %#v, want none", miss.Results)
	}
}

func TestGroupByHavingAggregates(t *testing.T) {
	g := seedCMDBGraph(t)
	response, err := Execute(g, Request{
		Op:        "match",
		Kind:      "host",
		GroupBy:   []string{"owner"},
		Aggregate: []Aggregation{{Op: "count"}, {Name: "avg_cpu", Op: "avg", Field: "cpu"}},
		Having:    []Filter{{Field: "count", Op: "gte", Value: 2}},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("group query: %v", err)
	}
	if len(response.Groups) != 1 {
		t.Fatalf("groups = %#v, want one platform group", response.Groups)
	}
	if response.Groups[0].Key["owner"] != "platform" || response.Groups[0].Aggregates["count"] != 2 {
		t.Fatalf("group = %#v", response.Groups[0])
	}
}

func TestAdvancedGQLCompilesAndExecutes(t *testing.T) {
	g := seedCMDBGraph(t)
	request, err := ParseGQL(`FIND host WHERE (cpu >= 16 OR hostname = "db-01") AND NOT owner EXISTS GROUP BY region AGG count() HAVING count >= 1 LIMIT 10`)
	if err != nil {
		t.Fatalf("parse gql: %v", err)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatalf("execute gql: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:db-01" {
		t.Fatalf("results = %#v, want db host", response.Results)
	}
	if len(response.Groups) != 1 || response.Groups[0].Key["region"] != "us-east-1" {
		t.Fatalf("groups = %#v", response.Groups)
	}
}

func TestGQLPathStepAndEdgeWhere(t *testing.T) {
	request, err := ParseGQL(`TRAVERSE service:frontend OUT DEPTH 2 PATH STEP REL depends_on NODE service STEP REL depends_on NODE database EDGE WHERE status = "active" END KIND database LIMIT 10`)
	if err != nil {
		t.Fatalf("parse gql path step: %v", err)
	}
	if len(request.Path.Steps) != 2 || request.Path.Steps[1].EdgeWhere[0].Field != "status" {
		t.Fatalf("request path = %#v", request.Path)
	}
}

func seedEdgeFilterGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "edge-filter",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{
				{ID: "service:api", Kind: "service"},
				{ID: "host:active", Kind: "host"},
				{ID: "host:stale", Kind: "host"},
			},
			UpsertEdges: []graph.Edge{
				{ID: "edge:active", Type: "runs_on", From: "service:api", To: "host:active", Fields: graph.Fields{"status": "active"}},
				{ID: "edge:stale", Type: "runs_on", From: "service:api", To: "host:stale", Fields: graph.Fields{"status": "stale"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed edge filter graph: %v", err)
	}
	return g
}
