package query

import (
	"testing"

	"graphdb/internal/graph"
)

func TestGQLFilterOperatorMatrix(t *testing.T) {
	g := seedCMDBGraph(t)
	tests := []struct {
		name string
		gql  string
		want []string
	}{
		{name: "eq", gql: `FIND host WHERE hostname = "app-01" LIMIT 10`, want: []string{"host:app-01"}},
		{name: "neq", gql: `FIND host WHERE hostname != "db-01" ORDER BY hostname LIMIT 10`, want: []string{"host:app-01", "host:app-02"}},
		{name: "in", gql: `FIND host WHERE region IN ["eu-west-1"] LIMIT 10`, want: []string{"host:app-02"}},
		{name: "exists", gql: `FIND host WHERE owner EXISTS ORDER BY hostname LIMIT 10`, want: []string{"host:app-01", "host:app-02"}},
		{name: "range", gql: `FIND host WHERE cpu > 4 AND cpu <= 16 ORDER BY cpu LIMIT 10`, want: []string{"host:app-01", "host:app-02"}},
		{name: "prefix", gql: `FIND host WHERE hostname PREFIX "app-" ORDER BY hostname LIMIT 10`, want: []string{"host:app-01", "host:app-02"}},
		{name: "contains", gql: `FIND host WHERE hostname CONTAINS "pp-0" ORDER BY hostname LIMIT 10`, want: []string{"host:app-01", "host:app-02"}},
		{name: "fuzzy", gql: `FIND service WHERE name FUZZY "frtd" LIMIT 10`, want: []string{"service:frontend"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := ParseGQL(tt.gql)
			if err != nil {
				t.Fatalf("parse gql: %v", err)
			}
			response, err := Execute(g, request)
			if err != nil {
				t.Fatalf("execute gql: %v", err)
			}
			got := entityResultIDs(response)
			if len(got) != len(tt.want) {
				t.Fatalf("got ids %v want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("got ids %v want %v", got, tt.want)
				}
			}
		})
	}
}

func TestGQLLiteralAliasAndKeywordOperatorMatrix(t *testing.T) {
	g := seedGQLLiteralGraph(t)
	tests := []struct {
		name string
		gql  string
		want []string
	}{
		{name: "keyword eq bool", gql: `FIND host WHERE enabled EQ true LIMIT 10`, want: []string{"host:blue"}},
		{name: "keyword lt float", gql: `FIND host WHERE load LT 1.0 LIMIT 10`, want: []string{"host:blue"}},
		{name: "keyword lte negative", gql: `FIND host WHERE offset LTE -3 LIMIT 10`, want: []string{"host:blue"}},
		{name: "null equality", gql: `FIND host WHERE nullable = null LIMIT 10`, want: []string{"host:blue"}},
		{name: "prefix exists", gql: `FIND host WHERE EXISTS nullable LIMIT 10`, want: []string{"host:blue"}},
		{name: "single quoted escaped string", gql: `FIND host WHERE hostname = 'app "blue"' LIMIT 10`, want: []string{"host:blue"}},
		{name: "sort by alias", gql: `FIND host SORT BY hostname DESC LIMIT 1`, want: []string{"host:red"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := ParseGQL(tt.gql)
			if err != nil {
				t.Fatalf("parse gql: %v", err)
			}
			response, err := Execute(g, request)
			if err != nil {
				t.Fatalf("execute gql: %v", err)
			}
			assertEntityResultIDs(t, response, tt.want)
		})
	}
}

func TestGQLAggregateAndRelationAliases(t *testing.T) {
	request, err := ParseGQL(`AGGREGATE count() AS total, max(cpu) AS max_cpu`)
	if err == nil || request.Op != "" {
		t.Fatalf("standalone aggregate should be rejected, request=%#v err=%v", request, err)
	}

	request, err = ParseGQL(`FIND host GROUP BY region AGGREGATE count() AS total, max(cpu) AS max_cpu HAVING total GTE 1 SORT hostname ASC LIMIT 10`)
	if err != nil {
		t.Fatalf("parse aggregate aliases: %v", err)
	}
	if len(request.Aggregate) != 2 || request.Aggregate[0].Name != "total" || request.Aggregate[1].Name != "max_cpu" {
		t.Fatalf("aggregates = %#v", request.Aggregate)
	}
	if len(request.Having) != 1 || request.Having[0].Op != "gte" {
		t.Fatalf("having = %#v expr=%#v", request.Having, request.HavingExpr)
	}

	graphRequest, err := ParseGQL(`NEIGHBORS service:frontend BOTH RELATION depends_on LIMIT 10`)
	if err != nil {
		t.Fatalf("parse relation alias: %v", err)
	}
	if graphRequest.Direction != "both" || len(graphRequest.RelationTypes) != 1 || graphRequest.RelationTypes[0] != "depends_on" {
		t.Fatalf("graph request = %#v", graphRequest)
	}
}

func entityResultIDs(response Response) []string {
	ids := make([]string, 0, len(response.Results))
	for _, result := range response.Results {
		if result.Entity != nil {
			ids = append(ids, result.Entity.ID)
		}
	}
	return ids
}

func assertEntityResultIDs(t *testing.T, response Response, want []string) {
	t.Helper()
	got := entityResultIDs(response)
	if len(got) != len(want) {
		t.Fatalf("got ids %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got ids %v want %v", got, want)
		}
	}
}

func seedGQLLiteralGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g := graph.New()
	if err := g.ApplyCommit(graph.Commit{
		ID:      "gql-literals",
		Version: 1,
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{
			{ID: "host:blue", Kind: "host", Fields: graph.Fields{
				"hostname": "app \"blue\"",
				"enabled":  true,
				"load":     0.75,
				"offset":   -3,
				"nullable": nil,
			}},
			{ID: "host:red", Kind: "host", Fields: graph.Fields{
				"hostname": "red-01",
				"enabled":  false,
				"load":     1.25,
				"offset":   5,
			}},
		}},
	}); err != nil {
		t.Fatalf("seed gql literal graph: %v", err)
	}
	return g
}
