package query

import "testing"

func TestParseGQLFind(t *testing.T) {
	request, err := ParseGQL(`FIND host WHERE cpu >= 8 AND region IN ["us-east-1", "eu-west-1"] PROJECT id, hostname, cpu ORDER BY cpu DESC LIMIT 100`)
	if err != nil {
		t.Fatalf("parse gql: %v", err)
	}
	if request.Op != "match" || request.Kind != "host" || request.Limit != 100 {
		t.Fatalf("request = %#v", request)
	}
	if len(request.Where) != 2 || request.Where[0].Field != "cpu" || request.Where[0].Op != "gte" {
		t.Fatalf("where = %#v", request.Where)
	}
	if len(request.Project) != 3 || request.Project[1] != "hostname" {
		t.Fatalf("project = %#v", request.Project)
	}
	if len(request.Sort) != 1 || request.Sort[0].Field != "cpu" || !request.Sort[0].Desc {
		t.Fatalf("sort = %#v", request.Sort)
	}
}

func TestParseGQLGraphOps(t *testing.T) {
	tests := []struct {
		name string
		text string
		want Request
	}{
		{
			name: "neighbors",
			text: `NEIGHBORS service:checkout OUT REL depends_on, runs_on WHERE kind IN ["host", "database"] PROJECT id, name LIMIT 100`,
			want: Request{Op: "neighbors", ID: "service:checkout", Direction: "out", RelationTypes: []string{"depends_on", "runs_on"}, Limit: 100},
		},
		{
			name: "traverse",
			text: `TRAVERSE service:checkout OUT REL depends_on DEPTH 3 PATH NODES service, host, database END KIND database LIMIT 50`,
			want: Request{Op: "traverse", ID: "service:checkout", Direction: "out", RelationTypes: []string{"depends_on"}, Depth: 3, Limit: 50},
		},
		{
			name: "impact",
			text: `IMPACT database:orders DEPTH 4 END KIND service LIMIT 100`,
			want: Request{Op: "impact", ID: "database:orders", Depth: 4, Limit: 100},
		},
		{
			name: "shortest",
			text: `SHORTEST service:checkout TO database:orders OUT REL depends_on DEPTH 6`,
			want: Request{Op: "shortest_path", ID: "service:checkout", TargetID: "database:orders", Direction: "out", RelationTypes: []string{"depends_on"}, Depth: 6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGQL(tt.text)
			if err != nil {
				t.Fatalf("parse gql: %v", err)
			}
			if got.Op != tt.want.Op || got.ID != tt.want.ID || got.TargetID != tt.want.TargetID || got.Direction != tt.want.Direction || got.Depth != tt.want.Depth || got.Limit != tt.want.Limit {
				t.Fatalf("request = %#v want core %#v", got, tt.want)
			}
			if len(tt.want.RelationTypes) > 0 && got.RelationTypes[0] != tt.want.RelationTypes[0] {
				t.Fatalf("relation types = %#v", got.RelationTypes)
			}
		})
	}
}

func TestParseGQLAggregatesExplainProfile(t *testing.T) {
	request, err := ParseGQL(`PROFILE FIND host WHERE hostname PREFIX "app-" AGG count(), avg(cpu) AS avg_cpu LIMIT 10`)
	if err != nil {
		t.Fatalf("parse profile: %v", err)
	}
	if request.Op != "profile" || request.TargetOp != "match" || request.Kind != "host" {
		t.Fatalf("profile request = %#v", request)
	}
	if len(request.Aggregate) != 2 || request.Aggregate[1].Name != "avg_cpu" || request.Aggregate[1].Field != "cpu" {
		t.Fatalf("aggregates = %#v", request.Aggregate)
	}

	explain, err := ParseGQL(`EXPLAIN FIND host WHERE hostname = "app-01"`)
	if err != nil {
		t.Fatalf("parse explain: %v", err)
	}
	if explain.Op != "explain" || explain.TargetOp != "match" {
		t.Fatalf("explain request = %#v", explain)
	}
}

func TestExecuteGQLFind(t *testing.T) {
	g := seedCMDBGraph(t)
	request, err := ParseGQL(`FIND host WHERE cpu >= 8 AND hostname PREFIX "app-" PROJECT id, hostname, cpu ORDER BY cpu DESC LIMIT 1`)
	if err != nil {
		t.Fatalf("parse gql: %v", err)
	}
	response, err := Execute(g, request)
	if err != nil {
		t.Fatalf("execute gql: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("results = %#v", response.Results)
	}
	if response.Results[0].Fields["cpu"] != float64(16) {
		t.Fatalf("projection = %#v", response.Results[0].Fields)
	}
}

func TestParseGQLRejectsInvalidSyntax(t *testing.T) {
	if _, err := ParseGQL(`FIND host WHERE cpu BETWEEN 1 AND 2`); err == nil {
		t.Fatal("expected invalid filter op")
	}
	if _, err := ParseGQL(`SHORTEST service:a database:b`); err == nil {
		t.Fatal("expected missing TO error")
	}
}
