package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"graphdb/internal/graph"
	"graphdb/internal/query"
	"graphdb/internal/storage"
)

func TestHTTPGQLFullCMDBUseCases(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	seedFullGQLCMDBTenant(t, handler)

	t.Run("find boolean projection sort aggregate group", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			FIND host
			WHERE (cpu >= 16 OR hostname = "db-01") AND source_priority >= 100
			PROJECT id, hostname, cpu, source, source_priority
			ORDER BY cpu DESC
			GROUP BY owner
			AGG count(), avg(cpu) AS avg_cpu
			HAVING count >= 1
			LIMIT 10
		`})
		assertResultIDs(t, response, []string{"host:db-01", "host:app-02"})
		if len(response.Groups) != 2 {
			t.Fatalf("groups = %#v, want platform and dba", response.Groups)
		}
	})

	t.Run("neighbors edge where", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			NEIGHBORS service:checkout OUT
			REL runs_on
			EDGE WHERE status = "active" AND source_priority >= 100
			PROJECT id, hostname
			LIMIT 10
		`})
		assertResultIDs(t, response, []string{"host:app-01"})
		if response.Results[0].Edge == nil || response.Results[0].Edge.Fields["status"] != "active" {
			t.Fatalf("edge = %#v", response.Results[0].Edge)
		}
	})

	t.Run("traverse path steps and edge filters", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			TRAVERSE service:checkout OUT
			DEPTH 2
			PATH
			STEP REL depends_on NODE service EDGE WHERE status = "active"
			STEP REL depends_on NODE database EDGE WHERE protocol = "tcp"
			END KIND database
			LIMIT 10
		`})
		if len(response.Results) != 1 || response.Results[0].Path == nil || pathEndID(response.Results[0].Path) != "database:orders" {
			t.Fatalf("traverse results = %#v", response.Results)
		}
	})

	t.Run("impact reverse dependency", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			IMPACT database:orders
			DEPTH 3
			END KIND service
			LIMIT 10
		`})
		assertPathEndIDs(t, response, []string{"service:api", "service:checkout"})
	})

	t.Run("shortest path", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			SHORTEST service:checkout TO database:orders
			OUT
			REL depends_on
			DEPTH 4
		`})
		if len(response.Results) != 1 || response.Results[0].Path == nil || len(response.Results[0].Path.Edges) != 2 {
			t.Fatalf("shortest results = %#v", response.Results)
		}
	})

	t.Run("explain and profile", func(t *testing.T) {
		explain := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `EXPLAIN FIND host WHERE hostname PREFIX "app-"`})
		if explain.Plan == nil || explain.Plan.Op != "match" {
			t.Fatalf("explain plan = %#v", explain.Plan)
		}
		profile := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `PROFILE FIND host WHERE hostname = "app-01" LIMIT 1`})
		if profile.Plan == nil || len(profile.Profile) == 0 {
			t.Fatalf("profile response plan=%#v profile=%#v", profile.Plan, profile.Profile)
		}
	})

	t.Run("pagination cursor", func(t *testing.T) {
		first := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `FIND host ORDER BY hostname LIMIT 1`})
		if len(first.Results) != 1 || first.NextCursor == "" {
			t.Fatalf("first page = %#v cursor=%q", first.Results, first.NextCursor)
		}
		second := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `FIND host ORDER BY hostname LIMIT 1`, Cursor: first.NextCursor})
		if len(second.Results) != 1 || second.Results[0].Entity.ID == first.Results[0].Entity.ID {
			t.Fatalf("second page = %#v first=%#v", second.Results, first.Results)
		}
	})

	t.Run("literal values and sort alias", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			FIND host
			WHERE (maintenance = false OR nullable = null) AND load GTE 0.75 AND offset LTE 0
			PROJECT id, hostname, maintenance, nullable, load, offset
			SORT BY hostname ASC
			LIMIT 10
		`})
		assertResultIDs(t, response, []string{"host:app-01", "host:app-02"})
	})

	t.Run("in and both direction neighbors", func(t *testing.T) {
		incoming := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			NEIGHBORS host:app-01 IN
			REL runs_on
			LIMIT 10
		`})
		assertResultIDs(t, incoming, []string{"service:checkout"})

		both := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			NEIGHBORS service:api BOTH
			REL depends_on, runs_on
			ORDER BY id
			LIMIT 10
		`})
		assertResultIDs(t, both, []string{"database:orders", "host:app-02", "service:checkout"})
	})

	t.Run("path node filters and end where", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			TRAVERSE service:checkout OUT
			DEPTH 2
			PATH RELATIONS depends_on NODES service, database
			STEP REL depends_on NODE service WHERE tier = "backend" EDGE WHERE status = "active"
			STEP REL depends_on NODE database WHERE engine = "postgres" EDGE WHERE protocol = "tcp"
			END WHERE criticality = "high"
			LIMIT 10
		`})
		if len(response.Results) != 1 || response.Results[0].Path == nil || pathEndID(response.Results[0].Path) != "database:orders" {
			t.Fatalf("path filter results = %#v", response.Results)
		}
	})

	t.Run("shortest path with path pruning", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			SHORTEST service:checkout TO database:orders OUT
			DEPTH 4
			PATH
			STEP REL depends_on NODE service WHERE tier = "backend"
			STEP REL depends_on NODE database EDGE WHERE protocol = "tcp"
			END WHERE engine = "postgres"
		`})
		if len(response.Results) != 1 || response.Results[0].Path == nil || len(response.Results[0].Path.Edges) != 2 {
			t.Fatalf("shortest pruned results = %#v", response.Results)
		}
	})

	t.Run("aggregate aliases and keyword operators", func(t *testing.T) {
		response := postGQL(t, handler, "tenant-a", GQLQueryRequest{Query: `
			FIND host
			WHERE source_priority GTE 100
			GROUP BY region
			AGGREGATE count() AS total, max(cpu) AS max_cpu
			HAVING total GTE 2 AND max_cpu GTE 16
			LIMIT 10
		`})
		if len(response.Groups) != 1 || response.Groups[0].Key["region"] != "us-east-1" {
			t.Fatalf("groups = %#v", response.Groups)
		}
		if response.Groups[0].Aggregates["total"] != float64(3) || response.Groups[0].Aggregates["max_cpu"] != float64(32) {
			t.Fatalf("group aggregates = %#v", response.Groups[0].Aggregates)
		}
	})
}

func TestHTTPGQLStreamFreshnessAndErrorBoundaries(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	seedFullGQLCMDBTenant(t, handler)

	stream := serveJSON(handler, http.MethodPost, "/v1/query/gql/stream", "tenant-a", GQLQueryRequest{
		Query: `FIND host GROUP BY owner AGG count() HAVING count >= 1 LIMIT 10`,
	})
	if stream.Code != http.StatusOK || !strings.Contains(stream.Body.String(), `"groups"`) || !strings.Contains(stream.Body.String(), `"done":true`) {
		t.Fatalf("stream = %d body=%s", stream.Code, stream.Body.String())
	}

	notFresh := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{
		Query:      `FIND host LIMIT 1`,
		MinVersion: 99,
	})
	if notFresh.Code != http.StatusServiceUnavailable || !strings.Contains(notFresh.Body.String(), `"code":"reader_not_fresh"`) {
		t.Fatalf("reader freshness = %d body=%s", notFresh.Code, notFresh.Body.String())
	}

	invalid := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{Query: `FIND host WHERE cpu BETWEEN 1 AND 2`})
	if invalid.Code != http.StatusUnprocessableEntity || !strings.Contains(invalid.Body.String(), `"code":"invalid_query"`) {
		t.Fatalf("invalid syntax = %d body=%s", invalid.Code, invalid.Body.String())
	}

	limited := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{
		Query:     `FIND host LIMIT 10`,
		CostLimit: 1,
	})
	if limited.Code != http.StatusTooManyRequests || !strings.Contains(limited.Body.String(), `"code":"query_limit_exceeded"`) {
		t.Fatalf("cost limited = %d body=%s", limited.Code, limited.Body.String())
	}

	profile := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{
		Query:   `FIND host LIMIT 1`,
		Profile: true,
	})
	if profile.Code != http.StatusOK || !strings.Contains(profile.Body.String(), `"profile"`) || !strings.Contains(profile.Body.String(), `"plan"`) {
		t.Fatalf("profile flag = %d body=%s", profile.Code, profile.Body.String())
	}

	badTimeout := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{
		Query:     `FIND host LIMIT 1`,
		TimeoutMS: -1,
	})
	if badTimeout.Code != http.StatusUnprocessableEntity || !strings.Contains(badTimeout.Body.String(), `"code":"invalid_query"`) {
		t.Fatalf("bad timeout = %d body=%s", badTimeout.Code, badTimeout.Body.String())
	}
}

func seedFullGQLCMDBTenant(t *testing.T, handler http.Handler) {
	t.Helper()
	rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "depends_on", FromKinds: []string{"service"}, ToKinds: []string{"service", "database"}, Directed: true, Cardinality: graph.ManyToMany, ImpactDirection: "reverse"},
			{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true, Cardinality: graph.ManyToMany, ImpactDirection: "reverse"},
		},
		UpsertEntities: []graph.Entity{
			{ID: "service:checkout", Kind: "service", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"name": "checkout", "owner": "platform", "tier": "frontend"}},
			{ID: "service:api", Kind: "service", Source: "agent", SourceRank: 100, Fields: graph.Fields{"name": "api", "owner": "platform", "tier": "backend"}},
			{ID: "database:orders", Kind: "database", Source: "cloud", SourceRank: 50, Fields: graph.Fields{"name": "orders", "engine": "postgres", "criticality": "high"}},
			{ID: "host:app-01", Kind: "host", Source: "agent", SourceRank: 100, Fields: graph.Fields{"hostname": "app-01", "cpu": 8, "region": "us-east-1", "owner": "platform", "maintenance": false, "load": 0.75, "offset": -1}},
			{ID: "host:app-02", Kind: "host", Source: "agent", SourceRank: 100, Fields: graph.Fields{"hostname": "app-02", "cpu": 16, "region": "us-east-1", "owner": "platform", "maintenance": true, "nullable": nil, "load": 1.25, "offset": -2}},
			{ID: "host:db-01", Kind: "host", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"hostname": "db-01", "cpu": 32, "region": "us-east-1", "owner": "dba", "maintenance": true, "load": 2.0, "offset": 1}},
			{ID: "host:stale-01", Kind: "host", Source: "agent", SourceRank: 100, Fields: graph.Fields{"hostname": "stale-01", "cpu": 4, "region": "us-west-2", "owner": "platform", "maintenance": false, "load": 0.25, "offset": -1}},
		},
		UpsertEdges: []graph.Edge{
			{ID: "edge:checkout-api", Type: "depends_on", From: "service:checkout", To: "service:api", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"status": "active", "protocol": "http"}},
			{ID: "edge:api-orders", Type: "depends_on", From: "service:api", To: "database:orders", Source: "agent", SourceRank: 100, Fields: graph.Fields{"status": "active", "protocol": "tcp"}},
			{ID: "edge:checkout-host", Type: "runs_on", From: "service:checkout", To: "host:app-01", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"status": "active"}},
			{ID: "edge:checkout-stale", Type: "runs_on", From: "service:checkout", To: "host:stale-01", Source: "agent", SourceRank: 100, Fields: graph.Fields{"status": "stale"}},
			{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-02", Source: "agent", SourceRank: 100, Fields: graph.Fields{"status": "active"}},
		},
	}})
	if rr.Code != http.StatusOK {
		t.Fatalf("seed full gql cmdb = %d body=%s", rr.Code, rr.Body.String())
	}
}

func postGQL(t *testing.T, handler http.Handler, tenantID string, request GQLQueryRequest) query.Response {
	t.Helper()
	rr := serveJSON(handler, http.MethodPost, "/v1/query/gql", tenantID, request)
	if rr.Code != http.StatusOK {
		t.Fatalf("gql query status = %d body=%s", rr.Code, rr.Body.String())
	}
	var response query.Response
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode gql response: %v body=%s", err, rr.Body.String())
	}
	return response
}

func assertResultIDs(t *testing.T, response query.Response, want []string) {
	t.Helper()
	if len(response.Results) != len(want) {
		t.Fatalf("result count = %d want %d results=%#v", len(response.Results), len(want), response.Results)
	}
	for i, id := range want {
		if response.Results[i].Entity == nil || response.Results[i].Entity.ID != id {
			t.Fatalf("result %d = %#v want entity %q", i, response.Results[i], id)
		}
	}
}

func assertPathEndIDs(t *testing.T, response query.Response, want []string) {
	t.Helper()
	if len(response.Results) != len(want) {
		t.Fatalf("path result count = %d want %d results=%#v", len(response.Results), len(want), response.Results)
	}
	for i, id := range want {
		if response.Results[i].Path == nil || pathEndID(response.Results[i].Path) != id {
			t.Fatalf("path result %d = %#v want end %q", i, response.Results[i], id)
		}
	}
}

func pathEndID(path *graph.Path) string {
	if path == nil || len(path.Entities) == 0 {
		return ""
	}
	return path.Entities[len(path.Entities)-1].ID
}
