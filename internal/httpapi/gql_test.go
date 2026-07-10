package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPGQLQueryJSON(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	seedHTTPGQLTenant(t, handler)

	rr := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{
		Query: `FIND host WHERE hostname PREFIX "app-" PROJECT id, hostname, cpu ORDER BY cpu DESC LIMIT 1`,
	})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"host:app-02"`) || !strings.Contains(rr.Body.String(), `"version":1`) {
		t.Fatalf("gql query = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPGQLQueryTextBodies(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	seedHTTPGQLTenant(t, handler)

	for _, contentType := range []string{"text/plain", "application/gql"} {
		t.Run(contentType, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/query/gql", strings.NewReader(`FIND host WHERE cpu >= 16 LIMIT 10`))
			req.Header.Set("X-Tenant-ID", "tenant-a")
			req.Header.Set("Content-Type", contentType)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"host:app-02"`) {
				t.Fatalf("gql text query = %d body=%s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestHTTPGQLStreamIncludesGroups(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	seedHTTPGQLTenant(t, handler)

	rr := serveJSON(handler, http.MethodPost, "/v1/query/gql/stream", "tenant-a", GQLQueryRequest{
		Query: `FIND host GROUP BY kind AGG count() HAVING count >= 2 LIMIT 10`,
	})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"groups"`) || !strings.Contains(rr.Body.String(), `"count":2`) {
		t.Fatalf("gql stream = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPGQLInvalidSyntax(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query/gql", "tenant-a", GQLQueryRequest{Query: `FIND host WHERE cpu BETWEEN 1 AND 2`})
	if rr.Code != http.StatusUnprocessableEntity || !strings.Contains(rr.Body.String(), `"code":"invalid_query"`) {
		t.Fatalf("invalid gql = %d body=%s", rr.Code, rr.Body.String())
	}
}

func seedHTTPGQLTenant(t *testing.T, handler http.Handler) {
	t.Helper()
	rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "cpu": 8}},
			{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "cpu": 16}},
		},
	}})
	if rr.Code != http.StatusOK {
		t.Fatalf("seed commit = %d body=%s", rr.Code, rr.Body.String())
	}
}
