package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestGraphQLQueryEndpoint(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID:     "host:1",
			Kind:   "host",
			Fields: graph.Fields{"hostname": "app-01"},
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	response := serveJSON(handler, http.MethodPost, "/v1/query/graphql", "tenant-a", map[string]any{
		"query": `query Find($request: QueryRequest!) {
			graph(request: $request) {
				version
				results
				stats
			}
		}`,
		"operationName": "Find",
		"variables": map[string]any{
			"request": map[string]any{"op": "match", "kind": "host", "limit": 10},
		},
	})
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"data"`) ||
		!strings.Contains(response.Body.String(), `"host:1"`) ||
		!strings.Contains(response.Body.String(), `"returned":1`) {
		t.Fatalf("GraphQL response = %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-GraphDB-Query-ID") == "" {
		t.Fatal("GraphQL response did not include query id")
	}
}

func TestGraphQLRejectsLegacyTextSyntax(t *testing.T) {
	handler := (&Server{
		Store: storage.NewTenantStore(storage.NewMemoryStore(), "test"),
		Mode:  "all",
	}).Handler()
	response := serveJSON(handler, http.MethodPost, "/v1/query/graphql", "tenant-a", map[string]any{
		"query": `FIND host LIMIT 10`,
	})
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), `"errors"`) {
		t.Fatalf("GraphQL invalid response = %d %s", response.Code, response.Body.String())
	}
}

func TestGraphQLExecutionErrorUsesGraphQLEnvelope(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("init: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	response := serveJSON(handler, http.MethodPost, "/v1/query/graphql", "tenant-a", map[string]any{
		"query": `{ graph(request: {op: "match", minVersion: 2}) { version } }`,
	})
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"data":{"graph":null}`) ||
		!strings.Contains(response.Body.String(), `"errors"`) ||
		!strings.Contains(response.Body.String(), `"code":"reader_not_fresh"`) {
		t.Fatalf("GraphQL execution error = %d %s", response.Code, response.Body.String())
	}
}
