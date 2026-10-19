package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
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

func TestGraphQLExplainAndProfileWrappers(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	for _, op := range []string{"explain", "profile"} {
		response := serveJSON(handler, http.MethodPost, "/v1/query/graphql", "tenant-a", map[string]any{
			"query":     `query Q($request: QueryRequest!) { graph(request: $request) { plan profile results } }`,
			"variables": map[string]any{"request": map[string]any{"op": op, "targetOp": "match", "kind": "host"}},
		})
		var body struct {
			Data map[string]struct {
				Plan    *query.Plan          `json:"plan"`
				Profile []query.OperatorStat `json:"profile"`
				Results []query.Result       `json:"results"`
			} `json:"data"`
			Errors json.RawMessage `json:"errors"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		root := body.Data["graph"]
		if response.Code != 200 || len(body.Errors) > 0 || root.Plan == nil {
			t.Fatalf("%s: %d %s", op, response.Code, response.Body.String())
		}
		if op == "profile" && (len(root.Profile) == 0 || len(root.Results) != 1) {
			t.Fatalf("profile: %s", response.Body.String())
		}
	}
}
