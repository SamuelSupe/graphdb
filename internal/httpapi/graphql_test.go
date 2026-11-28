package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
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

func TestQueryEntrypointsPreserveProjectedSortedPages(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "host:d", Kind: "host", Fields: graph.Fields{"rank": 1, "name": "delta"}},
		{ID: "host:c", Kind: "host", Fields: graph.Fields{"rank": 3, "name": "charlie"}},
		{ID: "host:b", Kind: "host", Fields: graph.Fields{"rank": 0, "name": "excluded"}},
		{ID: "host:a", Kind: "host", Fields: graph.Fields{"rank": 3, "name": "alpha"}},
	}}
	if _, err := store.Commit(context.Background(), "tenant-a", mutations, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	const text = `FIND host WHERE rank >= 1 PROJECT id, name ORDER BY rank DESC LIMIT 2`
	const document = `query Search($request: QueryRequest!) {
		lookup: graph(request: $request) { version results next_cursor: nextCursor }
	}`
	for _, tc := range []struct {
		path    string
		body    func(query.Request) any
		graphql bool
		stream  bool
	}{
		{"/v1/query", func(r query.Request) any { return r }, false, false},
		{"/v1/query/stream", func(r query.Request) any { return r }, false, true},
		{"/v1/query/gql", func(r query.Request) any { return map[string]any{"query": text, "cursor": r.Cursor} }, false, false},
		{"/v1/query/gql/stream", func(r query.Request) any { return map[string]any{"query": text, "cursor": r.Cursor} }, false, true},
		{"/v1/query/graphql", func(r query.Request) any {
			return map[string]any{"query": document, "variables": map[string]any{"request": r}}
		}, true, false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			request := query.Request{
				Op: "match", Kind: "host", Where: []query.Filter{{Field: "rank", Op: "gte", Value: 1}},
				Project: []string{"id", "name"}, Sort: []query.SortSpec{{Field: "rank", Desc: true}}, Limit: 2,
			}
			for page, wantIDs := range [][]string{{"host:a", "host:c"}, {"host:d"}} {
				response := serveJSON(handler, http.MethodPost, tc.path, "tenant-a", tc.body(request))
				if response.Code != http.StatusOK {
					t.Fatalf("page %d: %d %s", page, response.Code, response.Body)
				}
				decoder := json.NewDecoder(response.Body)
				var observed query.Response
				switch {
				case tc.stream:
					var first query.StreamMeta
					if err := decoder.Decode(&first); err != nil || first.Done || first.Version != 1 {
						t.Fatalf("page %d stream header = %+v, %v", page, first, err)
					}
					for range wantIDs {
						var row query.Result
						if err := decoder.Decode(&row); err != nil {
							t.Fatal(err)
						}
						observed.Results = append(observed.Results, row)
					}
					var end query.StreamMeta
					if err := decoder.Decode(&end); err != nil || !end.Done || end.Stats == nil || end.Stats.Returned != len(wantIDs) {
						t.Fatalf("page %d stream tail = %+v, %v", page, end, err)
					}
					observed.Version, observed.NextCursor = end.Version, end.NextCursor
				case tc.graphql:
					var envelope struct {
						Data   map[string]query.Response
						Errors []json.RawMessage
					}
					if err := decoder.Decode(&envelope); err != nil || len(envelope.Errors) > 0 {
						t.Fatalf("page %d GraphQL response = %+v, %v", page, envelope, err)
					}
					observed = envelope.Data["lookup"]
				default:
					if err := decoder.Decode(&observed); err != nil {
						t.Fatal(err)
					}
				}
				ids := make([]string, 0, len(observed.Results))
				for _, row := range observed.Results {
					if row.Entity == nil || len(row.Entity.Fields) != 1 || len(row.Fields) != 2 || row.Fields["id"] != row.Entity.ID {
						t.Fatalf("page %d invalid projected row: %+v", page, row)
					}
					name, ok := row.Entity.Fields["name"].(string)
					if !ok || row.Fields["name"] != name {
						t.Fatalf("page %d name mismatch: %+v", page, row)
					}
					ids = append(ids, row.Entity.ID)
				}
				if !slices.Equal(ids, wantIDs) || observed.Version != 1 || (observed.NextCursor != "") != (page == 0) {
					t.Fatalf("page %d IDs = %v, want %v, version = %d, cursor = %q", page, ids, wantIDs, observed.Version, observed.NextCursor)
				}
				request.Cursor = observed.NextCursor
			}
		})
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
