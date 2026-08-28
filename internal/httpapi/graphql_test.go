package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

type recordingEvidenceSearcher struct {
	tenantID string
	request  retrieval.SearchRequest
	response retrieval.SearchResponse
	err      error
}

func (s *recordingEvidenceSearcher) SearchEvidence(
	_ context.Context,
	tenantID string,
	request retrieval.SearchRequest,
) (retrieval.SearchResponse, error) {
	s.tenantID = tenantID
	s.request = request
	return s.response, s.err
}

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

func TestGraphQLEvidenceSearchEndpoint(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("init: %v", err)
	}
	searcher := &recordingEvidenceSearcher{response: retrieval.SearchResponse{
		Version:             12,
		RetrievalRevision:   15,
		EmbeddingGeneration: "model-v2",
		Evidence: []retrieval.Evidence{{
			Rank:   1,
			ID:     "chunk:checkout",
			Score:  0.91,
			Scores: retrieval.ScoreBreakdown{Vector: 0.8, Lexical: 0.5, Fusion: 0.91},
		}},
		Stats: retrieval.SearchStats{
			VectorCandidates:  100,
			LexicalCandidates: 80,
			Returned:          1,
		},
	}}
	handler := (&Server{
		Store:             store,
		Mode:              "all",
		RetrievalSearcher: searcher,
	}).Handler()
	response := serveJSON(handler, http.MethodPost, "/v1/query/graphql", "tenant-a", map[string]any{
		"query": `query Search($input: EvidenceSearchInput!) {
			answer: evidenceSearch(input: $input) {
				version
				retrievalRevision
				embeddingGeneration
				evidence
				stats
			}
		}`,
		"operationName": "Search",
		"variables": map[string]any{
			"input": map[string]any{
				"query": "why did checkout fail?",
				"topK":  5,
			},
		},
	})
	body := response.Body.String()
	if response.Code != http.StatusOK ||
		!strings.Contains(body, `"answer"`) ||
		!strings.Contains(body, `"version":12`) ||
		!strings.Contains(body, `"retrievalRevision":15`) ||
		!strings.Contains(body, `"embeddingGeneration":"model-v2"`) ||
		!strings.Contains(body, `"chunk:checkout"`) {
		t.Fatalf("GraphQL evidence response = %d %s", response.Code, body)
	}
	if response.Header().Get("X-GraphDB-Query-ID") == "" {
		t.Fatal("GraphQL evidence response did not include query id")
	}
	if searcher.tenantID != "tenant-a" ||
		searcher.request.Query != "why did checkout fail?" ||
		searcher.request.TopK != 5 ||
		searcher.request.Expansion == nil ||
		searcher.request.Expansion.Depth() != retrieval.DefaultMaxDepth {
		t.Fatalf("search request = %#v tenant=%q", searcher.request, searcher.tenantID)
	}
}

func TestGraphQLEvidenceSearchReportsNotReady(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("init: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	response := serveJSON(handler, http.MethodPost, "/v1/query/graphql", "tenant-a", map[string]any{
		"query": `{ evidenceSearch(input: {query: "why?"}) { version } }`,
	})
	body := response.Body.String()
	if response.Code != http.StatusOK ||
		!strings.Contains(body, `"data":{"evidenceSearch":null}`) ||
		!strings.Contains(body, `"code":"retrieval_not_ready"`) ||
		!strings.Contains(body, `"httpStatus":503`) {
		t.Fatalf("GraphQL retrieval-not-ready = %d %s", response.Code, body)
	}
}
