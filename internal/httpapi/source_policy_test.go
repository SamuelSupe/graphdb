package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPSourcePolicyAndCommitSuppressed(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	policy := graph.SourcePolicy{DefaultPriority: 0, Sources: []graph.SourcePolicyItem{
		{Name: "manual", Priority: 1000},
		{Name: "aws", Priority: 50},
	}}
	if rr := serveJSON(handler, http.MethodPut, "/v1/source-policy", "tenant-a", policy); rr.Code != http.StatusOK {
		t.Fatalf("put source policy = %d body=%s", rr.Code, rr.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/source-policy", nil)
	get.Header.Set("X-Tenant-ID", "tenant-b")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"configured":false`) {
		t.Fatalf("tenant-b source policy = %d body=%s", rr.Code, rr.Body.String())
	}
	manual := CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "manual", Fields: graph.Fields{"owner": "platform"},
	}}}}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", manual); rr.Code != http.StatusOK {
		t.Fatalf("manual commit = %d body=%s", rr.Code, rr.Body.String())
	}
	aws := CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:1", Kind: "host", Source: "aws", Fields: graph.Fields{"owner": "ec2"},
	}}}}
	rr = serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", aws)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"suppressed"`) {
		t.Fatalf("aws commit = %d body=%s", rr.Code, rr.Body.String())
	}
	edgeSeed := CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:1", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{{
			ID: "manual-edge", Type: "runs_on", From: "service:api", To: "host:1",
			Source: "manual", Fields: graph.Fields{"note": "manual"},
		}},
	}}
	edgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:1")
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", edgeSeed); rr.Code != http.StatusOK {
		t.Fatalf("manual edge commit = %d body=%s", rr.Code, rr.Body.String())
	} else if !strings.Contains(rr.Body.String(), `"canonical_edges"`) || !strings.Contains(rr.Body.String(), edgeID) {
		t.Fatalf("manual edge commit missing canonical edge = %d body=%s", rr.Code, rr.Body.String())
	}
	edgeConflict := CommitRequest{Mutations: graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "aws-edge", Type: "runs_on", From: "service:api", To: "host:1",
		Source: "aws", Fields: graph.Fields{"note": "collector"},
	}}}}
	rr = serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", edgeConflict)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"resource_type":"edge"`) || !strings.Contains(rr.Body.String(), edgeID) {
		t.Fatalf("edge conflict = %d body=%s", rr.Code, rr.Body.String())
	}
	reader := (&Server{Store: storage.NewTenantStore(storage.NewMemoryStore(), "test"), Mode: "reader"}).Handler()
	if rr := serveJSON(reader, http.MethodPut, "/v1/source-policy", "tenant-a", policy); rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("reader policy put = %d body=%s", rr.Code, rr.Body.String())
	}
}
