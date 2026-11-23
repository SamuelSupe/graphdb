package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestObserveHTTPRecoversPanicWithJSON500(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	server := &Server{Store: store, Cache: storage.NewReaderCache(store, 0), Mode: "all"}
	handler := server.observeHTTP(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("expected JSON error body, got %q: %v", rec.Body.String(), err)
	}
	if payload["error"] == "" {
		t.Fatalf("missing error field: %+v", payload)
	}
}

func TestStreamEntitiesWritesTerminalErrorFrameOnCancel(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}},
		UpsertEntities: []graph.Entity{
			{ID: "host:1", Kind: "host"},
			{ID: "host:2", Kind: "host"},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	server := &Server{Store: store, Cache: storage.NewReaderCache(store, 0), Mode: "all"}
	streamCtx, cancel := context.WithCancel(ctx)
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/entities/stream", nil).WithContext(streamCtx)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code == http.StatusOK && !strings.Contains(rec.Body.String(), `"type":"error"`) {
		t.Fatalf("expected terminal error frame on canceled stream, got status=%d body=%q", rec.Code, rec.Body.String())
	}
}
