package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/adminui"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound7WriteStorageErrorMapsBackpressureAndSetsRetryAfter(t *testing.T) {
	pressureErr := &storage.BackpressureError{
		RetryAfter: 3 * time.Second,
		Reasons: []storage.BackpressureReason{
			{Code: "disk_space_low", Message: "disk free space below reserve"},
		},
	}
	rec := httptest.NewRecorder()
	writeStorageError(rec, pressureErr)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("backpressure status = %d, want 429 (body=%s)", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "3" {
		t.Fatalf("Retry-After = %q, want 3", rec.Header().Get("Retry-After"))
	}

	recLimit := httptest.NewRecorder()
	writeStorageError(recLimit, query.ErrLimitExceeded)
	if recLimit.Code != http.StatusTooManyRequests {
		t.Fatalf("query limit status = %d, want 429", recLimit.Code)
	}

	recNotFresh := httptest.NewRecorder()
	writeStorageError(recNotFresh, &readerNotFreshError{VisibleVersion: 1, RequiredVersion: 2})
	if recNotFresh.Code != http.StatusServiceUnavailable {
		t.Fatalf("readerNotFresh status = %d, want 503", recNotFresh.Code)
	}
}

func TestRound7DisableUsageCacheDisablesCaching(t *testing.T) {
	srv := &Server{DisableUsageCache: true}
	if ttl := srv.tenantUsageCacheTTL(); ttl >= 0 {
		t.Fatalf("expected negative TTL when DisableUsageCache=true, got %v", ttl)
	}
	defaultSrv := &Server{}
	if ttl := defaultSrv.tenantUsageCacheTTL(); ttl != defaultTenantUsageCacheTTL {
		t.Fatalf("expected default 60s TTL on zero Server, got %v", ttl)
	}
}

func TestRound7TenantNamedBackupsRoutesToTenantInfo(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "r7-backups-tenant")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "backups", storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store}).Handler()

	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/backups", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/tenants/backups status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"tenant_id":"backups"`) {
		t.Fatalf("expected tenant info for tenant 'backups', got %s", rec.Body.String())
	}

	// Verify trace routing does not misclassify single-segment tenant IDs named after actions.
	if route := tenantAPITraceRoute(http.MethodGet, "/v1/tenants/disable"); route.operation != "tenant.get" {
		t.Fatalf("GET /v1/tenants/disable trace route = %+v, want tenant.get", route)
	}
	if route := tenantAPITraceRoute(http.MethodGet, "/v1/tenants/acme/backups"); route.operation != "tenant.backups" {
		t.Fatalf("GET /v1/tenants/acme/backups trace route = %+v, want tenant.backups", route)
	}
}

func TestRound7ResumedEntityAndEdgeStreamsEmitHeader(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "r7-stream")
	defer store.ShutdownTasks(ctx)
	tenant := "t-stream"
	if _, err := store.CreateTenant(ctx, tenant, storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "h1", Kind: "host"},
			{ID: "h2", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{
			{ID: "e1", Type: "links", From: "h1", To: "h2"},
			{ID: "e2", Type: "links", From: "h2", To: "h1"},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store}).Handler()

	// Get first page entity cursor, then resume streamEntities with that cursor.
	pageReq := httptest.NewRequest(http.MethodGet, "/v1/entities?limit=1", nil)
	pageReq.Header.Set("X-Tenant-ID", tenant)
	pageRec := httptest.NewRecorder()
	handler.ServeHTTP(pageRec, pageReq)
	var entityPage struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(pageRec.Body.Bytes(), &entityPage); err != nil || entityPage.NextCursor == "" {
		t.Fatalf("expected entity next_cursor, got status=%d body=%s", pageRec.Code, pageRec.Body.String())
	}

	streamReq := httptest.NewRequest(http.MethodGet, "/v1/entities/stream?cursor="+entityPage.NextCursor, nil)
	streamReq.Header.Set("X-Tenant-ID", tenant)
	streamRec := httptest.NewRecorder()
	handler.ServeHTTP(streamRec, streamReq)
	if streamRec.Code != http.StatusOK {
		t.Fatalf("resumed entity stream status = %d", streamRec.Code)
	}
	firstLine, _ := bufio.NewReader(streamRec.Body).ReadString('\n')
	if !strings.Contains(firstLine, `"stream":true`) || !strings.Contains(firstLine, `"resource":"entity"`) {
		t.Fatalf("expected resumed entity stream to emit initial header frame, got %q", firstLine)
	}

	// Same check for edges stream.
	edgePageReq := httptest.NewRequest(http.MethodGet, "/v1/edges?limit=1", nil)
	edgePageReq.Header.Set("X-Tenant-ID", tenant)
	edgePageRec := httptest.NewRecorder()
	handler.ServeHTTP(edgePageRec, edgePageReq)
	var edgePage struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(edgePageRec.Body.Bytes(), &edgePage); err != nil || edgePage.NextCursor == "" {
		t.Fatalf("expected edge next_cursor, got status=%d body=%s", edgePageRec.Code, edgePageRec.Body.String())
	}
	edgeStreamReq := httptest.NewRequest(http.MethodGet, "/v1/edges/stream?cursor="+edgePage.NextCursor, nil)
	edgeStreamReq.Header.Set("X-Tenant-ID", tenant)
	edgeStreamRec := httptest.NewRecorder()
	handler.ServeHTTP(edgeStreamRec, edgeStreamReq)
	if edgeStreamRec.Code != http.StatusOK {
		t.Fatalf("resumed edge stream status = %d", edgeStreamRec.Code)
	}
	edgeFirstLine, _ := bufio.NewReader(edgeStreamRec.Body).ReadString('\n')
	if !strings.Contains(edgeFirstLine, `"stream":true`) || !strings.Contains(edgeFirstLine, `"resource":"edge"`) {
		t.Fatalf("expected resumed edge stream to emit initial header frame, got %q", edgeFirstLine)
	}
}

func TestRound7AdminUIUUIDFallbackPresent(t *testing.T) {
	uiHandler := adminui.Handler()
	for _, asset := range []string{"/ui/data.js", "/ui/operations.js"} {
		req := httptest.NewRequest(http.MethodGet, asset, nil)
		rec := httptest.NewRecorder()
		uiHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d", asset, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "crypto.randomUUID()") {
			t.Fatalf("%s still calls crypto.randomUUID() directly without non-HTTPS fallback", asset)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/ui/api.js", nil)
	rec := httptest.NewRecorder()
	uiHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "export function uuid()") {
		t.Fatalf("api.js missing exported uuid() fallback helper")
	}
}
