package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPCommitGetAndTenantIsolation(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()

	body := CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:alice", Kind: "person", Fields: graph.Fields{"name": "Alice"}}},
	}}
	requestJSON, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewReader(requestJSON))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("commit status = %d body=%s", rr.Code, rr.Body.String())
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/entities/person:alice", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", rr.Code, rr.Body.String())
	}

	isolated := httptest.NewRequest(http.MethodGet, "/v1/entities/person:alice", nil)
	isolated.Header.Set("X-Tenant-ID", "tenant-b")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, isolated)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("tenant isolation status = %d, want 404", rr.Code)
	}
}

func TestHTTPOpenAPIContractRoute(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Header().Get("Content-Type"), "application/yaml") {
		t.Fatalf("openapi status=%d content-type=%q body=%s", rr.Code, rr.Header().Get("Content-Type"), rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "/v1/commits:") || !strings.Contains(body, "WriteBackpressureError:") {
		t.Fatalf("openapi body missing required contract snippets: %s", body)
	}
}

func TestHTTPCommitReturnsIndexWarnings(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}},
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	store.Objects = &httpFailPutStore{ObjectStore: objects, contains: "/indexes/catalog.parquet"}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
		}},
	}})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"index_warnings"`) {
		t.Fatalf("commit status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPCommitReturnsReadableVersionAndCanonicalEntities(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{
			Kind: "host", Source: "aws", ExternalID: "i-123", Fields: graph.Fields{"hostname": "app-01"},
		}},
	}})
	if rr.Code != http.StatusOK {
		t.Fatalf("commit status = %d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"readable_version":1`) || !strings.Contains(body, `"canonical_entities"`) {
		t.Fatalf("commit body missing read/canonical fields: %s", body)
	}
}

func TestHTTPCommitSkipsUnchangedContent(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	request := CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}}
	first := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", request)
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"version":1`) || strings.Contains(first.Body.String(), `"skipped":true`) {
		t.Fatalf("first commit status = %d body=%s", first.Code, first.Body.String())
	}
	second := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", request)
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"version":1`) || !strings.Contains(second.Body.String(), `"skipped":true`) || !strings.Contains(second.Body.String(), `"data_md5"`) {
		t.Fatalf("second commit status = %d body=%s", second.Code, second.Body.String())
	}
}

func TestHTTPCommitIdempotencyReplayAndConflict(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	request := CommitRequest{
		IdempotencyKey: "idem-1",
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
		},
	}
	first := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", request)
	if first.Code != http.StatusOK {
		t.Fatalf("first commit status = %d body=%s", first.Code, first.Body.String())
	}
	second := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", request)
	var replay storage.CommitResult
	if err := json.Unmarshal(second.Body.Bytes(), &replay); err != nil {
		t.Fatalf("decode replay: %v body=%s", err, second.Body.String())
	}
	if second.Code != http.StatusOK || replay.Version != 1 || !replay.IdempotentReplay || !replay.Skipped {
		t.Fatalf("replay status = %d result=%#v body=%s", second.Code, replay, second.Body.String())
	}

	conflictRequest := request
	conflictRequest.Mutations = graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}}
	conflict := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", conflictRequest)
	var body ErrorResponse
	if err := json.Unmarshal(conflict.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode conflict: %v body=%s", err, conflict.Body.String())
	}
	if conflict.Code != http.StatusBadRequest || body.Code != "idempotency_conflict" || !strings.Contains(body.Message, "idempotency conflict") {
		t.Fatalf("conflict status = %d body=%#v raw=%s", conflict.Code, body, conflict.Body.String())
	}
}

func TestHTTPGetEntityAllowsEscapedSlashID(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	id := "aws/ec2/i-123"
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: id, Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/entities/"+url.PathEscape(id), "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"`+id+`"`) {
		t.Fatalf("get status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPGetEntityAllowsEscapedSlashIDFromPersistedIndex(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	id := "aws/ec2/i-123"
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: id, Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/entities/"+url.PathEscape(id), "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"id":"`+id+`"`) {
		t.Fatalf("get status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReaderModeRejectsWrites(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewReader([]byte(`{"mutations":{}}`)))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
}

func TestHTTPRepairDryRunAllowedButApplyRejectedInReaderMode(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	dryRun := serveJSON(handler, http.MethodPost, "/v1/control/repair", "tenant-a", map[string]any{})
	if dryRun.Code != http.StatusOK || !strings.Contains(dryRun.Body.String(), `"apply":false`) {
		t.Fatalf("dry-run status = %d body=%s", dryRun.Code, dryRun.Body.String())
	}
	apply := serveJSON(handler, http.MethodPost, "/v1/control/repair", "tenant-a", map[string]any{"apply": true})
	if apply.Code != http.StatusMethodNotAllowed {
		t.Fatalf("apply status = %d body=%s, want 405", apply.Code, apply.Body.String())
	}
}

func TestHTTPIntegrityAuditEndpoint(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/control/integrity-audit", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"status":"ok"`) || !strings.Contains(rr.Body.String(), `"checks"`) {
		t.Fatalf("integrity audit = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReaderLagReportsCachedVisibleVersion(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	cache := storage.NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("cache load: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	handler := (&Server{Store: store, Cache: cache, Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/control/reader-lag", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"manifest_version":2`) || !strings.Contains(rr.Body.String(), `"visible_version":1`) || !strings.Contains(rr.Body.String(), `"lag":1`) ||
		!strings.Contains(rr.Body.String(), `"status":"stale"`) || !strings.Contains(rr.Body.String(), `"replay_status":"pending"`) {
		t.Fatalf("reader lag = %d body=%s", rr.Code, rr.Body.String())
	}
	freshness := serveJSON(handler, http.MethodGet, "/v1/control/reader-freshness", "tenant-a", nil)
	if freshness.Code != http.StatusOK || !strings.Contains(freshness.Body.String(), `"writer_manifest_version":2`) || !strings.Contains(freshness.Body.String(), `"reader_manifest_version":1`) {
		t.Fatalf("reader freshness = %d body=%s", freshness.Code, freshness.Body.String())
	}
}

func TestHTTPReaderFleetReadinessReportsReadyHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/control/reader-fleet-readiness?min_ready=1&max_staleness_ms=30000", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"ready":true`) || !strings.Contains(rr.Body.String(), `"ready_readers":1`) {
		t.Fatalf("fleet readiness = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReaderFleetReadinessAllowsBoundedStaleness(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	cache := storage.NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("cache load: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	handler := (&Server{Store: store, Cache: cache, Mode: "reader"}).Handler()

	rr := serveJSON(handler, http.MethodGet, "/v1/control/reader-fleet-readiness?min_ready=1&max_staleness_ms=30000", "tenant-a", nil)
	var ready ReaderFleetReadinessReport
	if err := json.Unmarshal(rr.Body.Bytes(), &ready); err != nil {
		t.Fatalf("decode ready: %v body=%s", err, rr.Body.String())
	}
	if rr.Code != http.StatusOK || !ready.Ready || ready.ReadyReaders != 1 {
		t.Fatalf("bounded stale readiness = %d report=%#v body=%s", rr.Code, ready, rr.Body.String())
	}

	strict := serveJSON(handler, http.MethodGet, "/v1/control/reader-fleet-readiness?min_ready=1&min_version=2&max_staleness_ms=30000", "tenant-a", nil)
	var strictReport ReaderFleetReadinessReport
	if err := json.Unmarshal(strict.Body.Bytes(), &strictReport); err != nil {
		t.Fatalf("decode strict: %v body=%s", err, strict.Body.String())
	}
	if strict.Code != http.StatusOK || strictReport.Ready || len(strictReport.Readers) != 1 || strictReport.Readers[0].Reason != "version_lag" {
		t.Fatalf("strict readiness = %d report=%#v body=%s", strict.Code, strictReport, strict.Body.String())
	}
}

func TestHTTPReaderTrafficGateDrainsAndRestoresStaleReader(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	cache := storage.NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("cache load: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	handler := (&Server{Store: store, Cache: cache, Mode: "reader"}).Handler()

	drain := serveJSON(handler, http.MethodGet, "/v1/control/reader-traffic-gate?refresh=false", "tenant-a", nil)
	if drain.Code != http.StatusServiceUnavailable ||
		drain.Header().Get("X-GraphDB-Reader-Traffic") != "draining" ||
		!strings.Contains(drain.Body.String(), `"serve_traffic":false`) ||
		!strings.Contains(drain.Body.String(), `"reason":"version_lag"`) {
		t.Fatalf("traffic gate drain = %d headers=%#v body=%s", drain.Code, drain.Header(), drain.Body.String())
	}
	ready := serveJSON(handler, http.MethodGet, "/v1/control/reader-traffic-gate", "tenant-a", nil)
	if ready.Code != http.StatusOK ||
		ready.Header().Get("X-GraphDB-Reader-Traffic") != "ready" ||
		!strings.Contains(ready.Body.String(), `"serve_traffic":true`) ||
		!strings.Contains(ready.Body.String(), `"refresh_success":true`) {
		t.Fatalf("traffic gate ready = %d headers=%#v body=%s", ready.Code, ready.Header(), ready.Body.String())
	}
}

func TestHTTPReaderTrafficGateAllowsExplicitBoundedStaleness(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	cache := storage.NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("cache load: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:2", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	handler := (&Server{Store: store, Cache: cache, Mode: "reader"}).Handler()

	rr := serveJSON(handler, http.MethodGet, "/v1/control/reader-traffic-gate?allow_stale=true&max_staleness_ms=30000", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"allow_stale":true`) || !strings.Contains(rr.Body.String(), `"serve_traffic":true`) {
		t.Fatalf("traffic gate bounded stale = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReaderFleetReadinessUsesIndexCatalogWhenCacheCold(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	cache := storage.NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "service"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	handler := (&Server{Store: store, Cache: cache, Mode: "reader"}).Handler()

	lag := serveJSON(handler, http.MethodGet, "/v1/control/reader-lag", "tenant-a", nil)
	if lag.Code != http.StatusOK || !strings.Contains(lag.Body.String(), `"visible_version":1`) || !strings.Contains(lag.Body.String(), `"replay_status":"indexed_stale"`) {
		t.Fatalf("reader lag = %d body=%s", lag.Code, lag.Body.String())
	}
	rr := serveJSON(handler, http.MethodGet, "/v1/control/reader-fleet-readiness?min_ready=1&max_staleness_ms=30000", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"ready":true`) || !strings.Contains(rr.Body.String(), `"ready_readers":1`) {
		t.Fatalf("fleet readiness = %d body=%s", rr.Code, rr.Body.String())
	}
	strict := serveJSON(handler, http.MethodGet, "/v1/control/reader-fleet-readiness?min_ready=1&min_version=2&max_staleness_ms=30000", "tenant-a", nil)
	if strict.Code != http.StatusOK || !strings.Contains(strict.Body.String(), `"ready":false`) || !strings.Contains(strict.Body.String(), `"reason":"version_lag"`) {
		t.Fatalf("strict readiness = %d body=%s", strict.Code, strict.Body.String())
	}
}

func TestHTTPReaderModeRejectsSavedQueryWrites(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	saved := storage.SavedQuery{Name: "hosts", Request: query.Request{Op: "match", Kind: "host"}}
	rr := serveJSON(handler, http.MethodPost, "/v1/query/templates", "tenant-a", saved)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d body=%s, want 405", rr.Code, rr.Body.String())
	}
	items, err := store.ListSavedQueries(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("list saved queries: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("reader wrote saved queries: %#v", items)
	}
}

func TestHTTPTenantUsageReportsCategories(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/tenant-usage", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"object_count"`) || !strings.Contains(rr.Body.String(), `"name":"commits"`) {
		t.Fatalf("tenant usage = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPTenantUsageFallsBackToStaleCache(t *testing.T) {
	ctx := context.Background()
	objects := &switchListStore{ObjectStore: storage.NewMemoryStore()}
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all", UsageCacheTTL: time.Nanosecond}).Handler()
	first := serveJSON(handler, http.MethodGet, "/v1/tenant-usage", "tenant-a", nil)
	if first.Code != http.StatusOK || strings.Contains(first.Body.String(), `"stale":true`) {
		t.Fatalf("first usage = %d body=%s", first.Code, first.Body.String())
	}
	time.Sleep(time.Millisecond)
	objects.setFail(true)
	second := serveJSON(handler, http.MethodGet, "/v1/tenant-usage", "tenant-a", nil)
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"stale":true`) || !strings.Contains(second.Body.String(), "list failed") {
		t.Fatalf("stale usage = %d body=%s", second.Code, second.Body.String())
	}
}

func TestHTTPRejectsTrailingJSONDocument(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(`{"op":"match"} {"op":"match"}`))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "single JSON document") {
		t.Fatalf("status = %d body=%s, want trailing document rejection", rr.Code, rr.Body.String())
	}
}

func TestHTTPErrorResponseEnvelope(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	req := httptest.NewRequest(http.MethodGet, "/v1/entities/host:a", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	var body ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error: %v body=%s", err, rr.Body.String())
	}
	if rr.Code != http.StatusBadRequest || body.Code != "tenant_required" || body.Message == "" || body.Retryable {
		t.Fatalf("status=%d body=%#v raw=%s", rr.Code, body, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(`{"op":`))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode invalid json error: %v body=%s", err, rr.Body.String())
	}
	if rr.Code != http.StatusBadRequest || body.Code != "invalid_json" {
		t.Fatalf("invalid json status=%d body=%#v raw=%s", rr.Code, body, rr.Body.String())
	}
}

func TestHTTPRejectsOversizedJSONBody(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := `{"default_priority":0,"sources":[{"name":"manual","priority":1000,"description":"` + strings.Repeat("x", maxConfigRequestBytes) + `"}]}`
	req := httptest.NewRequest(http.MethodPut, "/v1/source-policy", strings.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rr.Body.String(), "request body exceeds") {
		t.Fatalf("status = %d body=%s, want 413 body limit", rr.Code, rr.Body.String())
	}
}

func TestHTTPTenantConfigControlsQuota(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	store.Backpressure = storage.NewWritePressure(storage.BackpressureConfig{})
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	one := 1
	config := storage.TenantConfig{Quota: storage.TenantQuotaConfig{MaxEntitiesPerTenant: &one}}

	put := serveJSON(handler, http.MethodPut, "/v1/tenant-config", "tenant-a", config)
	if put.Code != http.StatusOK || !strings.Contains(put.Body.String(), `"configured":true`) {
		t.Fatalf("put tenant config = %d body=%s", put.Code, put.Body.String())
	}
	get := serveJSON(handler, http.MethodGet, "/v1/tenant-config", "tenant-a", nil)
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"max_entities_per_tenant":1`) {
		t.Fatalf("get tenant config = %d body=%s", get.Code, get.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}}); rr.Code != http.StatusOK {
		t.Fatalf("seed commit = %d body=%s", rr.Code, rr.Body.String())
	}
	blocked := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}})
	if blocked.Code != http.StatusTooManyRequests || !strings.Contains(blocked.Body.String(), `"code":"quota_exceeded"`) || !strings.Contains(blocked.Body.String(), `"tenant_entity_quota_exceeded"`) {
		t.Fatalf("blocked commit = %d body=%s", blocked.Code, blocked.Body.String())
	}

	reader := (&Server{Store: store, Mode: "reader"}).Handler()
	denied := serveJSON(reader, http.MethodPut, "/v1/tenant-config", "tenant-a", config)
	if denied.Code != http.StatusMethodNotAllowed {
		t.Fatalf("reader put tenant config = %d body=%s", denied.Code, denied.Body.String())
	}
}

func TestHTTPIngestPartialFailureAndCollectorStatus(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := storage.IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []storage.IngestItem{
			{ExternalID: "i-1", Entity: &graph.Entity{ID: "host:i-1", Kind: "host"}},
			{ExternalID: "empty"},
		},
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", body)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("ingest status = %d body=%s", rr.Code, rr.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/ingest/collectors/aws/collector-a", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"failed_total":1`) {
		t.Fatalf("collector status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPIngestAmbiguousItemIsPartialFailure(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := storage.IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "ambiguous-item",
		Items: []storage.IngestItem{{
			ExternalID: "ambiguous-1",
			Entity:     &graph.Entity{ID: "host:ambiguous", Kind: "host"},
			Edge:       &graph.Edge{ID: "edge:ambiguous", Type: "runs_on", From: "service:api", To: "host:ambiguous"},
		}},
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", body)
	if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "more than one") {
		t.Fatalf("ambiguous ingest = %d body=%s", rr.Code, rr.Body.String())
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("manifest version = %d, want no commit", manifest.Version)
	}
	if _, ok := g.GetEntity("host:ambiguous"); ok {
		t.Fatal("ambiguous entity was committed")
	}
}

func TestHTTPIngestRejectsBatchIDReuseWithDifferentPayload(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	first := storage.IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "batch-reused",
		Items: []storage.IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", first); rr.Code != http.StatusOK {
		t.Fatalf("first ingest = %d body=%s", rr.Code, rr.Body.String())
	}
	second := first
	second.Items = []storage.IngestItem{{
		ExternalID: "i-2",
		Entity:     &graph.Entity{ID: "host:b", Kind: "host"},
	}}
	rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", second)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "ingest record conflict") {
		t.Fatalf("second ingest = %d body=%s, want conflict", rr.Code, rr.Body.String())
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want original version 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("conflicting HTTP ingest committed host:b")
	}
}

func TestHTTPCollectorStatusAllowsEscapedSlashScope(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	source := "aws/prod"
	collectorID := "collector/a"
	body := storage.IngestRequest{
		Source:      source,
		CollectorID: collectorID,
		BatchID:     "batch-1",
		Items: []storage.IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest status = %d body=%s", rr.Code, rr.Body.String())
	}
	path := "/v1/ingest/collectors/" + url.PathEscape(source) + "/" + url.PathEscape(collectorID)
	rr = serveJSON(handler, http.MethodGet, path, "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"source":"`+source+`"`) || !strings.Contains(rr.Body.String(), `"collector_id":"`+collectorID+`"`) {
		t.Fatalf("collector status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPIngestMetadataErrorInvalidatesCache(t *testing.T) {
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(&httpFailPutStore{ObjectStore: objects, contains: "/ingest/aws/idempotency/"}, "test")
	cache := storage.NewReaderCache(store, time.Hour)
	handler := (&Server{Store: store, Cache: cache, Mode: "all"}).Handler()

	get := httptest.NewRequest(http.MethodGet, "/v1/entities/host:i-1", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("initial get status = %d body=%s", rr.Code, rr.Body.String())
	}

	body := storage.IngestRequest{
		Source:         "aws",
		CollectorID:    "collector-a",
		BatchID:        "batch-1",
		IdempotencyKey: "idem-1",
		Items: []storage.IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{ID: "host:i-1", Kind: "host"},
		}},
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", body)
	if rr.Code != http.StatusInternalServerError || !strings.Contains(rr.Body.String(), "save ingest batch") {
		t.Fatalf("ingest status = %d body=%s", rr.Code, rr.Body.String())
	}

	get = httptest.NewRequest(http.MethodGet, "/v1/entities/host:i-1", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK {
		t.Fatalf("get after failed metadata status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPGetEntityFallbackDoesNotUseOlderCacheAfterManifestRefresh(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	reader := storage.NewTenantStore(objects, "test")
	cache := storage.NewReaderCache(reader, time.Hour)
	handler := (&Server{Store: reader, Cache: cache, Mode: "reader"}).Handler()

	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	rr := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("initial get = %d body=%s", rr.Code, rr.Body.String())
	}

	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	rr = serveJSON(handler, http.MethodGet, "/v1/entities/host:b", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"version":2`) {
		t.Fatalf("get after manifest refresh = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReadMinVersionCatchesUpStaleReaderCache(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	reader := storage.NewTenantStore(objects, "test")
	cache := storage.NewReaderCache(reader, time.Hour)
	handler := (&Server{Store: reader, Cache: cache, Mode: "reader"}).Handler()

	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}

	get := httptest.NewRequest(http.MethodGet, "/v1/entities/host:b", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	get.Header.Set("X-GraphDB-Min-Version", "2")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"version":2`) || !strings.Contains(rr.Body.String(), `"id":"host:b"`) {
		t.Fatalf("min version get = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReadMinVersionAboveManifestReturnsReaderNotFresh(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	handler := (&Server{Store: store, Mode: "reader"}).Handler()

	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host", MinVersion: 2})
	if rr.Code != http.StatusServiceUnavailable ||
		rr.Header().Get("Retry-After") == "" ||
		!strings.Contains(rr.Body.String(), `"code":"reader_not_fresh"`) ||
		!strings.Contains(rr.Body.String(), `"visible_version":1`) ||
		!strings.Contains(rr.Body.String(), `"required_version":2`) {
		t.Fatalf("reader_not_fresh = %d headers=%#v body=%s", rr.Code, rr.Header(), rr.Body.String())
	}
}

func TestHTTPQueryAllowStaleCanUseCachedReaderVersion(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	reader := storage.NewTenantStore(objects, "test")
	cache := storage.NewReaderCache(reader, time.Hour)
	handler := (&Server{Store: reader, Cache: cache, Mode: "reader"}).Handler()

	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}

	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host", AllowStale: true})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"version":1`) || strings.Contains(rr.Body.String(), `"id":"host:b"`) {
		t.Fatalf("allow stale query = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPNoCacheLoadGraphHonorsManifestVersionFloor(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	manifestKey := "test/tenants/tenant-a/manifest.parquet"
	staleManifest, staleMeta, err := objects.GetWithMeta(ctx, manifestKey)
	if err != nil {
		t.Fatalf("read stale manifest: %v", err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}

	readerObjects := &staleManifestSecondReadStore{
		ObjectStore: objects,
		key:         manifestKey,
		stale:       staleManifest,
		staleMeta:   staleMeta,
	}
	reader := storage.NewTenantStore(readerObjects, "test")
	handler := (&Server{Store: reader, Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-a", nil)
	if rr.Code == http.StatusOK && strings.Contains(rr.Body.String(), `"version":1`) {
		t.Fatalf("served stale version below floor: %s", rr.Body.String())
	}
	if rr.Code != http.StatusServiceUnavailable ||
		!strings.Contains(rr.Body.String(), `"code":"reader_not_fresh"`) ||
		!strings.Contains(rr.Body.String(), `"visible_version":1`) ||
		!strings.Contains(rr.Body.String(), `"required_version":2`) {
		t.Fatalf("status=%d body=%s, want reader_not_fresh version floor error", rr.Code, rr.Body.String())
	}
}

type staleManifestSecondReadStore struct {
	storage.ObjectStore
	mu        sync.Mutex
	key       string
	stale     []byte
	staleMeta storage.ObjectMeta
	reads     int
}

func (s *staleManifestSecondReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, storage.ObjectMeta, error) {
	if key != s.key {
		return s.ObjectStore.GetWithMeta(ctx, key)
	}
	s.mu.Lock()
	s.reads++
	reads := s.reads
	s.mu.Unlock()
	if reads == 2 {
		return append([]byte(nil), s.stale...), s.staleMeta, nil
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func TestHTTPGetEntityFallsBackWhenPersistedEntityPageHashMismatch(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	pageKey := catalogEntityPageObjectKey(t, catalog, "host:app-01")
	recordKey := singleObjectKey(t, ctx, objects, "test/tenants/tenant-a/indexes/entities/by-id/")
	stalePage, err := objects.Get(ctx, pageKey)
	if err != nil {
		t.Fatalf("read stale page: %v", err)
	}
	staleRecord, err := objects.Get(ctx, recordKey)
	if err != nil {
		t.Fatalf("read stale record: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	currentCatalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current catalog: %v", err)
	}
	currentPageKey := catalogEntityPageObjectKey(t, currentCatalog, "host:app-01")
	if err := objects.Put(ctx, currentPageKey, stalePage); err != nil {
		t.Fatalf("restore stale page: %v", err)
	}
	if err := objects.Put(ctx, recordKey, staleRecord); err != nil {
		t.Fatalf("restore stale record: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/entities/host:app-01", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"hostname":"app-02"`) || strings.Contains(rr.Body.String(), `"hostname":"app-01"`) {
		t.Fatalf("get = %d body=%s", rr.Code, rr.Body.String())
	}
}

func singleObjectKey(t *testing.T, ctx context.Context, objects *storage.MemoryStore, prefix string) string {
	t.Helper()
	items, err := objects.List(ctx, prefix)
	if err != nil {
		t.Fatalf("list %s: %v", prefix, err)
	}
	if len(items) != 1 {
		t.Fatalf("list %s got %d objects, want 1: %#v", prefix, len(items), items)
	}
	return items[0].Key
}

func catalogFieldObjectKey(t *testing.T, catalog storage.IndexCatalog, kind string, field string) string {
	t.Helper()
	for _, index := range catalog.Indexes {
		if index.Kind == kind && index.Field == field {
			return firstCatalogObjectKey(t, index.Objects)
		}
	}
	t.Fatalf("catalog missing field index %s.%s: %#v", kind, field, catalog.Indexes)
	return ""
}

func catalogEdgeShardObjectKey(t *testing.T, catalog storage.IndexCatalog, relationType string, shard string) string {
	t.Helper()
	for _, edgeShard := range catalog.EdgeShards {
		if edgeShard.RelationType == relationType && edgeShard.Shard == shard {
			return firstCatalogObjectKey(t, edgeShard.Objects)
		}
	}
	t.Fatalf("catalog missing edge shard %s/%s: %#v", relationType, shard, catalog.EdgeShards)
	return ""
}

func catalogEntityPageObjectKey(t *testing.T, catalog storage.IndexCatalog, entityID string) string {
	t.Helper()
	shard := testShardID(entityID)
	for _, page := range catalog.EntityPages {
		if page.Shard == shard {
			return firstCatalogObjectKey(t, page.Objects)
		}
	}
	t.Fatalf("catalog missing entity page %s: %#v", shard, catalog.EntityPages)
	return ""
}

func firstCatalogObjectKey(t *testing.T, objects []storage.IndexObject) string {
	t.Helper()
	if len(objects) == 0 || objects[0].Key == "" {
		t.Fatalf("catalog object missing key: %#v", objects)
	}
	return objects[0].Key
}

func testShardID(id string) string {
	if id == "" {
		return "default"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(id)))
	return fmt.Sprintf("%02x", int(sum[0])%64)
}

func TestHTTPQueryStreamSavedQueryAndIndexCatalog(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	commitBody := CommitRequest{Mutations: graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", commitBody); rr.Code != http.StatusOK {
		t.Fatalf("commit status = %d body=%s", rr.Code, rr.Body.String())
	}

	streamBody := query.Request{Op: "profile", TargetOp: "match", Kind: "host", Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}}}
	rr := serveJSON(handler, http.MethodPost, "/v1/query/stream", "tenant-a", streamBody)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"strategy":"field-index"`) {
		t.Fatalf("query stream = %d body=%s", rr.Code, rr.Body.String())
	}

	saved := storage.SavedQuery{Name: "host-by-name", Request: streamBody}
	if rr := serveJSON(handler, http.MethodPost, "/v1/query/templates", "tenant-a", saved); rr.Code != http.StatusOK {
		t.Fatalf("save query = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/query/templates/host-by-name/run", "tenant-a", map[string]any{})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host:app-01"`) {
		t.Fatalf("run saved query = %d body=%s", rr.Code, rr.Body.String())
	}

	slashSaved := storage.SavedQuery{Name: "team/hosts", Request: streamBody}
	if rr := serveJSON(handler, http.MethodPost, "/v1/query/templates", "tenant-a", slashSaved); rr.Code != http.StatusOK {
		t.Fatalf("save slash query = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/query/templates/"+url.PathEscape(slashSaved.Name)+"/run", "tenant-a", map[string]any{})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host:app-01"`) {
		t.Fatalf("run slash saved query = %d body=%s", rr.Code, rr.Body.String())
	}

	rr = serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild", "tenant-a", map[string]any{})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host.hostname"`) {
		t.Fatalf("rebuild indexes = %d body=%s", rr.Code, rr.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/indexes", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host.hostname"`) {
		t.Fatalf("index catalog = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPQueryStreamStopsOnEncoderError(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host"},
			{ID: "host:app-02", Kind: "host"},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body, err := json.Marshal(query.Request{Op: "match", Kind: "host", Limit: 10})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/query/stream", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")
	rr := &failingResponseWriter{header: http.Header{}, failOnWrite: 2}
	handler.ServeHTTP(rr, req)
	if rr.writes != 2 {
		t.Fatalf("writes = %d, want stop after first encoder error", rr.writes)
	}
}

func TestHTTPQueryStreamRejectsInvalidFilterOp(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query/stream", "tenant-a", query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "regex", Value: "app-.*"}},
	})
	if rr.Code != http.StatusUnprocessableEntity || !strings.Contains(rr.Body.String(), "unsupported filter op") {
		t.Fatalf("stream invalid filter = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPQueryRejectsInvalidControlParameter(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
		Op:        "match",
		Kind:      "host",
		TimeoutMS: -1,
	})
	var body ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid control response json: %v body=%s", err, rr.Body.String())
	}
	if rr.Code != http.StatusUnprocessableEntity || !strings.Contains(body.Error, "timeout_ms must be >= 0") {
		t.Fatalf("invalid control query = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPQueryRejectsScalarInFilter(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "region", Op: "in", Value: "us-east-1"}},
	})
	if rr.Code != http.StatusUnprocessableEntity || !strings.Contains(rr.Body.String(), "in filter value must be an array") {
		t.Fatalf("scalar in filter = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPLazyMatchProjectionDoesNotChangeSortOrAggregateInputs(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	commitBody := CommitRequest{Mutations: graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string"},
				"region":   {Type: "string", Indexed: true},
				"cpu":      {Type: "number"},
			},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "r1", "cpu": 8}},
			{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "region": "r1", "cpu": 16}},
		},
	}}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", commitBody); rr.Code != http.StatusOK {
		t.Fatalf("commit status = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild", "tenant-a", map[string]any{}); rr.Code != http.StatusOK {
		t.Fatalf("rebuild status = %d body=%s", rr.Code, rr.Body.String())
	}

	request := query.Request{
		Op:        "match",
		Kind:      "host",
		Where:     []query.Filter{{Field: "region", Op: "eq", Value: "r1"}},
		Sort:      []query.SortSpec{{Field: "cpu", Desc: true}},
		Project:   []string{"hostname"},
		Aggregate: []query.Aggregation{{Op: "avg", Field: "cpu", Name: "avg_cpu"}},
		Limit:     1,
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", request)
	if rr.Code != http.StatusOK {
		t.Fatalf("query status = %d body=%s", rr.Code, rr.Body.String())
	}
	var response query.Response
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("results = %#v, want highest cpu first", response.Results)
	}
	if _, ok := response.Results[0].Fields["cpu"]; ok {
		t.Fatalf("projection leaked cpu: %#v", response.Results[0].Fields)
	}
	if response.Aggregates["avg_cpu"] != float64(12) {
		t.Fatalf("avg_cpu = %#v, want 12", response.Aggregates["avg_cpu"])
	}
}

func TestHTTPQueryFallsBackWhenCatalogCannotMaterializeEntities(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := objects.Delete(ctx, "test/tenants/tenant-a/indexes/catalog.parquet"); err != nil {
		t.Fatalf("delete parquet catalog: %v", err)
	}

	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
	})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host:app-01"`) {
		t.Fatalf("query status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPQueryFallsBackWhenPersistedFieldIndexObjectIsUnavailable(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := objects.Put(ctx, catalogFieldObjectKey(t, catalog, "host", "hostname"), []byte("corrupt index object")); err != nil {
		t.Fatalf("put corrupt field index: %v", err)
	}

	handler := (&Server{Store: store, Mode: "all"}).Handler()
	request := query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit: 1,
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", request)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host:app-01"`) {
		t.Fatalf("query status = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/query/stream", "tenant-a", request)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host:app-01"`) || strings.Contains(rr.Body.String(), "persisted index unavailable") {
		t.Fatalf("stream status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPQueryStreamFallsBackWhenPersistedEntityRecordIsUnavailable(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{ID: "host-app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := objects.Delete(ctx, "test/tenants/tenant-a/indexes/entities/by-id/host-app-01.parquet"); err != nil {
		t.Fatalf("delete entity record: %v", err)
	}

	handler := (&Server{Store: store, Mode: "all"}).Handler()
	request := query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit: 1,
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/query/stream", "tenant-a", request)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host-app-01"`) || strings.Contains(rr.Body.String(), "persisted index unavailable") {
		t.Fatalf("stream status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPQueryStreamFallbackIncludesNextCursor(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}},
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
			{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02"}},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	request := query.Request{
		Op:    "match",
		Kind:  "host",
		Sort:  []query.SortSpec{{Field: "hostname"}},
		Limit: 1,
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/query/stream", "tenant-a", request)
	if rr.Code != http.StatusOK {
		t.Fatalf("stream status = %d body=%s", rr.Code, rr.Body.String())
	}
	firstLine := strings.Split(strings.TrimSpace(rr.Body.String()), "\n")[0]
	if !strings.Contains(firstLine, `"next_cursor"`) {
		t.Fatalf("stream fallback meta missing next_cursor: %s", rr.Body.String())
	}
}

func TestHTTPTraversalFallsBackWhenPersistedEdgeShardIsUnavailable(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := objects.Delete(ctx, catalogEdgeShardObjectKey(t, catalog, "runs_on", testShardID("service:api"))); err != nil {
		t.Fatalf("delete edge shard: %v", err)
	}

	handler := (&Server{Store: store, Mode: "all"}).Handler()
	for _, request := range []query.Request{
		{Op: "neighbors", ID: "service:api", Direction: "out", RelationType: "runs_on", Limit: 10},
		{Op: "traverse", ID: "service:api", Direction: "out", RelationType: "runs_on", Depth: 1, Limit: 10},
	} {
		rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", request)
		if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"host:app-01"`) {
			t.Fatalf("%s status = %d body=%s", request.Op, rr.Code, rr.Body.String())
		}
	}
}

func TestHTTPDeadLettersReplayAndWriterLease(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	bad := storage.IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "bad-edge",
		Items: []storage.IngestItem{{
			ExternalID: "edge",
			Edge:       &graph.Edge{ID: "edge:a-b", Type: "connects_to", From: "host:a", To: "host:b"},
		}},
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", bad); rr.Code != http.StatusMultiStatus {
		t.Fatalf("bad ingest = %d body=%s", rr.Code, rr.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/ingest/deadletters/agent", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"pending"`) {
		t.Fatalf("deadletters = %d body=%s", rr.Code, rr.Body.String())
	}
	commitBody := CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "host:a", Kind: "host"},
		{ID: "host:b", Kind: "host"},
	}}}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", commitBody); rr.Code != http.StatusOK {
		t.Fatalf("commit endpoints = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/ingest/deadletters/agent/replay", "tenant-a", map[string]any{})
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"resolved":1`) {
		t.Fatalf("replay = %d body=%s", rr.Code, rr.Body.String())
	}
	leaseReq := httptest.NewRequest(http.MethodGet, "/v1/control/writer-lease", nil)
	leaseReq.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, leaseReq)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"owner_id"`) {
		t.Fatalf("writer lease = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPDeadLetterReplayErrorInvalidatesCacheAfterCommit(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	store := storage.NewTenantStore(objects, "test")
	result, err := store.Ingest(ctx, "tenant-a", storage.IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "edge-before-node",
		Items: []storage.IngestItem{{
			ExternalID: "calls_api_edge",
			Edge:       &graph.Edge{ID: "edge:api-backend", Type: "calls_api", From: "service:api", To: "api:backend"},
		}},
	})
	if err != nil || result.Failed != 1 {
		t.Fatalf("seed deadletter result=%#v err=%v", result, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "calls_api", Directed: true, AllowCrossKind: true}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "api:backend", Kind: "api"},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit endpoints: %v", err)
	}
	store.Objects = &httpFailPutStore{ObjectStore: objects, contains: "/ingest/agent/collectors/collector-a.parquet"}
	cache := storage.NewReaderCache(store, time.Hour)
	handler := (&Server{Store: store, Cache: cache, Mode: "all"}).Handler()

	neighbors := query.Request{Op: "neighbors", ID: "service:api", Direction: "out", RelationType: "calls_api", Limit: 10}
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", neighbors)
	if rr.Code != http.StatusOK || strings.Contains(rr.Body.String(), `"api:backend"`) {
		t.Fatalf("initial neighbors = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/ingest/deadletters/agent/replay", "tenant-a", nil)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "save collector status") {
		t.Fatalf("replay = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", neighbors)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"api:backend"`) {
		t.Fatalf("neighbors after replay error = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPDeadLettersAllowEscapedSlashSource(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	source := "bad/src"
	rr := serveJSON(handler, http.MethodPost, "/v1/ingest/batches", "tenant-a", storage.IngestRequest{
		Source:      source,
		CollectorID: "collector-a",
		BatchID:     "bad-1",
		Items:       []storage.IngestItem{{ExternalID: "bad"}},
	})
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("ingest status = %d body=%s", rr.Code, rr.Body.String())
	}
	path := "/v1/ingest/deadletters/" + url.PathEscape(source)
	rr = serveJSON(handler, http.MethodGet, path, "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"source":"`+source+`"`) {
		t.Fatalf("deadletters = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, path+"/replay", "tenant-a", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"replayed":1`) {
		t.Fatalf("replay = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPDeadLetterReplayRejectsInvalidLimit(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	for _, rawLimit := range []string{"abc", "-1"} {
		path := "/v1/ingest/deadletters/agent/replay?limit=" + url.QueryEscape(rawLimit)
		rr := serveJSON(handler, http.MethodPost, path, "tenant-a", nil)
		if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "limit must be a non-negative integer") {
			t.Fatalf("limit %q replay = %d body=%s", rawLimit, rr.Code, rr.Body.String())
		}
	}
}

func TestHTTPQueryAdmissionRejectsWhenTenantQueueIsFull(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	admission := NewQueryAdmission(1, 1, time.Millisecond)
	release, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("acquire admission: %v", err)
	}
	defer release()
	handler := (&Server{Store: store, Mode: "all", Admission: admission}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host"})
	if rr.Code != http.StatusTooManyRequests || !strings.Contains(rr.Body.String(), `"code":"query_limit_exceeded"`) || !strings.Contains(rr.Body.String(), `"retryable":true`) {
		t.Fatalf("query status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPRunningQueryListAndKill(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	admission := NewQueryAdmission(0, 1, 5*time.Second)
	release, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("hold admission: %v", err)
	}
	defer release()
	handler := (&Server{Store: store, Mode: "all", Admission: admission}).Handler()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host"})
	}()
	var queryID string
	for i := 0; i < 100; i++ {
		list := serveJSON(handler, http.MethodGet, "/v1/queries/running", "tenant-a", nil)
		if list.Code != http.StatusOK {
			t.Fatalf("list running = %d body=%s", list.Code, list.Body.String())
		}
		var body struct {
			Queries []RunningQueryInfo `json:"queries"`
		}
		if err := json.Unmarshal(list.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode running queries: %v", err)
		}
		if len(body.Queries) == 1 {
			queryID = body.Queries[0].ID
			break
		}
		time.Sleep(time.Millisecond)
	}
	if queryID == "" {
		t.Fatal("running query was not registered")
	}
	kill := serveJSON(handler, http.MethodDelete, "/v1/queries/running/"+queryID, "tenant-a", nil)
	if kill.Code != http.StatusOK || !strings.Contains(kill.Body.String(), `"killed":true`) {
		t.Fatalf("kill running query = %d body=%s", kill.Code, kill.Body.String())
	}
	select {
	case rr := <-done:
		if rr.Code != http.StatusTooManyRequests && rr.Code != http.StatusBadRequest {
			t.Fatalf("query after kill = %d body=%s", rr.Code, rr.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("killed query did not return")
	}
}

func TestQueryAdmissionTenantWaiterDoesNotStarveOtherTenant(t *testing.T) {
	admission := NewQueryAdmission(2, 1, 100*time.Millisecond)
	releaseA, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	waiterDone := make(chan error, 1)
	go func() {
		release, err := admission.Acquire(context.Background(), "tenant-a")
		if err == nil {
			release()
		}
		waiterDone <- err
	}()
	waitAdmissionTenantRefs(t, admission, "tenant-a", 2)

	releaseB, err := admission.Acquire(context.Background(), "tenant-b")
	if err != nil {
		t.Fatalf("tenant-b acquire was starved by tenant-a waiter: %v", err)
	}
	releaseB()
	releaseA()

	select {
	case err := <-waiterDone:
		if err != nil {
			t.Fatalf("tenant-a waiter acquire: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tenant-a waiter did not finish after release")
	}
}

func TestQueryAdmissionDropsIdleTenantSlots(t *testing.T) {
	admission := NewQueryAdmission(0, 1, time.Millisecond)
	release, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	if got := admissionTenantCount(admission); got != 0 {
		t.Fatalf("tenant slots after release = %d, want 0", got)
	}
}

func TestQueryAdmissionDropsTimedOutTenantWaiter(t *testing.T) {
	admission := NewQueryAdmission(0, 1, time.Millisecond)
	release, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := admission.Acquire(context.Background(), "tenant-a"); !errors.Is(err, query.ErrLimitExceeded) {
		t.Fatalf("second acquire err = %v, want ErrLimitExceeded", err)
	}
	if got := admissionTenantCount(admission); got != 1 {
		t.Fatalf("tenant slots while first held = %d, want 1", got)
	}
	release()
	if got := admissionTenantCount(admission); got != 0 {
		t.Fatalf("tenant slots after release = %d, want 0", got)
	}
}

func admissionTenantCount(admission *QueryAdmission) int {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return len(admission.tenants)
}

func waitAdmissionTenantRefs(t *testing.T, admission *QueryAdmission, tenantID string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		admission.mu.Lock()
		got := 0
		if tenant := admission.tenants[tenantID]; tenant != nil {
			got = tenant.refs
		}
		admission.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("tenant %q refs did not reach %d", tenantID, want)
}

func serveJSON(handler http.Handler, method string, path string, tenantID string, body any) *httptest.ResponseRecorder {
	requestJSON, _ := json.Marshal(body)
	req := httptest.NewRequest(method, path, bytes.NewReader(requestJSON))
	req.Header.Set("X-Tenant-ID", tenantID)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

type failingResponseWriter struct {
	header      http.Header
	status      int
	writes      int
	failOnWrite int
}

func (w *failingResponseWriter) Header() http.Header {
	return w.header
}

func (w *failingResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *failingResponseWriter) Write(_ []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.writes++
	if w.failOnWrite > 0 && w.writes >= w.failOnWrite {
		return 0, errors.New("injected stream write failure")
	}
	return 1, nil
}

type httpFailPutStore struct {
	storage.ObjectStore
	contains string
}

func (s *httpFailPutStore) Put(ctx context.Context, key string, data []byte) error {
	if strings.Contains(key, s.contains) {
		return errors.New("injected put failure")
	}
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *httpFailPutStore) PutConditional(ctx context.Context, key string, data []byte, condition storage.PutCondition) (storage.ObjectMeta, error) {
	if strings.Contains(key, s.contains) {
		return storage.ObjectMeta{}, errors.New("injected put failure")
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

type switchListStore struct {
	storage.ObjectStore
	mu   sync.Mutex
	fail bool
}

func (s *switchListStore) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func (s *switchListStore) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	s.mu.Lock()
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return nil, errors.New("list failed")
	}
	return s.ObjectStore.List(ctx, prefix)
}
