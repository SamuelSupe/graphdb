package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/query"
	"graphdb/internal/storage"
)

func TestHTTPIndexHealthAndAsyncRebuildTask(t *testing.T) {
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
		t.Fatalf("commit = %d body=%s", rr.Code, rr.Body.String())
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/indexes/health", nil)
	get.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, get)
	if rr.Code != http.StatusOK {
		t.Fatalf("health before rebuild = %d body=%s", rr.Code, rr.Body.String())
	}
	var health storage.IndexHealth
	if err := json.Unmarshal(rr.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health.Status != "missing" {
		t.Fatalf("health = %#v", health)
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild?async=true", "tenant-a", map[string]any{})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("async rebuild = %d body=%s", rr.Code, rr.Body.String())
	}
	var task storage.IndexTask
	if err := json.Unmarshal(rr.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	task = awaitIndexTask(t, handler, task.ID)
	if task.Status != "succeeded" || task.CatalogVersion == 0 || task.ProgressCompleted != task.ProgressTotal {
		t.Fatalf("task = %#v", task)
	}
}

func awaitIndexTask(t *testing.T, handler http.Handler, taskID string) storage.IndexTask {
	t.Helper()
	var task storage.IndexTask
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/indexes/tasks/"+taskID, nil)
		req.Header.Set("X-Tenant-ID", "tenant-a")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("task get = %d body=%s", rr.Code, rr.Body.String())
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &task); err != nil {
			t.Fatalf("decode task get: %v", err)
		}
		if task.Status != "running" {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	return task
}

func TestHTTPCreateAndDropIndexDefinitions(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	commitBody := CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"owner": "platform"}}},
	}}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", commitBody); rr.Code != http.StatusOK {
		t.Fatalf("commit = %d body=%s", rr.Code, rr.Body.String())
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/indexes", "tenant-a", storage.IndexDefinition{Kind: "host", Field: "owner"})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("create index = %d body=%s", rr.Code, rr.Body.String())
	}
	var created storage.IndexDefinitionResult
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	task := awaitIndexTask(t, handler, created.Task.ID)
	if task.Status != "succeeded" {
		t.Fatalf("create task = %#v", task)
	}
	defs := serveJSON(handler, http.MethodGet, "/v1/indexes/definitions", "tenant-a", nil)
	if defs.Code != http.StatusOK || !strings.Contains(defs.Body.String(), `"host.owner"`) {
		t.Fatalf("definitions = %d body=%s", defs.Code, defs.Body.String())
	}
	catalog := serveJSON(handler, http.MethodGet, "/v1/indexes", "tenant-a", nil)
	if catalog.Code != http.StatusOK || !strings.Contains(catalog.Body.String(), `"host.owner"`) {
		t.Fatalf("catalog = %d body=%s", catalog.Code, catalog.Body.String())
	}
	rr = serveJSON(handler, http.MethodDelete, "/v1/indexes/definitions/host.owner", "tenant-a", nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("drop index = %d body=%s", rr.Code, rr.Body.String())
	}
	var dropped storage.IndexDefinitionResult
	if err := json.Unmarshal(rr.Body.Bytes(), &dropped); err != nil {
		t.Fatalf("decode drop: %v", err)
	}
	task = awaitIndexTask(t, handler, dropped.Task.ID)
	if task.Status != "succeeded" {
		t.Fatalf("drop task = %#v", task)
	}
	catalog = serveJSON(handler, http.MethodGet, "/v1/indexes", "tenant-a", nil)
	if catalog.Code != http.StatusOK || strings.Contains(catalog.Body.String(), `"host.owner"`) {
		t.Fatalf("catalog after drop = %d body=%s", catalog.Code, catalog.Body.String())
	}
}

func TestHTTPRebuildIndexesWritesParquet(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	commitBody := CommitRequest{Mutations: graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}, "region": {Type: "string"}},
		}},
		UpsertRelationTypes: []graph.RelationType{{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "api"}},
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "r1"}},
		},
		UpsertEdges: []graph.Edge{{ID: "collector-edge-1", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", commitBody); rr.Code != http.StatusOK {
		t.Fatalf("commit = %d body=%s", rr.Code, rr.Body.String())
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild", "tenant-a", map[string]any{})
	if rr.Code != http.StatusOK {
		t.Fatalf("parquet rebuild = %d body=%s", rr.Code, rr.Body.String())
	}
	var catalog storage.IndexCatalog
	if err := json.Unmarshal(rr.Body.Bytes(), &catalog); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	if len(catalog.Indexes) == 0 || catalog.Indexes[0].Format != storage.IndexFormatParquet || len(catalog.Indexes[0].Objects) == 0 {
		t.Fatalf("field index catalog = %#v", catalog.Indexes)
	}
	if len(catalog.EdgeShards) == 0 || catalog.EdgeShards[0].Format != storage.IndexFormatParquet || len(catalog.EdgeShards[0].Objects) == 0 {
		t.Fatalf("edge shard catalog = %#v", catalog.EdgeShards)
	}
	if len(catalog.EntityPages) == 0 || catalog.EntityPages[0].Format != storage.IndexFormatParquet || len(catalog.EntityPages[0].Objects) == 0 {
		t.Fatalf("entity page catalog = %#v", catalog.EntityPages)
	}
	match := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
		Op:      "match",
		Kind:    "host",
		Where:   []query.Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Project: []string{"hostname"},
		Limit:   10,
	})
	if match.Code != http.StatusOK || !strings.Contains(match.Body.String(), `"hostname":"app-01"`) {
		t.Fatalf("parquet match = %d body=%s", match.Code, match.Body.String())
	}
	neighbors := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
		Op: "neighbors", ID: "service:api", Direction: "out", RelationType: "runs_on", Limit: 10,
	})
	if neighbors.Code != http.StatusOK || !strings.Contains(neighbors.Body.String(), `"host:app-01"`) {
		t.Fatalf("parquet neighbors = %d body=%s", neighbors.Code, neighbors.Body.String())
	}
	entities := serveJSON(handler, http.MethodGet, "/v1/entities?kind=host&limit=10", "tenant-a", nil)
	if entities.Code != http.StatusOK {
		t.Fatalf("entity scan = %d body=%s", entities.Code, entities.Body.String())
	}
	var entityPage storage.EntityScanResult
	if err := json.Unmarshal(entities.Body.Bytes(), &entityPage); err != nil {
		t.Fatalf("decode entity page: %v", err)
	}
	if !entityPage.IndexedRead || len(entityPage.Entities) != 1 || entityPage.Entities[0].ID != "host:app-01" {
		t.Fatalf("entity page = %#v", entityPage)
	}
}

func TestHTTPSyncRebuildSkipsEntityRecordCleanup(t *testing.T) {
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
		t.Fatalf("commit = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild", "tenant-a", map[string]any{}); rr.Code != http.StatusOK {
		t.Fatalf("first rebuild = %d body=%s", rr.Code, rr.Body.String())
	}
	if count := countEntityRecordObjects(t, store, "tenant-a"); count == 0 {
		t.Fatal("first rebuild did not create entity records")
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild", "tenant-a", map[string]any{}); rr.Code != http.StatusOK {
		t.Fatalf("second rebuild = %d body=%s", rr.Code, rr.Body.String())
	}
	if count := countEntityRecordObjects(t, store, "tenant-a"); count == 0 {
		t.Fatal("sync rebuild cleanup deleted entity records")
	}
}

func countEntityRecordObjects(t *testing.T, store *storage.TenantStore, tenantID string) int {
	t.Helper()
	objects, err := store.Objects.List(t.Context(), "test/tenants/"+tenantID+"/indexes/entities/by-id/")
	if err != nil {
		t.Fatalf("list entity records: %v", err)
	}
	return len(objects)
}

func TestHTTPRebuildIndexesRejectsFormatOverride(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/indexes/rebuild?format=json", "tenant-a", map[string]any{})
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "fixed to parquet") {
		t.Fatalf("format override = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPReaderModeRejectsIndexDDL(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	create := serveJSON(handler, http.MethodPost, "/v1/indexes", "tenant-a", storage.IndexDefinition{Kind: "host", Field: "owner"})
	if create.Code != http.StatusMethodNotAllowed {
		t.Fatalf("create status = %d body=%s", create.Code, create.Body.String())
	}
	drop := serveJSON(handler, http.MethodDelete, "/v1/indexes/definitions/host.owner", "tenant-a", nil)
	if drop.Code != http.StatusMethodNotAllowed {
		t.Fatalf("drop status = %d body=%s", drop.Code, drop.Body.String())
	}
}
