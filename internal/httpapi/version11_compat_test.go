package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPGenericEntityTypeAndLabelsCompatibility(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := []byte(`{
		"mutations": {
			"upsert_entity_types": [{"name":"document","display_name":"Document"}],
			"upsert_entities": [{"id":"document:1","kind":"document","labels":["article","knowledge"],"fields":{"title":"GGraphDB 1.1"}}]
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("commit status=%d body=%s", rr.Code, rr.Body.String())
	}

	assertTypeList := func(path, field string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Tenant-ID", "tenant-a")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
		var response map[string]json.RawMessage
		if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		var types []graph.EntityType
		if err := json.Unmarshal(response[field], &types); err != nil || len(types) != 1 || types[0].Name != "document" {
			t.Fatalf("%s %s=%s err=%v", path, field, response[field], err)
		}
	}
	assertTypeList("/v1/entity-types", "entity_types")
	assertTypeList("/v1/ci-types", "ci_types")

	req = httptest.NewRequest(http.MethodGet, "/v1/entities/document:1", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("get entity status=%d body=%s", rr.Code, rr.Body.String())
	}
	var response struct {
		Entity graph.Entity `json:"entity"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	labels := graph.EntityLabels(response.Entity)
	if len(labels) != 2 || labels[0] != "article" || labels[1] != "knowledge" {
		t.Fatalf("labels=%#v entity=%#v", labels, response.Entity)
	}
	if _, ok := response.Entity.Fields[graph.ReservedLabelsField]; !ok {
		t.Fatalf("1.0-compatible reserved field missing: %#v", response.Entity.Fields)
	}
}

func TestHTTPGenericAndLegacyTypeMutationNamesCannotConflict(t *testing.T) {
	handler := (&Server{Store: storage.NewTenantStore(storage.NewMemoryStore(), "test"), Mode: "all"}).Handler()
	body := []byte(`{"mutations":{"upsert_ci_types":[{"name":"legacy"}],"upsert_entity_types":[{"name":"generic"}]}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHTTPIngestAcceptsEntityTypeAlias(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := []byte(`{
		"source":"manual","collector_id":"generic-import","items":[
			{"external_id":"type-document","entity_type":{"name":"document"}},
			{"external_id":"document-1","entity":{"id":"document:1","kind":"document","labels":["article"]}}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest status=%d body=%s", rr.Code, rr.Body.String())
	}
	loaded, _, err := store.Load(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded.CITypes["document"]; !ok {
		t.Fatalf("entity type alias was not persisted: %#v", loaded.CITypes)
	}
}

func TestHTTPRelationSchemaSidecar(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(t.Context(), "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "references", FromKind: "document", ToKind: "document", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := []byte(`{"strict":true,"fields":{"page":{"type":"number","required":true}}}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/relation-schemas/references", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("put schema status=%d body=%s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/relation-schemas", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"relation_type":"references"`)) {
		t.Fatalf("get schemas status=%d body=%s", rr.Code, rr.Body.String())
	}

	commit := []byte(`{"mutations":{"upsert_edges":[{"type":"references","from":"document:a","to":"document:b","fields":{"page":"one"}}]}}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/commits", bytes.NewReader(commit))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid relation property status=%d body=%s", rr.Code, rr.Body.String())
	}
}
