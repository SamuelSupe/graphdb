package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPStartsJSONLImportTask(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	body := strings.Join([]string{
		`{"entity_type":{"name":"concept"}}`,
		`{"entity":{"id":"concept:graph","kind":"concept","labels":["knowledge"],"fields":{"name":"Graph"}}}`,
	}, "\n")
	req := httptest.NewRequest(http.MethodPost, "/v1/imports?batch_size=1", strings.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/x-ndjson")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var task storage.Task
	if err := json.Unmarshal(rr.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	if rr.Header().Get("Location") != "/v1/tasks/"+task.ID || task.Type != storage.TaskTypeBulkImport {
		t.Fatalf("task = %#v location=%q", task, rr.Header().Get("Location"))
	}
	task = waitForHTTPTask(t, store, "tenant-a", task.ID)
	if task.Status != storage.TaskStatusSucceeded || task.Result["applied"] != float64(2) {
		t.Fatalf("task = %#v", task)
	}
	g, _, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := g.Entities["concept:graph"]; !ok {
		t.Fatal("imported entity not found")
	}
}

func TestHTTPImportRejectsUnknownFormat(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/imports", strings.NewReader("data"))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/octet-stream")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}
