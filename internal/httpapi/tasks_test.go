package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/storage"
)

func TestHTTPTaskStartGetAndList(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CommitWithReport(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	start := serveJSON(handler, http.MethodPost, "/v1/tasks", "tenant-a", TaskStartRequest{Type: storage.TaskTypeExportSnapshot})
	if start.Code != http.StatusAccepted {
		t.Fatalf("start status = %d body=%s", start.Code, start.Body.String())
	}
	var task storage.Task
	if err := json.Unmarshal(start.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	task = waitForHTTPTask(t, store, "tenant-a", task.ID)
	get := serveJSON(handler, http.MethodGet, "/v1/tasks/"+task.ID, "tenant-a", nil)
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"status":"succeeded"`) {
		t.Fatalf("get status = %d body=%s", get.Code, get.Body.String())
	}
	list := serveJSON(handler, http.MethodGet, "/v1/tasks?type=export_snapshot&status=succeeded", "tenant-a", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), task.ID) {
		t.Fatalf("list status = %d body=%s", list.Code, list.Body.String())
	}
}

func TestHTTPTaskStartRejectedInReaderMode(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/tasks", "tenant-a", TaskStartRequest{Type: storage.TaskTypeCompact})
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("start status = %d body=%s", rr.Code, rr.Body.String())
	}
	list := serveJSON(handler, http.MethodGet, "/v1/tasks", "tenant-a", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", list.Code, list.Body.String())
	}
	cancel := serveJSON(handler, http.MethodPost, "/v1/tasks/task-a/cancel", "tenant-a", nil)
	if cancel.Code != http.StatusMethodNotAllowed {
		t.Fatalf("cancel status = %d body=%s", cancel.Code, cancel.Body.String())
	}
}

func TestHTTPTenantRestoreDrillStartsTask(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	start := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/restore-drill", "tenant-a", tenantRestoreDrillRequest{DryRun: true})
	if start.Code != http.StatusAccepted {
		t.Fatalf("start status = %d body=%s", start.Code, start.Body.String())
	}
	var task storage.Task
	if err := json.Unmarshal(start.Body.Bytes(), &task); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	if task.Type != storage.TaskTypeTenantRestoreDrill {
		t.Fatalf("task type = %q", task.Type)
	}
	task = waitForHTTPTask(t, store, "tenant-a", task.ID)
	if task.Result["status"] != "dry_run" {
		t.Fatalf("restore drill result = %#v", task.Result)
	}
}

func waitForHTTPTask(t *testing.T, store *storage.TenantStore, tenantID string, taskID string) storage.Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(context.Background(), tenantID, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if task.Status != "queued" && task.Status != "running" {
			if task.Status == "failed" {
				t.Fatalf("task failed: %#v", task)
			}
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish", taskID)
	return storage.Task{}
}
