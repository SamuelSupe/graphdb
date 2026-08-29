package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/config"
	"gitlab.jiagouyun.com/guance/graphdb/internal/httpapi"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestNewHTTPServerSetsProductionTimeouts(t *testing.T) {
	cfg := config.Config{Addr: "127.0.0.1:0", Mode: "all"}
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	server := newHTTPServer(cfg, &httpapi.Server{Store: store, Mode: cfg.Mode})

	if server.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want 5s", server.ReadHeaderTimeout)
	}
	if server.ReadTimeout != time.Minute {
		t.Fatalf("ReadTimeout = %s, want 1m", server.ReadTimeout)
	}
	if server.WriteTimeout != 10*time.Minute {
		t.Fatalf("WriteTimeout = %s, want 10m", server.WriteTimeout)
	}
	if server.IdleTimeout != 2*time.Minute {
		t.Fatalf("IdleTimeout = %s, want 2m", server.IdleTimeout)
	}
	if server.Handler == nil {
		t.Fatal("Handler is nil")
	}
}

func TestRunHTTPServerStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server := &http.Server{Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	if err := runHTTPServer(ctx, server, time.Second); err != nil {
		t.Fatalf("runHTTPServer err = %v, want nil graceful shutdown", err)
	}
}

func TestNewSeparateHTTPServersUseExpectedHandlers(t *testing.T) {
	cfg := config.Config{
		Addr:         "127.0.0.1:0",
		AdminAddr:    "127.0.0.1:0",
		PprofEnabled: true,
		Mode:         "all",
	}
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	api := &httpapi.Server{Store: store, Mode: cfg.Mode}

	data := newDataHTTPServer(cfg, api)
	metrics := httptestResponse(data.Handler, http.MethodGet, "/metrics")
	if metrics.Code != http.StatusNotFound {
		t.Fatalf("data metrics status=%d, want 404", metrics.Code)
	}

	admin := newAdminHTTPServer(cfg, api)
	pprof := httptestResponse(admin.Handler, http.MethodGet, "/debug/pprof/")
	if pprof.Code != http.StatusOK {
		t.Fatalf("admin pprof status=%d, want 200", pprof.Code)
	}
}

func TestStartAndWaitTaskReturnsTerminalState(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	task, err := startAndWaitTask(
		context.Background(),
		store,
		"tenant-a",
		storage.TaskTypeExportSnapshot,
		nil,
	)
	if err != nil {
		t.Fatalf("start and wait task: %v", err)
	}
	if task.Status != storage.TaskStatusSucceeded || task.ResultKey == "" {
		t.Fatalf("terminal task = %#v", task)
	}
}

func httptestResponse(handler http.Handler, method, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
	return response
}
