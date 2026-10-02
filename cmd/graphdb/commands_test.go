package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/config"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRuntimeRecoveryRestoresPendingWALAndOperationalIdentity(t *testing.T) {
	ctx, cancelTest := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelTest()
	data := filepath.Join(t.TempDir(), "data")
	walDir := filepath.Join(t.TempDir(), "wal")
	files, err := storage.OpenFileStore(data)
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewTenantStoreWithOptions(files, "graphdb", storage.TenantStoreOptions{InstanceID: "recovery"})
	request := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "committed", Cursor: "100", Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "host:a", Kind: "host"}}}}
	first, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hosts", ".hidden", ".tmp-hosts", ".runtime-restore-hosts"} {
		if _, err := store.SaveQuery(ctx, "tenant-a", storage.SavedQuery{Name: name, Request: query.Request{Op: "match", Kind: "host"}}); err != nil {
			t.Fatal(err)
		}
	}
	walConfig := storage.DefaultIngestServiceConfig(walDir)
	walConfig.OwnerID = "recovery"
	walConfig.FlushInterval = time.Hour
	service, err := storage.OpenIngestService(store, walConfig)
	if err != nil {
		t.Fatal(err)
	}
	request.BatchID = "pending"
	request.Cursor = "101"
	request.Items[0].Entity = &graph.Entity{ID: "host:b", Kind: "host"}
	accepted, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	closing, cancel := context.WithCancel(ctx)
	cancel()
	_ = service.Close(closing)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "runtime.backup")
	manifest := runtimeArchiveManifest{Format: 1, IngestMode: "wal", Roles: []string{"data", "wal"}}
	if err := backupRuntime(archive, map[string]string{"data": data, "wal": walDir}, manifest, 32<<20); err != nil {
		t.Fatal(err)
	}
	target := map[string]string{"data": filepath.Join(t.TempDir(), "data"), "wal": filepath.Join(t.TempDir(), "wal")}
	if err := restoreRuntime(archive, target, manifest, 32<<20, storage.DiskSpacePolicy{}); err != nil {
		t.Fatal(err)
	}

	t.Run("interrupted restore", func(t *testing.T) {
		raw, err := os.ReadFile(archive)
		if err != nil {
			t.Fatal(err)
		}
		contents, err := inspectRuntimeArchive(bytes.NewReader(raw), target, manifest, 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		partial := map[string]string{"data": filepath.Join(t.TempDir(), "data"), "wal": filepath.Join(t.TempDir(), "wal")}
		partial["wal"] = filepath.Join(partial["data"], "wal")
		for _, root := range partial {
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
		}
		name := contents.Files[0]
		role, relative, _ := runtimeArchiveName(name, target)
		original, err := os.ReadFile(filepath.Join(target[role], relative))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(partial[role], relative)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, original, 0600); err != nil {
			t.Fatal(err)
		}
		plan := runtimeRestorePlan{SHA256: hex.EncodeToString(raw[:sha256.Size]), Files: contents.Files}
		journal, _ := json.Marshal(plan)
		stage := filepath.Join(partial["wal"], ".runtime-restore-"+plan.SHA256[:16])
		if err := os.MkdirAll(stage, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stage, "unfinished"), []byte("staged"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(partial["data"], runtimeRestoreJournal), journal, 0600); err != nil {
			t.Fatal(err)
		}
		if files, err := storage.OpenFileStore(partial["data"]); err == nil {
			files.Close()
			t.Fatal("partial restore served data")
		}
		unexpected := filepath.Join(partial["data"], "operator-data")
		os.WriteFile(unexpected, []byte("keep"), 0600)
		if err := restoreRuntime(archive, partial, manifest, 32<<20, storage.DiskSpacePolicy{}); err == nil {
			t.Fatal("resume ignored unknown data")
		}
		if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, original) {
			t.Fatalf("failed resume deleted existing files: %v", err)
		}
		os.Remove(unexpected)
		if err := restoreRuntime(archive, partial, manifest, 32<<20, storage.DiskSpacePolicy{}); err != nil {
			t.Fatal(err)
		}
		if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, original) {
			t.Fatalf("resumed data differs: %v", err)
		}
	})
	restoredFiles, err := storage.OpenFileStore(target["data"])
	if err != nil {
		t.Fatal(err)
	}
	defer restoredFiles.Close()
	restored := storage.NewTenantStoreWithOptions(restoredFiles, "graphdb", storage.TenantStoreOptions{InstanceID: "recovery"})
	for _, name := range []string{"hosts", ".hidden", ".tmp-hosts", ".runtime-restore-hosts"} {
		if saved, err := restored.GetSavedQuery(ctx, "tenant-a", name); err != nil || saved.Request.Kind != "host" {
			t.Fatalf("saved query: %+v, %v", saved, err)
		}
	}
	walConfig.WAL.Dir = target["wal"]
	recovered, err := storage.OpenIngestService(restored, walConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close(context.Background())
	status, err := recovered.Status(ctx, "tenant-a", "agent", "collector", "pending")
	if err != nil || status.AcceptedLSN == 0 || !status.AcceptedAt.Equal(accepted.AcceptedAt) {
		t.Fatalf("pending identity: %+v, %v", status, err)
	}
	if err := recovered.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	replayed, err := recovered.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	result, err := recovered.Wait(ctx, replayed)
	if err != nil || result.Version != first.Version+1 {
		t.Fatalf("pending result: %+v, %v", result, err)
	}
	request.BatchID = "committed"
	request.Cursor = "100"
	request.Items[0].Entity = &graph.Entity{ID: "host:a", Kind: "host"}
	replayed, err = recovered.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	result, err = recovered.Wait(ctx, replayed)
	if err != nil || result.Version != first.Version {
		t.Fatalf("committed replay: %+v, %v", result, err)
	}
	g, current, err := restored.Load(ctx, "tenant-a")
	if err != nil || current.Version != first.Version+1 || len(g.Snapshot().Entities) != 2 {
		t.Fatalf("restored graph: %+v, %v", current, err)
	}
	body, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)-1] ^= 1
	corrupt := filepath.Join(t.TempDir(), "corrupt.backup")
	if err := os.WriteFile(corrupt, body, 0600); err != nil {
		t.Fatal(err)
	}
	fresh := map[string]string{"data": filepath.Join(t.TempDir(), "data"), "wal": filepath.Join(t.TempDir(), "wal")}
	if err := restoreRuntime(corrupt, fresh, manifest, 32<<20, storage.DiskSpacePolicy{}); err == nil {
		t.Fatal("corrupt archive restored")
	}
	if _, err := os.Stat(fresh["data"]); !os.IsNotExist(err) {
		t.Fatalf("corrupt archive changed target: %v", err)
	}
	if err := backupRuntime(filepath.Join(t.TempDir(), "live.backup"), target, manifest, 32<<20); err == nil {
		t.Fatal("live directory was archived")
	}
}

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

func TestRunHTTPServerCancelsRequestsAfterGracePeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	address := make(chan string, 1)
	entered, ended := make(chan struct{}), make(chan struct{})
	server := &http.Server{Addr: "127.0.0.1:0",
		BaseContext: func(listener net.Listener) context.Context {
			address <- listener.Addr().String()
			return context.Background()
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-r.Context().Done()
			close(ended)
		}),
	}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- runHTTPServer(ctx, server, 30*time.Millisecond) }()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := http.Get("http://" + <-address)
		if err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach server")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown did not report expired grace period: %v", err)
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("request context survived shutdown")
	}
	<-clientDone
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
