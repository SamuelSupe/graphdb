package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"graphdb/internal/config"
	"graphdb/internal/httpapi"
	"graphdb/internal/storage"
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
