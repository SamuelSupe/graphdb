package sharding

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCatalogReadCallerCancelDoesNotTripCircuitBreaker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/raft/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"leader_id": 1})
		default:
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	catalog := Shard{ID: "catalog", ClusterID: "catalog", Peers: map[uint64]string{1: server.URL}}
	router := NewRouter(catalog, "secret")
	defer router.Client.HTTP.CloseIdleConnections()

	// Warm leader cache first.
	if _, err := router.Client.Leader(context.Background(), catalog); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var res map[string]any
	if err := router.catalogRead(callerCtx, "/cluster/identity", &res); err == nil {
		t.Fatal("expected canceled catalogRead to fail")
	}
	if router.catalogUnavailable.Load() {
		t.Fatal("caller cancellation must not trip global catalogUnavailable circuit breaker")
	}
}

func TestRouterBusinessConflictKeepsPlacementCacheWarm(t *testing.T) {
	var lookups atomic.Int64
	var mode atomic.Int32 // 0 = 200, 1 = business 409, 2 = shard_epoch_changed 409
	shard := Shard{ID: "data", ClusterID: "data", Peers: map[uint64]string{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/raft/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"leader_id": 1})
		case "/cluster/placement/tenant-a":
			lookups.Add(1)
			_ = json.NewEncoder(w).Encode(Resolution{
				Placement: Placement{Tenant: "tenant-a", Shard: "data", Epoch: 1, State: "active"},
				Shard:     shard,
			})
		default:
			switch mode.Load() {
			case 1:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"code":"version_conflict","error":"stale expected_version","retryable":false}`)
			case 2:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"code":"shard_epoch_changed","error":"moved","retryable":true}`)
			default:
				_, _ = io.WriteString(w, `{}`)
			}
		}
	}))
	defer server.Close()
	shard.Peers[1] = server.URL
	router := NewRouter(shard, "secret")
	defer router.Client.HTTP.CloseIdleConnections()

	doPost := func() int {
		req := httptest.NewRequest(http.MethodPost, "/v1/commits", strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer secret")
		req.Header.Set("X-Tenant-ID", "tenant-a")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := doPost(); code != http.StatusOK {
		t.Fatalf("warm post: %d", code)
	}
	if lookups.Load() != 1 {
		t.Fatalf("expected 1 lookup, got %d", lookups.Load())
	}

	// Ordinary business 409 must NOT invalidate placement cache.
	mode.Store(1)
	if code := doPost(); code != http.StatusConflict {
		t.Fatalf("business conflict: %d", code)
	}
	mode.Store(0)
	if code := doPost(); code != http.StatusOK {
		t.Fatalf("post after business conflict: %d", code)
	}
	if lookups.Load() != 1 {
		t.Fatalf("business 409 invalidated placement cache: lookups=%d", lookups.Load())
	}

	// shard_epoch_changed 409 MUST invalidate placement cache.
	mode.Store(2)
	if code := doPost(); code != http.StatusConflict {
		t.Fatalf("epoch conflict: %d", code)
	}
	router.mu.Lock()
	_, stillCached := router.placements["tenant-a"]
	router.mu.Unlock()
	if stillCached {
		t.Fatal("shard_epoch_changed 409 failed to invalidate placement cache")
	}
}
