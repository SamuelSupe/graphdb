package sharding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestLeaderCacheInvalidationDoesNotReplayMutations(t *testing.T) {
	var leader, probes, mutations atomic.Int64
	leader.Store(1)
	shard := Shard{ID: "data", ClusterID: "data", Peers: map[uint64]string{}}
	for id := uint64(1); id <= 3; id++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/raft/status" {
				probes.Add(1)
				json.NewEncoder(w).Encode(map[string]any{"leader_id": leader.Load()})
				return
			}
			mutations.Add(1)
			if int64(id) != leader.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}))
		t.Cleanup(server.Close)
		shard.Peers[id] = server.URL
	}
	client := NewClient("test")
	defer client.HTTP.CloseIdleConnections()
	for range 100 {
		if _, err := client.Leader(context.Background(), shard); err != nil {
			t.Fatal(err)
		}
	}
	if probes.Load() != 1 {
		t.Fatalf("cache still probes for each request: %d", probes.Load())
	}
	leader.Store(2)
	response, err := client.Do(context.Background(), shard, http.MethodPost, "/write", map[string]any{"id": "one"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || mutations.Load() != 1 {
		t.Fatalf("ambiguous mutation was replayed: status=%d attempts=%d", response.StatusCode, mutations.Load())
	}
	response, err = client.Do(context.Background(), shard, http.MethodPost, "/write", map[string]any{"id": "one"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || mutations.Load() != 2 || probes.Load() != 2 {
		t.Fatalf("explicit retry did not discover the new leader: status=%d attempts=%d probes=%d", response.StatusCode, mutations.Load(), probes.Load())
	}
	var metrics bytes.Buffer
	client.WriteMetrics(&metrics)
	for _, want := range []string{
		`graphdb_sharding_client_events_total{event="leader_cache_hit"} 100`,
		`graphdb_sharding_client_operation_seconds_count{operation="write_request",status="error"} 1`,
		`graphdb_sharding_client_operation_seconds_count{operation="write_request",status="ok"} 1`,
		`graphdb_sharding_client_operations_inflight{operation="write_request"} 0`,
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("leader retry diagnostics missing %s: %s", want, &metrics)
		}
	}
}

func TestRouterDrainRemovesReadinessButKeepsInFlightRouting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/raft/status":
			json.NewEncoder(w).Encode(map[string]any{"leader_id": 1})
		case "/cluster/identity":
			json.NewEncoder(w).Encode(map[string]any{"catalog": true})
		case "/cluster/catalog":
			json.NewEncoder(w).Encode(Catalog{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	router := NewRouter(Shard{ID: "catalog", ClusterID: "catalog", Peers: map[uint64]string{1: server.URL}}, "secret")
	defer router.Client.HTTP.CloseIdleConnections()
	call := func(method, path, token string) int {
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w.Code
	}
	if call("POST", "/v1/router/drain", "wrong") != http.StatusUnauthorized || call("GET", "/v1/readiness", "") != http.StatusOK {
		t.Fatal("unauthorized request drained router")
	}
	if call("POST", "/v1/router/drain", "secret") != http.StatusNoContent || call("GET", "/v1/readiness", "") != http.StatusServiceUnavailable {
		t.Fatal("drain did not remove router readiness")
	}
	if call("GET", "/v1/tenants", "") != http.StatusOK {
		t.Fatal("drain interrupted an existing data route")
	}
	if call("POST", "/v1/router/resume", "secret") != http.StatusNoContent || call("GET", "/v1/readiness", "") != http.StatusOK {
		t.Fatal("router did not resume")
	}
	server.Close()
	call("POST", "/v1/router/drain", "secret")
	for _, path := range []string{"/metrics", "/v1/diagnostics"} {
		if call("GET", path, "wrong") != http.StatusUnauthorized || call("POST", path, "secret") != http.StatusMethodNotAllowed {
			t.Fatalf("diagnostics authorization/method contract failed: %s", path)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		request := httptest.NewRequest("GET", path, nil).WithContext(ctx)
		request.Header.Set("Authorization", "Bearer secret")
		writer := httptest.NewRecorder()
		router.ServeHTTP(writer, request)
		if writer.Code != http.StatusOK {
			t.Fatalf("drained router diagnostics contacted unavailable catalog: %d %s", writer.Code, writer.Body)
		}
		if path == "/metrics" && !strings.Contains(writer.Body.String(), "graphdb_router_draining 1") {
			t.Fatalf("missing local drain metric: %s", writer.Body)
		}
	}
}

func TestRouterCatalogFallbackRemainsBoundedAndFenced(t *testing.T) {
	var unavailable atomic.Bool
	var state atomic.Int64
	state.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/raft/status":
			json.NewEncoder(w).Encode(map[string]any{"leader_id": 1})
		case "/cluster/placement/known":
			placement := Placement{Tenant: "known", Shard: "data", Epoch: 1, State: "active"}
			if state.Load() == 2 {
				placement.State = "moving"
			}
			json.NewEncoder(w).Encode(Resolution{Placement: placement, Shard: Shard{ID: "data"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	router := NewRouter(Shard{ID: "catalog", ClusterID: "catalog", Peers: map[uint64]string{1: server.URL}}, "secret")
	defer router.Client.HTTP.CloseIdleConnections()
	ctx := context.Background()
	if _, err := router.resolve(ctx, "known"); err != nil {
		t.Fatal(err)
	}
	unavailable.Store(true)
	router.mu.Lock()
	cached := router.placements["known"]
	cached.expires = time.Now().Add(-time.Second)
	router.placements["known"] = cached
	router.mu.Unlock()
	if route, err := router.resolve(ctx, "known"); err != nil || route.Placement.Epoch != 1 {
		t.Fatalf("known route unavailable: %+v, %v", route, err)
	}
	if _, err := router.resolve(ctx, "unknown"); err == nil {
		t.Fatal("invented a route during catalog outage")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := router.resolve(canceled, "known"); err == nil {
		t.Fatal("fallback ignored caller cancellation")
	}
	router.mu.Lock()
	cached.retainUntil = time.Now().Add(-time.Second)
	router.placements["known"] = cached
	router.mu.Unlock()
	if _, err := router.resolve(ctx, "known"); err == nil {
		t.Fatal("fallback outlived its bound")
	}
	unavailable.Store(false)
	router.catalogRetryAt.Store(0)
	if _, err := router.resolve(ctx, "known"); err != nil {
		t.Fatal(err)
	}
	state.Store(2)
	router.mu.Lock()
	cached = router.placements["known"]
	cached.expires = time.Now().Add(-time.Second)
	router.placements["known"] = cached
	router.mu.Unlock()
	if _, err := router.resolve(ctx, "known"); err == nil {
		t.Fatal("ignored authoritative migration state")
	}
	unavailable.Store(true)
	if _, err := router.resolve(ctx, "known"); err == nil {
		t.Fatal("resurrected route invalidated by migration")
	}
}

func TestRouterTenantPagesUseCatalogOrderAndOwnershipHeaders(t *testing.T) {
	state := Catalog{Shards: map[string]Shard{}, Tenants: map[string]Placement{}}
	var detailReads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/raft/status":
			json.NewEncoder(w).Encode(map[string]any{"leader_id": 1})
		case "/cluster/catalog":
			json.NewEncoder(w).Encode(state)
		default:
			id := strings.TrimPrefix(r.URL.Path, "/cluster/data/v1/tenants/")
			if _, ok := state.Tenants[id]; !ok {
				http.NotFound(w, r)
				return
			}
			detailReads.Add(1)
			if r.Header.Get("X-Tenant-ID") != id || r.Header.Get(EpochHeader) != "7" || r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("X-Raft-Cluster") != "data" {
				t.Errorf("routing headers=%v", r.Header)
			}
			json.NewEncoder(w).Encode(storage.TenantInfo{TenantMetadata: storage.TenantMetadata{TenantID: id}})
		}
	}))
	defer server.Close()
	state.Shards["data"] = Shard{ID: "data", ClusterID: "data", Peers: map[uint64]string{1: server.URL}}
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("tenant-%05d", i)
		state.Tenants[id] = Placement{Tenant: id, Shard: "data", Epoch: 7}
	}
	router := NewRouter(Shard{ID: "catalog", ClusterID: "catalog", Peers: map[uint64]string{1: server.URL}}, "secret")
	defer router.Client.HTTP.CloseIdleConnections()
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants?limit=25&cursor=tenant-09974", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)
	var page storage.TenantInfoPage
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Tenants) != 25 || page.Total != 10000 || page.Tenants[0].TenantID != "tenant-09975" || page.NextCursor != "" || detailReads.Load() != 25 {
		t.Fatalf("page=%s reads=%d", w.Body.String(), detailReads.Load())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/cluster?limit=25&q=TENANT-099", nil)
	request.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, request)
	var catalogPage struct {
		Tenants map[string]Placement `json:"tenants"`
		Total   int                  `json:"total"`
		Next    string               `json:"next_cursor"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &catalogPage) != nil || len(catalogPage.Tenants) != 25 || catalogPage.Total != 100 || catalogPage.Next != "tenant-09924" {
		t.Fatalf("catalog page=%s", w.Body.String())
	}
}

func TestRouterRequiresOperatorTokenForAdministrativeForwarding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
	defer server.Close()
	router := NewRouter(Shard{ID: "catalog", ClusterID: "catalog", Peers: map[uint64]string{1: server.URL}}, "secret")
	defer router.Client.HTTP.CloseIdleConnections()
	for _, route := range []struct{ method, path string }{{"POST", "/v1/tenants/tenant-a/purge"}, {"POST", "/v1/control/gc"}, {"POST", "/v1/control/recover"}, {"PUT", "/v1/tenant-config"}} {
		request := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
		request.Header.Set("X-Tenant-ID", "tenant-a")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Errorf("unauthorized administration %s returned %d", route.path, response.Code)
		}
	}
}

func TestLeaderDiscoveryHedgesAndCoalesces(t *testing.T) {
	var probes atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { probes.Add(1); <-r.Context().Done() }))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		time.Sleep(20 * time.Millisecond)
		json.NewEncoder(w).Encode(map[string]any{"leader_id": 2})
	}))
	defer fast.Close()
	client := NewClient("token")
	shard := Shard{ClusterID: "cluster", ID: "a", Peers: map[uint64]string{1: slow.URL, 2: fast.URL}}
	start := time.Now()
	var wg sync.WaitGroup
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			origin, err := client.Leader(context.Background(), shard)
			if err != nil || origin != fast.URL {
				t.Errorf("leader=%s err=%v", origin, err)
			}
		}()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("dead peer stalls discovery: %s", elapsed)
	}
	if n := probes.Load(); n > 3 {
		t.Errorf("discovery stampede: %d probes", n)
	}
}

func TestRouterLostMutationResponseIsNotRetryable(t *testing.T) {
	var mutations atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/raft/status" {
			json.NewEncoder(w).Encode(map[string]any{"leader_id": 1})
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		mutations.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer backend.Close()
	shard := Shard{ID: "a", ClusterID: "c", Peers: map[uint64]string{1: backend.URL}}
	router := NewRouter(shard, "token")
	defer router.Client.HTTP.CloseIdleConnections()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/commits", strings.NewReader("{}"))
	router.proxy(response, request, shard, 1, "tenant-a")
	var failure struct {
		Code      string
		Error     string
		Message   string
		Retryable bool
	}
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil || failure.Code != "write_outcome_unknown" || failure.Message == "" || failure.Message != failure.Error || failure.Retryable || response.Code != http.StatusBadGateway || mutations.Load() != 1 {
		t.Fatalf("lost response outcome: %d %s mutations=%d err=%v", response.Code, response.Body.String(), mutations.Load(), err)
	}
}
