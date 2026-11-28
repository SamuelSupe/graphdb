package sharding

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Pause the transport's request length check until the client sees response
// headers, so a server-side close of the original body always wins the race.
type gatedProxyBody struct {
	io.ReadCloser
	ctx       context.Context
	release   <-chan struct{}
	remaining int64
}

func (b *gatedProxyBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		select {
		case <-b.release:
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}

func TestRouterFinishesQueryResponseWhileTransportFinishesRequest(t *testing.T) {
	for _, tc := range []struct{ name, path, contentType, first, last string }{
		{"json", "/v1/query", "application/json", `{"results":[`, "{\"id\":\"host:a\"}]}\n"},
		{"ndjson", "/v1/query/stream", "application/x-ndjson", "{\"version\":1}\n", "{\"id\":\"host:a\"}\n{\"done\":true}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			writeDone := make(chan error, 1)
			shard := Shard{ID: "data", ClusterID: "data", Peers: map[uint64]string{}}
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/raft/status":
					_, _ = io.WriteString(w, `{"node_id":1,"leader_id":1}`)
				case "/cluster/placement/tenant-a":
					_ = json.NewEncoder(w).Encode(Resolution{
						Placement: Placement{Tenant: "tenant-a", Shard: "data", Epoch: 1, State: "active"},
						Shard:     shard,
					})
				case "/cluster/data" + tc.path:
					controller := http.NewResponseController(w)
					if err := controller.EnableFullDuplex(); err != nil {
						t.Error(err)
						return
					}
					// Flush a response while the proxy still owns the request body.
					if _, err := io.CopyN(io.Discard, r.Body, 8192); err != nil {
						t.Error(err)
						return
					}
					w.Header().Set("Content-Type", tc.contentType)
					_, _ = io.WriteString(w, tc.first)
					if controller.Flush() != nil {
						return
					}
					select {
					case err := <-writeDone:
						if err != nil {
							panic(http.ErrAbortHandler)
						}
					case <-r.Context().Done():
						return
					}
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						return
					}
					_, _ = io.WriteString(w, tc.last)
				default:
					http.NotFound(w, r)
				}
			}))
			defer backend.Close()
			shard.Peers[1] = backend.URL
			router := NewRouter(shard, "secret")
			defer router.Client.HTTP.CloseIdleConnections()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Body = &gatedProxyBody{r.Body, r.Context(), release, r.ContentLength}
				trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
					writeDone <- info.Err
				}}
				router.ServeHTTP(w, r.WithContext(httptrace.WithClientTrace(r.Context(), trace)))
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+tc.path,
				strings.NewReader(strings.Repeat(" ", 64<<10)+`{"op":"match"}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Tenant-ID", "tenant-a")
			req.Header.Set("Content-Type", "application/json")
			resp, err := server.Client().Do(req)
			close(release)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK || err != nil || string(body) != tc.first+tc.last {
				t.Fatalf("query response status=%d body=%q error=%v", resp.StatusCode, body, err)
			}
		})
	}
}

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
