package sharding

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
)

func TestGraphAdmissionQueuesWithinOneHomeGroup(t *testing.T) {
	router := NewRouterWithGraphLimits(Shard{}, "token", GraphAdmissionLimits{Reads: 1, Writes: 1, Wait: time.Second})
	client := router.Client
	ctx := t.Context()
	admit := func(ctx context.Context, group, tenant, uri string) (func(), *httptest.ResponseRecorder) {
		request := httptest.NewRequest(http.MethodPost, uri, nil).WithContext(ctx)
		response := httptest.NewRecorder()
		return client.admitGraphRequest(response, request, partition.Group{ClusterID: group}, tenant), response
	}
	first, _ := admit(ctx, "a", "first", "/v1/query")
	if first == nil {
		t.Fatal("initial read was rejected")
	}
	waiterCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan func(), 1)
	go func() {
		release, _ := admit(waiterCtx, "a", "queued", "/v1/query")
		result <- release
	}()
	assertQueuedGraphReads(t, &client.graphAdmission, 1)

	read, _ := admit(ctx, "b", "first", "/v1/query")
	if read == nil {
		t.Fatal("a saturated home group blocked a different group")
	}
	read()
	write, _ := admit(ctx, "a", "first", "/v1/commits")
	if write == nil {
		t.Fatal("queued reads blocked the write lane")
	}
	write()
	release, response := admit(ctx, "a", "extra", "/v1/query")
	if release != nil || response.Code != http.StatusTooManyRequests ||
		response.Header().Get("Retry-After") != "1" || !strings.Contains(response.Body.String(), "graph_busy") {
		t.Fatalf("overflow response = %d %s", response.Code, response.Body.String())
	}
	first()
	select {
	case release := <-result:
		if release == nil {
			t.Fatal("queued request was not admitted after capacity became available")
		}
		release()
	case <-time.After(time.Second):
		t.Fatal("queued request did not wake")
	}
}

func TestGraphAdmissionRemovesCanceledAndExpiredWaiters(t *testing.T) {
	for _, wait := range []time.Duration{time.Second, 10 * time.Millisecond} {
		t.Run(wait.String(), func(t *testing.T) {
			a := graphAdmission{limits: GraphAdmissionLimits{Reads: 1, Writes: 1, Wait: wait}}
			scope := graphScope{"group", "tenant"}
			first := a.acquire(t.Context(), scope, false)
			if first == nil {
				t.Fatal("initial read was rejected")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if wait == time.Second {
				done := make(chan func(), 1)
				go func() { done <- a.acquire(ctx, scope, false) }()
				assertQueuedGraphReads(t, &a, 1)
				cancel()
				if release := <-done; release != nil {
					t.Fatal("canceled request got an active slot")
				}
			} else if release := a.acquire(ctx, scope, false); release != nil {
				t.Fatal("expired request got an active slot")
			}
			first()
			assertQueuedGraphReads(t, &a, 0)
			release := a.acquire(t.Context(), scope, false)
			if release == nil {
				t.Fatal("the waiter leaked an active slot")
			}
			release()
		})
	}
}

func assertQueuedGraphReads(t *testing.T, a *graphAdmission, want int) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		a.mu.Lock()
		count := a.read.queue.Len()
		a.mu.Unlock()
		if count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued reads = %d, want %d", count, want)
		}
		time.Sleep(time.Millisecond)
	}
}
