package sharding

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRound6RouterRejectsPathTraversalAndStrictCloneCheck(t *testing.T) {
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	router := NewRouter(Shard{ID: "catalog", Peers: map[uint64]string{1: upstream.URL}}, "secret-operator-token")

	traversalPaths := []string{
		"/v1/entities/..%2F..%2Fcontrol%2Frepair",
		"/v1/entities/a/../../control/repair",
		"/v1/entities/%2e%2e/control/repair",
		"/v1/entities/./foo",
		"/v1/entities/foo\\bar",
	}
	for _, rawPath := range traversalPaths {
		req := httptest.NewRequest(http.MethodGet, "http://router.local"+rawPath, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("path %q: expected 400 Bad Request, got %d (%s)", rawPath, rec.Code, rec.Body.String())
		}
	}
	if upstreamHits.Load() != 0 {
		t.Fatalf("expected traversal paths never to reach upstream, got %d hits", upstreamHits.Load())
	}

	// Non-tenant POST ending in /clone must NOT be treated as a tenant clone route (it's non-public -> 403 without token, not 400 invalid tenant JSON).
	nonTenantClone := httptest.NewRequest(http.MethodPost, "http://router.local/v1/entities/foo/clone", strings.NewReader("not-json"))
	nonTenantClone.Header.Set("Authorization", "Bearer secret-operator-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, nonTenantClone)
	if rec.Code == http.StatusBadRequest && strings.Contains(rec.Body.String(), "invalid tenant request") {
		t.Fatalf("POST /v1/entities/foo/clone was misclassified as tenant clone route")
	}
}
