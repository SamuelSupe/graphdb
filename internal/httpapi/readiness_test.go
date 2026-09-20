package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPReadinessTracksObjectStoreAvailability(t *testing.T) {
	objects := &readinessProbeStore{ObjectStore: storage.NewMemoryStore()}
	store := storage.NewTenantStore(objects, "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()

	ready := serveJSON(handler, http.MethodGet, "/v1/readiness", "", nil)
	if ready.Code != http.StatusOK ||
		!strings.Contains(ready.Body.String(), `"status":"ready"`) ||
		!strings.Contains(ready.Body.String(), `"object_store":{"available":true`) {
		t.Fatalf("ready response = %d body=%s", ready.Code, ready.Body.String())
	}

	objects.setError(errors.New("object backend unavailable"))
	notReady := serveJSON(handler, http.MethodGet, "/v1/readiness", "", nil)
	if notReady.Code != http.StatusServiceUnavailable ||
		!strings.Contains(notReady.Body.String(), `"status":"not_ready"`) ||
		!strings.Contains(notReady.Body.String(), `"object_store":{"available":false`) ||
		!strings.Contains(notReady.Body.String(), `object backend unavailable`) {
		t.Fatalf("outage response = %d body=%s", notReady.Code, notReady.Body.String())
	}

	health := serveJSON(handler, http.MethodGet, "/v1/health", "", nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"ok"`) {
		t.Fatalf("liveness response = %d body=%s", health.Code, health.Body.String())
	}
}

type readinessProbeStore struct {
	storage.ObjectStore
	mu  sync.RWMutex
	err error
}

func (s *readinessProbeStore) Probe(context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *readinessProbeStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}
