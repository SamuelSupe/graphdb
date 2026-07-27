package httpapi

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPReadMinVersionTimeoutDoesNotReportManifestAsVisible(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}

	readerObjects := &httpDelayedReadStore{ObjectStore: objects, delay: 10 * time.Millisecond}
	reader := storage.NewTenantStore(readerObjects, "test")
	cache := storage.NewReaderCache(reader, time.Hour)
	handler := (&Server{
		Store:                reader,
		Cache:                cache,
		Mode:                 "reader",
		ReaderCatchupTimeout: time.Millisecond,
	}).Handler()

	rr := serveJSON(handler, http.MethodGet, "/v1/entities/host:b?min_version=2", "tenant-a", nil)
	if rr.Code != http.StatusServiceUnavailable ||
		rr.Header().Get("Retry-After") == "" ||
		!strings.Contains(rr.Body.String(), `"code":"reader_not_fresh"`) ||
		!strings.Contains(rr.Body.String(), `"visible_version":0`) ||
		!strings.Contains(rr.Body.String(), `"required_version":2`) ||
		!strings.Contains(rr.Body.String(), `"reason":"catchup_timeout"`) {
		t.Fatalf("reader_not_fresh = %d headers=%#v body=%s", rr.Code, rr.Header(), rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), `"visible_version":2`) {
		t.Fatalf("cold reader reported manifest version as locally visible: %s", rr.Body.String())
	}
}

func TestHTTPCoordinatorOutageServesCacheAndFailsWritesClosed(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	cache := storage.NewReaderCache(store, time.Hour)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("prime reader cache: %v", err)
	}
	store.SetCoordinator(unavailableHTTPTestCoordinator{})
	handler := (&Server{Store: store, Cache: cache, Mode: "all"}).Handler()

	health := serveJSON(handler, http.MethodGet, "/v1/health", "", nil)
	if health.Code != http.StatusOK ||
		!strings.Contains(health.Body.String(), `"status":"degraded"`) ||
		!strings.Contains(health.Body.String(), `"available":false`) {
		t.Fatalf("outage health = %d body=%s", health.Code, health.Body.String())
	}
	readiness := serveJSON(handler, http.MethodGet, "/v1/readiness", "", nil)
	if readiness.Code != http.StatusServiceUnavailable ||
		!strings.Contains(readiness.Body.String(), `"status":"not_ready"`) ||
		!strings.Contains(readiness.Body.String(), `"available":false`) {
		t.Fatalf("outage readiness = %d body=%s", readiness.Code, readiness.Body.String())
	}
	cached := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-a", nil)
	if cached.Code != http.StatusOK ||
		!strings.Contains(cached.Body.String(), `"id":"host:a"`) ||
		!strings.Contains(cached.Body.String(), `"version":1`) {
		t.Fatalf("cached outage read = %d body=%s", cached.Code, cached.Body.String())
	}
	notFresh := serveJSON(handler, http.MethodGet, "/v1/entities/host:a?min_version=2", "tenant-a", nil)
	if notFresh.Code != http.StatusServiceUnavailable ||
		!strings.Contains(notFresh.Body.String(), `"code":"reader_not_fresh"`) ||
		!strings.Contains(notFresh.Body.String(), `"reason":"coordinator_unavailable"`) {
		t.Fatalf("outage version floor = %d body=%s", notFresh.Code, notFresh.Body.String())
	}
	write := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
		},
	})
	if write.Code != http.StatusServiceUnavailable ||
		!strings.Contains(write.Body.String(), `"code":"coordinator_unavailable"`) {
		t.Fatalf("outage write = %d body=%s", write.Code, write.Body.String())
	}
	for _, request := range []struct {
		name string
		path string
		body any
	}{
		{name: "source policy", path: "/v1/source-policy", body: graph.SourcePolicy{}},
		{name: "tenant config", path: "/v1/tenant-config", body: storage.TenantConfig{}},
	} {
		t.Run(request.name, func(t *testing.T) {
			response := serveJSON(handler, http.MethodPut, request.path, "tenant-a", request.body)
			if response.Code != http.StatusServiceUnavailable ||
				!strings.Contains(response.Body.String(), `"code":"coordinator_unavailable"`) {
				t.Fatalf("outage context write = %d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

type unavailableHTTPTestCoordinator struct {
	storage.WriteCoordinator
}

func (unavailableHTTPTestCoordinator) Backend() string {
	return storage.CoordinationPostgres
}

func (unavailableHTTPTestCoordinator) Namespace() string {
	return "unavailable-test"
}

func (unavailableHTTPTestCoordinator) Head(
	context.Context,
	string,
) (storage.CoordinationHead, bool, error) {
	return storage.CoordinationHead{}, false, storage.ErrCoordinatorUnavailable
}

func (unavailableHTTPTestCoordinator) Status(
	context.Context,
) (storage.CoordinatorStatus, error) {
	return storage.CoordinatorStatus{
		Backend:   storage.CoordinationPostgres,
		Available: false,
	}, storage.ErrCoordinatorUnavailable
}

type httpDelayedReadStore struct {
	storage.ObjectStore
	delay time.Duration
}

func (s *httpDelayedReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *httpDelayedReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, storage.ObjectMeta, error) {
	if err := s.wait(ctx); err != nil {
		return nil, storage.ObjectMeta{Key: key}, err
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *httpDelayedReadStore) wait(ctx context.Context) error {
	if s.delay <= 0 {
		return nil
	}
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
