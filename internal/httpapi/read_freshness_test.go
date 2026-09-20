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
