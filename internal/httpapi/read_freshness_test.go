package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHTTPDefaultColdReadUsesLoadBudget(t *testing.T) {
	for _, path := range []string{"/v1/export/snapshot", "/v1/entities?limit=10", "/v1/edges?limit=10", "/v1/entities/host:a"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			objects := storage.NewMemoryStore()
			writer := storage.NewTenantStore(objects, "test")
			if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
			}, storage.CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			reader := storage.NewTenantStore(&httpDelayedReadStore{ObjectStore: objects, delay: 10 * time.Millisecond}, "test")
			cache := storage.NewReaderCache(reader, time.Hour)
			cache.LoadTimeout = time.Second
			handler := (&Server{Store: reader, Cache: cache, Mode: "reader", ReaderCatchupTimeout: time.Millisecond}).Handler()
			rr := serveJSON(handler, http.MethodGet, path, "tenant-a", nil)
			if rr.Code != http.StatusOK {
				t.Fatalf("default cold read = %d body=%s", rr.Code, rr.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if version, exists := body["version"]; exists && version != float64(1) {
				t.Fatalf("cold read returned version %v, want 1", version)
			}
		})
	}
}

func TestHTTPDefaultWarmReaderStillBoundsCatchup(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	reader := storage.NewTenantStore(&httpDelayedReadStore{ObjectStore: objects, delay: 10 * time.Millisecond}, "test")
	cache := storage.NewReaderCache(reader, time.Hour)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: reader, Cache: cache, Mode: "reader", ReaderCatchupTimeout: time.Millisecond}).Handler()
	rr := serveJSON(handler, http.MethodGet, "/v1/export/snapshot", "tenant-a", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("lagging warm read = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestColdReadPreservesCallerDeadline(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	server := &Server{Store: store, Cache: storage.NewReaderCache(store, time.Hour)}
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	target := readTarget{TargetVersion: 1}
	_, _, err := server.loadGraphForRead(ctx, "tenant-a", target)
	if _, fresh := asReaderNotFresh(err); fresh || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cold graph load replaced caller deadline: %v", err)
	}
	err = server.withReadOnlyGraphForRead(ctx, "tenant-a", target, func(*graph.Graph, storage.Manifest) error {
		t.Fatal("expired request reached the read callback")
		return nil
	})
	if _, fresh := asReaderNotFresh(err); fresh || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cold read view replaced caller deadline: %v", err)
	}
}

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
