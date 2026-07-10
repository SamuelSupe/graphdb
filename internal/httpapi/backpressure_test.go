package httpapi

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/observability"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPCommitBackpressureReturns429(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	pressure := storage.NewWritePressure(storage.BackpressureConfig{ObjectLatencyThreshold: time.Millisecond})
	pressure.RecordObjectLatency(2 * time.Millisecond)
	store.Backpressure = pressure
	obs := observability.New(io.Discard, time.Second)
	handler := (&Server{
		Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second), Observability: obs,
	}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}})
	body := rr.Body.String()
	if rr.Code != 429 || rr.Header().Get("Retry-After") != "2" {
		t.Fatalf("status=%d retry=%q body=%s", rr.Code, rr.Header().Get("Retry-After"), body)
	}
	if !strings.Contains(body, `"error":"write backpressure"`) ||
		!strings.Contains(body, `"code":"object_store_unavailable"`) ||
		!strings.Contains(body, `"retryable":true`) ||
		!strings.Contains(body, `"detail"`) ||
		!strings.Contains(body, `"code":"object_store_latency_high"`) {
		t.Fatalf("body = %s, want backpressure envelope", body)
	}
	metrics := string(obs.Metrics.SnapshotPrometheus())
	if !strings.Contains(metrics, `graphdb_write_backpressure_total{tenant="tenant-a",reason="object_store_latency_high"} 1`) {
		t.Fatalf("metrics missing backpressure count:\n%s", metrics)
	}
}

func TestHTTPWriteAdmissionQueueTimeoutReturns429(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	store.Backpressure = storage.NewWritePressure(storage.BackpressureConfig{})
	admission := NewWriteAdmission(1, 1, time.Millisecond)
	release, _, err := admission.Acquire(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("pre-acquire: %v", err)
	}
	defer release()
	handler := (&Server{Store: store, Mode: "all", WriteAdmission: admission}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}})
	body := rr.Body.String()
	if rr.Code != 429 || !strings.Contains(body, `"code":"write_admission_queue_timeout"`) {
		t.Fatalf("status=%d body=%s, want admission backpressure", rr.Code, body)
	}
}

func TestHTTPIngestBackpressureReturns429WithoutFailedItems(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	pressure := storage.NewWritePressure(storage.BackpressureConfig{ObjectLatencyThreshold: time.Millisecond})
	pressure.RecordObjectLatency(2 * time.Millisecond)
	store.Backpressure = pressure
	handler := (&Server{Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second)}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/ingest/batches", "tenant-a", storage.IngestRequest{
		Source: "agent", CollectorID: "collector-a",
		Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "host:a", Kind: "host"}}},
	})
	body := rr.Body.String()
	if rr.Code != 429 || strings.Contains(body, `"failed"`) {
		t.Fatalf("status=%d body=%s, want 429 without ingest failures", rr.Code, body)
	}
}

func TestHTTPCommitTenantObjectCountBackpressureReturns429(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	pressure := storage.NewWritePressure(storage.BackpressureConfig{MaxObjectsPerTenant: 10})
	pressure.RecordTenantUsage("tenant-a", 10, 1024)
	store.Backpressure = pressure
	handler := (&Server{Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second)}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}})
	body := rr.Body.String()
	if rr.Code != 429 || !strings.Contains(body, `"code":"write_backpressure"`) || !strings.Contains(body, `"code":"tenant_object_count_high"`) {
		t.Fatalf("status=%d body=%s, want object count backpressure", rr.Code, body)
	}
}

func TestHTTPCommitWriteExecutionTimeoutReturnsGatewayTimeout(t *testing.T) {
	store := storage.NewTenantStore(&slowConditionalPutStore{ObjectStore: storage.NewMemoryStore(), delay: 50 * time.Millisecond}, "test")
	handler := (&Server{
		Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second), WriteExecutionTimeout: time.Millisecond,
	}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}})
	body := rr.Body.String()
	if rr.Code != 504 || !strings.Contains(body, `"code":"request_timeout"`) || !strings.Contains(body, `"retryable":true`) {
		t.Fatalf("status=%d body=%s, want request timeout", rr.Code, body)
	}
}

func TestHTTPIngestWriteExecutionTimeoutReturnsGatewayTimeout(t *testing.T) {
	store := storage.NewTenantStore(&slowConditionalPutStore{ObjectStore: storage.NewMemoryStore(), delay: 50 * time.Millisecond}, "test")
	handler := (&Server{
		Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second), WriteExecutionTimeout: time.Millisecond,
	}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/ingest/batches", "tenant-a", storage.IngestRequest{
		Source: "agent", CollectorID: "collector-a",
		Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "host:a", Kind: "host"}}},
	})
	body := rr.Body.String()
	if rr.Code != 504 || !strings.Contains(body, `"code":"request_timeout"`) || strings.Contains(body, `"failed"`) {
		t.Fatalf("status=%d body=%s, want request timeout without failed items", rr.Code, body)
	}
}

func TestHTTPCommitTimeoutAfterApplyReturnsGatewayTimeout(t *testing.T) {
	store := storage.NewTenantStore(&timeoutMatchingPutStore{
		ObjectStore: storage.NewMemoryStore(),
		contains:    "/idempotency/commits/",
	}, "test")
	handler := (&Server{
		Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second), WriteExecutionTimeout: 50 * time.Millisecond,
	}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/commits", "tenant-a", CommitRequest{
		IdempotencyKey: "idem-timeout",
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:after-apply", Kind: "host"}},
		},
	})
	body := rr.Body.String()
	if rr.Code != 504 || !strings.Contains(body, `"code":"request_timeout"`) || strings.Contains(body, `"code":"internal_error"`) {
		t.Fatalf("status=%d body=%s, want after-apply request timeout", rr.Code, body)
	}
	g, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want published commit", manifest.Version)
	}
	if _, ok := g.GetEntity("host:after-apply"); !ok {
		t.Fatal("published commit should remain visible after metadata timeout")
	}
}

func TestHTTPIngestTimeoutAfterApplyReturnsGatewayTimeout(t *testing.T) {
	store := storage.NewTenantStore(&timeoutMatchingPutStore{
		ObjectStore: storage.NewMemoryStore(),
		contains:    "/ingest/",
	}, "test")
	handler := (&Server{
		Store: store, Mode: "all", WriteAdmission: NewWriteAdmission(1, 1, time.Second), WriteExecutionTimeout: 50 * time.Millisecond,
	}).Handler()

	rr := serveJSON(handler, httpMethodPost, "/v1/ingest/batches", "tenant-a", storage.IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "batch-timeout", IdempotencyKey: "idem-timeout",
		Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "host:ingest-after-apply", Kind: "host"}}},
	})
	body := rr.Body.String()
	if rr.Code != 504 || !strings.Contains(body, `"code":"request_timeout"`) || strings.Contains(body, `"code":"internal_error"`) || strings.Contains(body, `"failed"`) {
		t.Fatalf("status=%d body=%s, want after-apply request timeout", rr.Code, body)
	}
	g, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want published ingest commit", manifest.Version)
	}
	if _, ok := g.GetEntity("host:ingest-after-apply"); !ok {
		t.Fatal("published ingest commit should remain visible after metadata timeout")
	}
}

const httpMethodPost = "POST"

type slowConditionalPutStore struct {
	storage.ObjectStore
	delay time.Duration
}

func (s *slowConditionalPutStore) PutConditional(ctx context.Context, key string, data []byte, condition storage.PutCondition) (storage.ObjectMeta, error) {
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return s.ObjectStore.PutConditional(ctx, key, data, condition)
	case <-ctx.Done():
		return storage.ObjectMeta{Key: key}, ctx.Err()
	}
}

type timeoutMatchingPutStore struct {
	storage.ObjectStore
	contains string
}

func (s *timeoutMatchingPutStore) PutConditional(ctx context.Context, key string, data []byte, condition storage.PutCondition) (storage.ObjectMeta, error) {
	if strings.Contains(key, s.contains) {
		<-ctx.Done()
		return storage.ObjectMeta{Key: key}, ctx.Err()
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}
