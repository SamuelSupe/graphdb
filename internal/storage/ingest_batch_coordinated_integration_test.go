package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatedIngestBatchesRebaseAcrossWriters(t *testing.T) {
	for _, writerCount := range []int{2, 4, 8} {
		writerCount := writerCount
		t.Run(fmt.Sprintf("%d-writers", writerCount), func(t *testing.T) {
			ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, fmt.Sprintf("ingest-rebase-%d", writerCount))
			dsn := postgresTestDSN(t)
			coordinators := make([]*PostgresCoordinator, 0, writerCount-1)
			t.Cleanup(func() {
				for _, coordinator := range coordinators {
					coordinator.Close()
				}
			})
			objects := NewMemoryStore()
			stores := make([]*TenantStore, writerCount)
			stores[0] = NewTenantStore(objects, "test")
			stores[0].InstanceID = "writer-0"
			stores[0].CoordinatorRetryLimit = 32
			stores[0].SetCoordinator(firstCoordinator)
			for index := 1; index < writerCount; index++ {
				coordinator, err := NewPostgresCoordinator(ctx, dsn, firstCoordinator.schema, firstCoordinator.namespace)
				if err != nil {
					t.Fatalf("new coordinator %d: %v", index, err)
				}
				coordinators = append(coordinators, coordinator)
				stores[index] = NewTenantStore(objects, "test")
				stores[index].InstanceID = fmt.Sprintf("writer-%d", index)
				stores[index].CoordinatorRetryLimit = 32
				stores[index].SetCoordinator(coordinator)
			}
			if _, err := stores[0].CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
				t.Fatalf("create coordinated tenant: %v", err)
			}

			requests := make([]IngestRequest, writerCount)
			for index := range requests {
				requests[index] = IngestRequest{
					Source: "agent", CollectorID: fmt.Sprintf("collector-%d", index), BatchID: fmt.Sprintf("batch-%d", index),
					Items: []IngestItem{{ExternalID: fmt.Sprintf("host:%d", index), Entity: &graph.Entity{ID: fmt.Sprintf("host:%d", index), Kind: "host"}}},
				}
			}
			start := make(chan struct{})
			results := make(chan struct {
				result IngestResult
				err    error
			}, writerCount)
			var wait sync.WaitGroup
			for index, store := range stores {
				index, store := index, store
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					result, err := ingestDurableWithExplicitRetry(ctx, store, "tenant-a", IngestBatchEntry{
						Request: requests[index], AcceptedAt: time.Now().UTC(),
					})
					results <- struct {
						result IngestResult
						err    error
					}{result: result, err: err}
				}()
			}
			close(start)
			wait.Wait()
			close(results)
			for outcome := range results {
				if outcome.err != nil {
					t.Fatalf("concurrent coordinated ingest: %v", outcome.err)
				}
				if outcome.result.Failed != 0 || outcome.result.Applied != 1 || outcome.result.Version <= 0 {
					t.Fatalf("coordinated ingest result = %#v", outcome.result)
				}
			}

			head, exists, err := firstCoordinator.Head(ctx, "tenant-a")
			if err != nil || !exists {
				t.Fatalf("coordinated head exists=%v err=%v", exists, err)
			}
			if head.GraphVersion != int64(writerCount) {
				t.Fatalf("coordinated head = %#v, want %d rebased versions", head, writerCount)
			}
			loaded, _, err := stores[0].Load(ctx, "tenant-a")
			if err != nil {
				t.Fatalf("load rebased graph: %v", err)
			}
			for index := range requests {
				id := fmt.Sprintf("host:%d", index)
				if _, ok := loaded.GetEntity(id); !ok {
					t.Fatalf("rebased graph is missing %s", id)
				}
			}
		})
	}
}

func TestPostgresCoordinatedIngestBatchesKeepTenantHeadsIndependent(t *testing.T) {
	ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, "ingest-multi-tenant")
	dsn := postgresTestDSN(t)
	const tenantCount = 4
	coordinators := make([]*PostgresCoordinator, 0, tenantCount-1)
	t.Cleanup(func() {
		for _, coordinator := range coordinators {
			coordinator.Close()
		}
	})
	objects := NewMemoryStore()
	stores := make([]*TenantStore, tenantCount)
	stores[0] = NewTenantStore(objects, "test")
	stores[0].InstanceID = "tenant-writer-0"
	stores[0].CoordinatorRetryLimit = 32
	stores[0].SetCoordinator(firstCoordinator)
	for index := 1; index < tenantCount; index++ {
		coordinator, err := NewPostgresCoordinator(ctx, dsn, firstCoordinator.schema, firstCoordinator.namespace)
		if err != nil {
			t.Fatalf("new tenant coordinator %d: %v", index, err)
		}
		coordinators = append(coordinators, coordinator)
		stores[index] = NewTenantStore(objects, "test")
		stores[index].InstanceID = fmt.Sprintf("tenant-writer-%d", index)
		stores[index].CoordinatorRetryLimit = 32
		stores[index].SetCoordinator(coordinator)
	}
	for index := range stores {
		if _, err := stores[index].CreateTenant(ctx, fmt.Sprintf("tenant-%d", index), TenantCreateOptions{}); err != nil {
			t.Fatalf("create tenant %d: %v", index, err)
		}
	}

	start := make(chan struct{})
	results := make(chan struct {
		index  int
		result IngestResult
		err    error
	}, tenantCount)
	var wait sync.WaitGroup
	for index, store := range stores {
		index, store := index, store
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := ingestDurableWithExplicitRetry(ctx, store, fmt.Sprintf("tenant-%d", index), IngestBatchEntry{
				Request: IngestRequest{
					Source: "agent", CollectorID: "collector-independent", BatchID: fmt.Sprintf("batch-%d", index),
					Items: []IngestItem{{ExternalID: fmt.Sprintf("host:%d", index), Entity: &graph.Entity{ID: fmt.Sprintf("host:%d", index), Kind: "host"}}},
				},
			})
			results <- struct {
				index  int
				result IngestResult
				err    error
			}{index: index, result: result, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("concurrent tenant %d ingest: %v", outcome.index, outcome.err)
		}
		if outcome.result.Version != 1 || outcome.result.Applied != 1 || outcome.result.Failed != 0 {
			t.Fatalf("tenant %d result = %#v, want independent version 1", outcome.index, outcome.result)
		}
	}
	for index, store := range stores {
		tenantID := fmt.Sprintf("tenant-%d", index)
		head, exists, err := firstCoordinator.Head(ctx, tenantID)
		if err != nil || !exists || head.GraphVersion != 1 {
			t.Fatalf("head %s = %#v exists=%v err=%v, want independent version 1", tenantID, head, exists, err)
		}
		loaded, _, err := store.Load(ctx, tenantID)
		if err != nil {
			t.Fatalf("load %s: %v", tenantID, err)
		}
		if _, ok := loaded.GetEntity(fmt.Sprintf("host:%d", index)); !ok {
			t.Fatalf("tenant %s graph missing host:%d", tenantID, index)
		}
	}
}

func TestPostgresCoordinatedIngestCrossWriterIdempotency(t *testing.T) {
	ctx, firstCoordinator := newPostgresIntegrationCoordinator(t, "ingest-idempotency")
	secondCoordinator, err := NewPostgresCoordinator(ctx, postgresTestDSN(t), firstCoordinator.schema, firstCoordinator.namespace)
	if err != nil {
		t.Fatalf("new second coordinator: %v", err)
	}
	t.Cleanup(secondCoordinator.Close)
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	first.InstanceID = "writer-a"
	first.CoordinatorRetryLimit = 32
	first.SetCoordinator(firstCoordinator)
	second := NewTenantStore(objects, "test")
	second.InstanceID = "writer-b"
	second.CoordinatorRetryLimit = 32
	second.SetCoordinator(secondCoordinator)
	if _, err := first.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}
	request := IngestRequest{
		Source: "agent", CollectorID: "collector-shared", BatchID: "batch-shared", IdempotencyKey: "idempotency-shared",
		Items: []IngestItem{{ExternalID: "host:shared", Entity: &graph.Entity{ID: "host:shared", Kind: "host"}}},
	}
	start := make(chan struct{})
	results := make(chan struct {
		result IngestResult
		err    error
	}, 2)
	var wait sync.WaitGroup
	for _, store := range []*TenantStore{first, second} {
		store := store
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := ingestDurableWithExplicitRetry(ctx, store, "tenant-a", IngestBatchEntry{Request: request})
			if err != nil {
				results <- struct {
					result IngestResult
					err    error
				}{err: err}
				return
			}
			results <- struct {
				result IngestResult
				err    error
			}{result: result}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var skipped int
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("cross-writer idempotent ingest: %v", outcome.err)
		}
		if outcome.result.Version != 1 || outcome.result.Failed != 0 || outcome.result.Applied != 1 {
			t.Fatalf("cross-writer result = %#v", outcome.result)
		}
		if outcome.result.Skipped {
			skipped++
		}
	}
	if skipped != 1 {
		t.Fatalf("cross-writer idempotent replay count = %d, want exactly one replay", skipped)
	}
	head, exists, err := firstCoordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 {
		t.Fatalf("head after idempotent race = %#v exists=%v err=%v", head, exists, err)
	}
}

func postgresTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	return dsn
}

// IngestDurableBatch is one coordinated attempt; the service scheduler owns
// retry/rebase. These direct-store integration cases model that boundary with
// an explicit retry loop so a single expected CAS race is not treated as a
// terminal ingest failure.
func ingestDurableWithExplicitRetry(
	ctx context.Context,
	store *TenantStore,
	tenantID string,
	entry IngestBatchEntry,
) (IngestResult, error) {
	var lastErr error
	for attempt := 0; attempt < 64; attempt++ {
		results, err := store.IngestDurableBatch(ctx, tenantID, []IngestBatchEntry{entry})
		if err == nil {
			if len(results) != 1 {
				return IngestResult{}, fmt.Errorf("coordinated ingest returned %d results", len(results))
			}
			return results[0], nil
		}
		if !errors.Is(err, ErrConflict) &&
			!errors.Is(err, ErrWriteConflict) &&
			!errors.Is(err, ErrIdempotencyInProgress) &&
			!errors.Is(err, ErrCoordinatorUnavailable) {
			return IngestResult{}, err
		}
		lastErr = err
		backoff := time.Duration(attempt+1) * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return IngestResult{}, ctx.Err()
		}
	}
	return IngestResult{}, fmt.Errorf("coordinated ingest retry budget exhausted: %w", lastErr)
}
