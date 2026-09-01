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

func TestPostgresCoordinatedIngestReactivatesLifecycleFailure(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-lifecycle-retry")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "lifecycle-writer"
	store.CoordinatorRetryLimit = 32
	store.SetCoordinator(coordinator)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	config := testIngestServiceConfig(t)
	config.OwnerID = store.InstanceID
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open ingest service: %v", err)
	}
	defer closeIngestService(t, service)
	flushTenant := func() {
		flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
			t.Fatalf("flush tenant: %v", err)
		}
	}

	request := ingestEntityRequest("batch-lifecycle-retry", "host:lifecycle")
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable coordinated tenant: %v", err)
	}
	failed, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept request while tenant disabled: %v", err)
	}
	flushTenant()
	failedResult, err := service.Wait(ctx, failed)
	if err != nil {
		t.Fatalf("wait lifecycle failure: %v", err)
	}
	if failedResult.Failed != 1 || failedResult.Applied != 0 || failedResult.Version != 0 {
		t.Fatalf("lifecycle failure result = %#v, want terminal failed result before CAS", failedResult)
	}
	failedRecord, err := store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("load persisted lifecycle failure: %v", err)
	}
	if failedRecord.Result.Failed != 1 || failedRecord.Result.Applied != 0 {
		t.Fatalf("persisted lifecycle failure = %#v", failedRecord.Result)
	}

	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
		t.Fatalf("reactivate coordinated tenant: %v", err)
	}
	retry, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept request after reactivation: %v", err)
	}
	flushTenant()
	retryResult, err := service.Wait(ctx, retry)
	if err != nil {
		t.Fatalf("wait request after reactivation: %v", err)
	}
	if retryResult.Version != 1 || retryResult.Applied != 1 || retryResult.Failed != 0 {
		t.Fatalf("reactivated request result = %#v, want successful CAS commit at version 1", retryResult)
	}
	committedRecord, err := store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("load replaced lifecycle metadata: %v", err)
	}
	if committedRecord.Result.Version != 1 || committedRecord.Result.Applied != 1 || committedRecord.Result.Failed != 0 {
		t.Fatalf("replaced lifecycle metadata = %#v, want committed result", committedRecord.Result)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 || head.Status != TenantStatusActive {
		t.Fatalf("head after lifecycle retry = %#v exists=%v err=%v", head, exists, err)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after lifecycle retry: %v", err)
	}
	if _, ok := loaded.GetEntity("host:lifecycle"); !ok {
		t.Fatal("reactivated lifecycle request did not publish its entity")
	}
}

func TestPostgresCoordinatedIngestWALLifecycleGenerationFence(t *testing.T) {
	ctx, writerCoordinator := newPostgresIntegrationCoordinator(
		t, "ingest-wal-lifecycle-generation",
	)
	lifecycleCoordinator, err := NewPostgresCoordinator(
		ctx,
		postgresTestDSN(t),
		writerCoordinator.schema,
		writerCoordinator.namespace,
	)
	if err != nil {
		t.Fatalf("new lifecycle coordinator: %v", err)
	}
	t.Cleanup(lifecycleCoordinator.Close)

	objects := NewMemoryStore()
	writerStore := NewTenantStore(objects, "test")
	writerStore.InstanceID = "writer-service"
	writerStore.CoordinatorRetryLimit = 32
	writerStore.SetCoordinator(writerCoordinator)
	lifecycleStore := NewTenantStore(objects, "test")
	lifecycleStore.InstanceID = "lifecycle-writer"
	lifecycleStore.CoordinatorRetryLimit = 32
	lifecycleStore.SetCoordinator(lifecycleCoordinator)
	if _, err := writerStore.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	config := testIngestServiceConfig(t)
	config.OwnerID = writerStore.InstanceID
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 8
	service, err := OpenIngestService(writerStore, config)
	if err != nil {
		t.Fatalf("open writer service: %v", err)
	}
	defer closeIngestService(t, service)

	before, exists, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head before accept exists=%v err=%v", exists, err)
	}
	request := ingestEntityRequest("batch-wal-generation-fence", "host:old-generation")
	accepted, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept pending WAL request: %v", err)
	}
	acceptedHead, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after accept: %v", err)
	}
	if accepted.pending == nil || accepted.pending.envelope.AcceptedGeneration != before.Generation {
		t.Fatalf("accepted WAL generation = %d, want %d", accepted.pending.envelope.AcceptedGeneration, before.Generation)
	}
	if acceptedHead != before {
		t.Fatalf("head changed before lifecycle transition: before=%#v accepted=%#v", before, acceptedHead)
	}

	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	disabled, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after disable: %v", err)
	}
	if disabled.Generation != before.Generation+1 || disabled.Status != TenantStatusDisabled ||
		disabled.GraphVersion != before.GraphVersion {
		t.Fatalf("disabled head = %#v, want generation %d and graph version %d", disabled, before.Generation+1, before.GraphVersion)
	}
	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
		t.Fatalf("reactivate tenant: %v", err)
	}
	active, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after reactivation: %v", err)
	}
	if active.Generation != before.Generation+2 || active.Status != TenantStatusActive ||
		active.GraphVersion != before.GraphVersion {
		t.Fatalf("active head = %#v, want generation %d and graph version %d", active, before.Generation+2, before.GraphVersion)
	}

	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		cancel()
		t.Fatalf("flush old-generation WAL request: %v", err)
	}
	cancel()
	result, err := service.Wait(ctx, accepted)
	if err != nil {
		t.Fatalf("wait old-generation WAL request: %v", err)
	}
	if result.Version != 0 || result.Applied != 0 || result.Failed != 1 {
		t.Fatalf("old-generation WAL result = %#v, want terminal failed result", result)
	}
	status, err := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("status after generation fence: %v", err)
	}
	if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed != 1 ||
		status.Result.Applied != 0 {
		t.Fatalf("status after generation fence = %#v, want failed terminal state", status)
	}

	after, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after old-generation WAL flush: %v", err)
	}
	if after.Generation != active.Generation || after.GraphVersion != before.GraphVersion {
		t.Fatalf("head after fenced WAL = %#v, want generation %d and graph version %d", after, active.Generation, before.GraphVersion)
	}
	loaded, manifest, err := writerStore.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after generation fence: %v", err)
	}
	if manifest.Version != before.GraphVersion {
		t.Fatalf("manifest after fenced WAL version = %d, want %d", manifest.Version, before.GraphVersion)
	}
	if _, ok := loaded.GetEntity("host:old-generation"); ok {
		t.Fatal("old-generation WAL request published an entity after lifecycle transition")
	}
}

func TestPostgresCoordinatedIngestLegacyWALGenerationBoundary(t *testing.T) {
	ctx, writerCoordinator := newPostgresIntegrationCoordinator(
		t, "ingest-wal-legacy-generation",
	)
	lifecycleCoordinator, err := NewPostgresCoordinator(
		ctx,
		postgresTestDSN(t),
		writerCoordinator.schema,
		writerCoordinator.namespace,
	)
	if err != nil {
		t.Fatalf("new lifecycle coordinator: %v", err)
	}
	t.Cleanup(lifecycleCoordinator.Close)

	objects := NewMemoryStore()
	writerStore := NewTenantStore(objects, "test")
	writerStore.InstanceID = "writer-service"
	writerStore.CoordinatorRetryLimit = 32
	writerStore.SetCoordinator(writerCoordinator)
	lifecycleStore := NewTenantStore(objects, "test")
	lifecycleStore.InstanceID = "lifecycle-writer"
	lifecycleStore.CoordinatorRetryLimit = 32
	lifecycleStore.SetCoordinator(lifecycleCoordinator)
	if _, err := writerStore.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	t.Run("generation-one-recovers", func(t *testing.T) {
		config := testIngestServiceConfig(t)
		config.OwnerID = writerStore.InstanceID
		config.FlushInterval = time.Hour
		config.FlushMaxRequests = 1
		request := writeLegacyAcceptedWAL(
			t, config, "tenant-a",
			ingestEntityRequest("batch-legacy-pg-generation-one", "host:legacy-pg-one"),
		)
		service, err := OpenIngestService(writerStore, config)
		if err != nil {
			t.Fatalf("open service from generation-one legacy WAL: %v", err)
		}
		defer closeIngestService(t, service)
		accepted, err := service.Accept(ctx, "tenant-a", request)
		if err != nil {
			t.Fatalf("accept recovered generation-one legacy WAL: %v", err)
		}
		if got := service.pendingAcceptedGeneration(accepted.pending); got != legacyUnboundIngestGeneration {
			t.Fatalf("legacy generation-one pending generation = %d, want %d", got, legacyUnboundIngestGeneration)
		}
		flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
			t.Fatalf("flush generation-one legacy WAL: %v", err)
		}
		result, err := service.Wait(flushCtx, accepted)
		if err != nil {
			t.Fatalf("wait generation-one legacy WAL: %v", err)
		}
		if result.Version != 1 || result.Applied != 1 || result.Failed != 0 {
			t.Fatalf("generation-one legacy WAL result = %#v, want successful version 1", result)
		}
		loaded, manifest, err := writerStore.Load(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("load generation-one graph: %v", err)
		}
		if manifest.Version != 1 {
			t.Fatalf("generation-one manifest version = %d, want 1", manifest.Version)
		}
		if _, ok := loaded.GetEntity("host:legacy-pg-one"); !ok {
			t.Fatal("generation-one legacy WAL entity is not visible")
		}
	})

	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant before legacy generation fence: %v", err)
	}
	if _, err := lifecycleStore.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
		t.Fatalf("reactivate tenant before legacy generation fence: %v", err)
	}
	active, _, err := writerCoordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after lifecycle transition: %v", err)
	}
	if active.Generation != 3 || active.Status != TenantStatusActive || active.GraphVersion != 1 {
		t.Fatalf("head after lifecycle transition = %#v, want generation 3 active graph version 1", active)
	}

	t.Run("generation-three-fences", func(t *testing.T) {
		config := testIngestServiceConfig(t)
		config.OwnerID = writerStore.InstanceID
		config.FlushInterval = time.Hour
		config.FlushMaxRequests = 1
		request := writeLegacyAcceptedWAL(
			t, config, "tenant-a",
			ingestEntityRequest("batch-legacy-pg-generation-three", "host:legacy-pg-three"),
		)
		service, err := OpenIngestService(writerStore, config)
		if err != nil {
			t.Fatalf("open service from generation-three legacy WAL: %v", err)
		}
		defer closeIngestService(t, service)
		accepted, err := service.Accept(ctx, "tenant-a", request)
		if err != nil {
			t.Fatalf("accept recovered generation-three legacy WAL: %v", err)
		}
		if got := service.pendingAcceptedGeneration(accepted.pending); got != legacyUnboundIngestGeneration {
			t.Fatalf("legacy generation-three pending generation = %d, want %d", got, legacyUnboundIngestGeneration)
		}
		flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
			t.Fatalf("flush generation-three legacy WAL: %v", err)
		}
		result, err := service.Wait(flushCtx, accepted)
		if err != nil {
			t.Fatalf("wait generation-three legacy WAL: %v", err)
		}
		if result.Version != 0 || result.Applied != 0 || result.Failed != 1 {
			t.Fatalf("generation-three legacy WAL result = %#v, want lifecycle-fenced failure", result)
		}
		status, err := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
		if err != nil {
			t.Fatalf("status after generation-three legacy WAL fence: %v", err)
		}
		if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed != 1 || status.Result.Applied != 0 {
			t.Fatalf("status after generation-three legacy WAL fence = %#v, want failed terminal state", status)
		}
		after, _, err := writerCoordinator.Head(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("head after generation-three legacy WAL fence: %v", err)
		}
		if after.Generation != active.Generation || after.GraphVersion != active.GraphVersion {
			t.Fatalf("head after generation-three legacy WAL fence = %#v, want generation %d graph version %d", after, active.Generation, active.GraphVersion)
		}
		loaded, manifest, err := writerStore.Load(ctx, "tenant-a")
		if err != nil {
			t.Fatalf("load generation-three graph: %v", err)
		}
		if manifest.Version != active.GraphVersion {
			t.Fatalf("generation-three manifest version = %d, want %d", manifest.Version, active.GraphVersion)
		}
		if _, ok := loaded.GetEntity("host:legacy-pg-three"); ok {
			t.Fatal("generation-three legacy WAL entity was published after lifecycle transition")
		}
	})
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
			!errors.Is(err, ErrTaskLeaseHeld) &&
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
