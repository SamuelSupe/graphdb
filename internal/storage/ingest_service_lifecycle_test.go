package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestServiceFinalizesLifecycleFenceAsFailed(t *testing.T) {
	store := &lifecycleFencingIngestStore{err: ErrTenantDeleted}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	request := IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "batch-deleted",
		Items: []IngestItem{{
			ExternalID: "host-a",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush after lifecycle fence: %v", err)
	}
	result, err := service.Wait(flushCtx, accepted)
	if err != nil {
		t.Fatalf("wait for terminal lifecycle failure: %v", err)
	}
	if result.Applied != 0 || result.Failed != 1 || len(result.Failures) != 1 ||
		!strings.Contains(result.Failures[0].Error, ErrTenantDeleted.Error()) {
		t.Fatalf("terminal lifecycle result = %#v", result)
	}

	status, err := service.Status(flushCtx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("status after terminal lifecycle failure: %v", err)
	}
	if status.State != IngestStateFailed || status.RecoveryPending || status.Result == nil || status.Result.Failed != 1 {
		t.Fatalf("status after lifecycle fence = %#v", status)
	}
}

func TestIngestServiceGenerationCaptureSingleflight(t *testing.T) {
	store := newGenerationCacheIngestStore(1)
	store.block = true
	service := newGenerationCacheTestService(store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type captureResult struct {
		generation int64
		err        error
	}
	first := make(chan captureResult, 1)
	go func() {
		generation, err := service.captureIngestWALGeneration(ctx, "tenant-a")
		first <- captureResult{generation: generation, err: err}
	}()
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("initial generation capture did not start")
	}

	const followerCount = 8
	ready := make(chan struct{}, followerCount)
	start := make(chan struct{})
	followers := make(chan captureResult, followerCount)
	for range followerCount {
		go func() {
			ready <- struct{}{}
			<-start
			generation, err := service.captureIngestWALGeneration(ctx, "tenant-a")
			followers <- captureResult{generation: generation, err: err}
		}()
	}
	for range followerCount {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("generation singleflight follower did not become ready")
		}
	}
	close(start)
	deadline := time.Now().Add(time.Second)
	for store.captureCalls() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := store.captureCalls(); got != 1 {
		t.Fatalf("generation capture calls while flight is blocked = %d, want 1", got)
	}
	close(store.release)

	select {
	case result := <-first:
		if result.err != nil || result.generation != 1 {
			t.Fatalf("initial generation result = %#v, want generation 1", result)
		}
	case <-ctx.Done():
		t.Fatal("initial generation capture did not finish")
	}
	for index := 0; index < followerCount; index++ {
		select {
		case result := <-followers:
			if result.err != nil || result.generation != 1 {
				t.Fatalf("singleflight follower result %d = %#v, want generation 1", index, result)
			}
		case <-ctx.Done():
			t.Fatalf("singleflight follower %d did not finish", index)
		}
	}
}

func TestIngestServiceGenerationCaptureStaleRefreshReturnsCachedGeneration(t *testing.T) {
	store := newGenerationCacheIngestStore(1)
	service := newGenerationCacheTestService(store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	generation, err := service.captureIngestWALGeneration(ctx, "tenant-a")
	if err != nil || generation != 1 {
		t.Fatalf("initial generation = %d err=%v, want generation 1", generation, err)
	}
	if got := store.captureCalls(); got != 1 {
		t.Fatalf("initial generation capture calls = %d, want 1", got)
	}
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("initial generation capture signal was not delivered")
	}

	service.generationMu.Lock()
	service.generations["tenant-a"] = ingestGenerationCacheEntry{
		generation: 1,
		expiresAt:  time.Now().Add(-time.Second),
	}
	service.generationMu.Unlock()
	store.setGeneration(2)
	store.block = true

	started := time.Now()
	stale, err := service.captureIngestWALGeneration(ctx, "tenant-a")
	if err != nil || stale != 1 {
		t.Fatalf("stale generation = %d err=%v, want cached generation 1", stale, err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("stale generation refresh blocked caller for %s", elapsed)
	}
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("stale generation refresh did not start")
	}

	secondStale, err := service.captureIngestWALGeneration(ctx, "tenant-a")
	if err != nil || secondStale != 1 {
		t.Fatalf("concurrent stale generation = %d err=%v, want cached generation 1", secondStale, err)
	}
	if got := store.captureCalls(); got != 2 {
		t.Fatalf("stale refresh capture calls = %d, want one refresh after initial load", got)
	}

	close(store.release)
	deadline := time.Now().Add(time.Second)
	for {
		service.generationMu.Lock()
		cached := service.generations["tenant-a"]
		active := service.generationLoad["tenant-a"]
		service.generationMu.Unlock()
		if active == nil && cached.generation == 2 && time.Now().Before(cached.expiresAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale generation refresh did not publish generation 2: cached=%#v active=%v", cached, active != nil)
		}
		time.Sleep(time.Millisecond)
	}
	fresh, err := service.captureIngestWALGeneration(ctx, "tenant-a")
	if err != nil || fresh != 2 {
		t.Fatalf("refreshed generation = %d err=%v, want generation 2", fresh, err)
	}
	if got := store.captureCalls(); got != 2 {
		t.Fatalf("generation capture calls after fresh cache hit = %d, want 2", got)
	}
}

func TestIngestServiceGenerationInvalidationDetachesOldRefresh(t *testing.T) {
	store := newGenerationCacheIngestStore(1)
	store.setBlock(true)
	service := newGenerationCacheTestService(store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type captureResult struct {
		generation int64
		err        error
	}
	oldDone := make(chan captureResult, 1)
	go func() {
		generation, err := service.captureIngestWALGeneration(ctx, "tenant-a")
		oldDone <- captureResult{generation: generation, err: err}
	}()
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("old generation refresh did not start")
	}

	store.setGeneration(2)
	service.invalidateIngestWALGeneration("tenant-a")
	store.setBlock(false)
	fresh, err := service.captureIngestWALGeneration(ctx, "tenant-a")
	if err != nil || fresh != 2 {
		t.Fatalf("generation after lifecycle invalidation = %d err=%v, want generation 2", fresh, err)
	}
	if got := store.captureCalls(); got != 2 {
		t.Fatalf("generation captures after invalidation = %d, want old and new flights", got)
	}

	close(store.release)
	select {
	case result := <-oldDone:
		if result.err != nil || result.generation != 1 {
			t.Fatalf("old generation flight result = %#v, want old generation 1", result)
		}
	case <-ctx.Done():
		t.Fatal("old generation flight did not finish")
	}

	service.generationMu.Lock()
	cached := service.generations["tenant-a"]
	active := service.generationLoad["tenant-a"]
	service.generationMu.Unlock()
	if cached.generation != 2 || !time.Now().Before(cached.expiresAt) || active != nil {
		t.Fatalf("cache after detached old flight = %#v active=%v, want generation 2 and no active flight", cached, active != nil)
	}
	final, err := service.captureIngestWALGeneration(ctx, "tenant-a")
	if err != nil || final != 2 {
		t.Fatalf("generation after old flight completion = %d err=%v, want generation 2", final, err)
	}
	if got := store.captureCalls(); got != 2 {
		t.Fatalf("generation captures after cache recheck = %d, want no stale overwrite", got)
	}
}

func TestIngestServiceFencesOldGenerationBeforeDisabledStatus(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-service-generation-disabled")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.InstanceID = "generation-order-writer"
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

	request := ingestEntityRequest("batch-generation-before-disabled", "host:old-generation-disabled")
	accepted, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept active-generation request: %v", err)
	}
	before, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after accept exists=%v err=%v", exists, err)
	}
	if accepted.pending == nil || accepted.pending.envelope.AcceptedGeneration != before.Generation {
		t.Fatalf("accepted generation = %d, want current generation %d", accepted.pending.envelope.AcceptedGeneration, before.Generation)
	}

	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant after accept: %v", err)
	}
	disabled, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after disable exists=%v err=%v", exists, err)
	}
	if disabled.Generation != before.Generation+1 || disabled.Status != TenantStatusDisabled ||
		disabled.GraphVersion != before.GraphVersion {
		t.Fatalf("disabled head = %#v, want generation %d and unchanged graph version %d", disabled, before.Generation+1, before.GraphVersion)
	}

	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush old-generation request while disabled: %v", err)
	}
	result, err := service.Wait(flushCtx, accepted)
	if err != nil {
		t.Fatalf("wait old-generation request: %v", err)
	}
	if result.Version != 0 || result.Applied != 0 || result.Failed != 1 || len(result.Failures) != 1 ||
		!strings.Contains(result.Failures[0].Error, ErrTenantDeleted.Error()) ||
		!strings.Contains(result.Failures[0].Error, errIngestGenerationFenced.Error()) {
		t.Fatalf("old-generation result = %#v, want generation-fenced terminal failure", result)
	}
	if _, err := store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canonical ingest record after generation fence = %v, want ErrNotFound", err)
	}
	after, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after generation fence exists=%v err=%v", exists, err)
	}
	if after.Generation != disabled.Generation || after.Status != TenantStatusDisabled ||
		after.GraphVersion != disabled.GraphVersion {
		t.Fatalf("head after generation fence = %#v, want unchanged disabled head %#v", after, disabled)
	}
}

func TestIngestServiceRestoresLifecycleFailureStatusAfterRestart(t *testing.T) {
	store := &lifecycleFencingIngestStore{err: ErrTenantDeleted}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}

	request := IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "batch-restart-deleted",
		Items: []IngestItem{{
			ExternalID: "host-a",
			Entity:     &graph.Entity{ID: "host:a", Kind: "host"},
		}},
	}
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatal(err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush after lifecycle fence: %v", err)
	}
	if _, err := service.Wait(flushCtx, accepted); err != nil {
		t.Fatalf("wait for lifecycle failure: %v", err)
	}
	closeIngestService(t, service)

	reopened, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, reopened)
	status, err := reopened.Status(
		flushCtx, "tenant-a", request.Source, request.CollectorID, request.BatchID,
	)
	if err != nil {
		t.Fatalf("status after restart: %v", err)
	}
	if status.State != IngestStateFailed || status.RecoveryPending || status.Result == nil ||
		status.Result.Failed != 1 || status.Result.Applied != 0 {
		t.Fatalf("status after restart = %#v", status)
	}
}

func TestIngestServiceStatusRestoresPersistedFailureState(t *testing.T) {
	started := time.Unix(100, 0).UTC()
	finished := started.Add(time.Second)
	store := &lifecycleFencingIngestStore{
		record: &IngestBatchRecord{
			TenantID: "tenant-a",
			Request: IngestRequest{
				Source: "agent", CollectorID: "collector-a", BatchID: "batch-persisted-failed",
			},
			Result: IngestResult{
				BatchID: "batch-persisted-failed",
				Failed:  1,
			},
			StartedAt:  started,
			FinishedAt: finished,
		},
	}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)

	status, err := service.Status(context.Background(), "tenant-a", "agent", "collector-a", "batch-persisted-failed")
	if err != nil {
		t.Fatalf("status from persisted failure: %v", err)
	}
	if status.State != IngestStateFailed || status.RecoveryPending || status.Result == nil ||
		status.Result.Failed != 1 || status.Result.Applied != 0 {
		t.Fatalf("status from persisted failure = %#v", status)
	}
}

func TestIngestServiceRecoversLegacyWALAtInitialGeneration(t *testing.T) {
	store := &lifecycleFencingIngestStore{generation: 1}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	request := writeLegacyAcceptedWAL(t, config, "tenant-a", ingestEntityRequest("batch-legacy-generation-one", "host:legacy-one"))

	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatalf("accept recovered legacy WAL request: %v", err)
	}
	if got := service.pendingAcceptedGeneration(accepted.pending); got != legacyUnboundIngestGeneration {
		t.Fatalf("legacy WAL accepted generation = %d, want %d", got, legacyUnboundIngestGeneration)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush legacy WAL at generation 1: %v", err)
	}
	result, err := service.Wait(flushCtx, accepted)
	if err != nil {
		t.Fatalf("wait legacy WAL at generation 1: %v", err)
	}
	if result.Version != 1 || result.Applied != 1 || result.Failed != 0 {
		t.Fatalf("legacy WAL generation 1 result = %#v, want successful recovery", result)
	}
}

func TestIngestServiceFencesLegacyWALAfterInitialGeneration(t *testing.T) {
	store := &lifecycleFencingIngestStore{generation: 2}
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	request := writeLegacyAcceptedWAL(t, config, "tenant-a", ingestEntityRequest("batch-legacy-generation-two", "host:legacy-two"))

	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	accepted, err := service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatalf("accept recovered legacy WAL request: %v", err)
	}
	if got := service.pendingAcceptedGeneration(accepted.pending); got != legacyUnboundIngestGeneration {
		t.Fatalf("legacy WAL accepted generation = %d, want %d", got, legacyUnboundIngestGeneration)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush legacy WAL after generation change: %v", err)
	}
	result, err := service.Wait(flushCtx, accepted)
	if err != nil {
		t.Fatalf("wait fenced legacy WAL: %v", err)
	}
	if result.Version != 0 || result.Applied != 0 || result.Failed != 1 || len(result.Failures) != 1 ||
		!strings.Contains(result.Failures[0].Error, ErrTenantDeleted.Error()) {
		t.Fatalf("legacy WAL generation 2 result = %#v, want lifecycle-fenced failure", result)
	}
	status, err := service.Status(flushCtx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("status after legacy WAL fence: %v", err)
	}
	if status.State != IngestStateFailed || status.Result == nil || status.Result.Failed != 1 || status.Result.Applied != 0 {
		t.Fatalf("status after legacy WAL fence = %#v, want failed terminal state", status)
	}
}

func TestIngestServiceAcceptsAndRecoversWhenGenerationStoreIsUnavailable(t *testing.T) {
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	request := ingestEntityRequest("batch-generation-store-outage", "host:generation-store-outage")

	blockedStore := &blockingGenerationIngestStore{
		lifecycleFencingIngestStore: &lifecycleFencingIngestStore{},
		started:                     make(chan struct{}, 1),
		release:                     make(chan struct{}),
	}
	defer close(blockedStore.release)
	service, err := OpenIngestService(blockedStore, config)
	if err != nil {
		t.Fatalf("open ingest service with unavailable generation store: %v", err)
	}

	type acceptResult struct {
		acceptance IngestAcceptance
		err        error
	}
	started := time.Now()
	resultCh := make(chan acceptResult, 1)
	go func() {
		acceptance, err := service.Accept(context.Background(), "tenant-a", request)
		resultCh <- acceptResult{acceptance: acceptance, err: err}
	}()
	select {
	case <-blockedStore.started:
	case <-time.After(time.Second):
		closeIngestService(t, service)
		t.Fatal("generation capture did not start")
	}
	var accepted IngestAcceptance
	select {
	case result := <-resultCh:
		if result.err != nil {
			closeIngestService(t, service)
			t.Fatalf("accept while generation store is unavailable: %v", result.err)
		}
		accepted = result.acceptance
	case <-time.After(500 * time.Millisecond):
		closeIngestService(t, service)
		t.Fatal("accept remained blocked after generation capture timeout")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		closeIngestService(t, service)
		t.Fatalf("accept latency with unavailable generation store = %s, want <= 500ms", elapsed)
	}
	if accepted.Durability != "durable" || accepted.pending == nil {
		closeIngestService(t, service)
		t.Fatalf("acceptance after generation outage = %#v, want synchronously durable pending record", accepted)
	}

	crashIngestService(t, service)

	recoveredStore := &lifecycleFencingIngestStore{generation: 1}
	reopened, err := OpenIngestService(recoveredStore, config)
	if err != nil {
		t.Fatalf("reopen ingest service after generation outage: %v", err)
	}
	defer closeIngestService(t, reopened)

	recovered, err := reopened.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatalf("recover accepted WAL record after restart: %v", err)
	}
	if recovered.State != IngestStateAccepted || recovered.pending == nil {
		t.Fatalf("recovered acceptance = %#v, want pending accepted record", recovered)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reopened.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush recovered WAL record at initial generation: %v", err)
	}
	flushed, err := reopened.Wait(flushCtx, recovered)
	if err != nil {
		t.Fatalf("wait recovered WAL record: %v", err)
	}
	if flushed.Applied != 1 || flushed.Failed != 0 {
		t.Fatalf("recovered WAL result = %#v, want one applied mutation", flushed)
	}

}

func TestIngestServiceFencesUnavailableGenerationWALAfterGenerationChange(t *testing.T) {
	config := testIngestServiceConfig(t)
	config.OwnerID = "writer-a"
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	request := ingestEntityRequest("batch-generation-store-fenced", "host:generation-store-fenced")

	blockedStore := &blockingGenerationIngestStore{
		lifecycleFencingIngestStore: &lifecycleFencingIngestStore{},
		started:                     make(chan struct{}, 1),
		release:                     make(chan struct{}),
	}
	service, err := OpenIngestService(blockedStore, config)
	if err != nil {
		t.Fatalf("open ingest service with unavailable generation store: %v", err)
	}
	defer close(blockedStore.release)
	_, err = service.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		closeIngestService(t, service)
		t.Fatalf("accept while generation store is unavailable: %v", err)
	}
	crashIngestService(t, service)

	fencedStore := &lifecycleFencingIngestStore{generation: 2}
	reopened, err := OpenIngestService(fencedStore, config)
	if err != nil {
		t.Fatalf("reopen ingest service after generation change: %v", err)
	}
	defer closeIngestService(t, reopened)
	recovered, err := reopened.Accept(context.Background(), "tenant-a", request)
	if err != nil {
		t.Fatalf("recover accepted WAL record after generation change: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reopened.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush generation-fenced WAL record: %v", err)
	}
	result, err := reopened.Wait(flushCtx, recovered)
	if err != nil {
		t.Fatalf("wait generation-fenced WAL record: %v", err)
	}
	if result.Applied != 0 || result.Failed != 1 || len(result.Failures) != 1 ||
		!strings.Contains(result.Failures[0].Error, ErrTenantDeleted.Error()) {
		t.Fatalf("generation-fenced WAL result = %#v, want terminal failure without publish", result)
	}
}

func TestIngestServiceReplaysPersistedSuccessAfterTenantLifecycleChange(t *testing.T) {
	for _, lifecycleStatus := range []string{TenantStatusDisabled, TenantStatusDeleted} {
		lifecycleStatus := lifecycleStatus
		t.Run(lifecycleStatus, func(t *testing.T) {
			ctx := context.Background()
			store := NewTenantStore(NewMemoryStore(), "test")
			if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
				t.Fatalf("create tenant: %v", err)
			}
			config := testIngestServiceConfig(t)
			config.FlushInterval = time.Hour
			config.FlushMaxRequests = 1
			service, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIngestService(t, service)

			flushTenant := func() {
				flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				if err := service.FlushTenant(flushCtx, "tenant-a"); err != nil {
					t.Fatalf("flush tenant: %v", err)
				}
			}

			request := ingestEntityRequest("batch-lifecycle-replay", "host:original")
			first, err := service.Accept(ctx, "tenant-a", request)
			if err != nil {
				t.Fatalf("accept initial request: %v", err)
			}
			flushTenant()
			firstResult, err := service.Wait(ctx, first)
			if err != nil {
				t.Fatalf("wait initial request: %v", err)
			}
			if firstResult.Version != 1 || firstResult.Applied != 1 || firstResult.Failed != 0 {
				t.Fatalf("initial result = %#v, want one applied mutation at version 1", firstResult)
			}

			if _, err := store.SetTenantStatus(ctx, "tenant-a", lifecycleStatus); err != nil {
				t.Fatalf("set tenant status %q: %v", lifecycleStatus, err)
			}
			replay, err := service.Accept(ctx, "tenant-a", request)
			if err != nil {
				t.Fatalf("accept exact replay while tenant %s: %v", lifecycleStatus, err)
			}
			flushTenant()
			replayResult, err := service.Wait(ctx, replay)
			if err != nil {
				t.Fatalf("wait exact replay while tenant %s: %v", lifecycleStatus, err)
			}
			if replayResult.Version != firstResult.Version || replayResult.Applied != firstResult.Applied ||
				replayResult.Failed != 0 || !replayResult.Skipped ||
				replayResult.SkipReason != IngestSkipReasonIdempotentReplay {
				t.Fatalf("lifecycle replay result = %#v, want the persisted success result", replayResult)
			}

			if lifecycleStatus == TenantStatusDisabled {
				if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusActive); err != nil {
					t.Fatalf("reactivate disabled tenant: %v", err)
				}
			} else if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
				t.Fatalf("recreate deleted tenant: %v", err)
			}
			next, err := service.Accept(ctx, "tenant-a", ingestEntityRequest("batch-after-lifecycle-replay", "host:next"))
			if err != nil {
				t.Fatalf("accept request after lifecycle replay: %v", err)
			}
			flushTenant()
			nextResult, err := service.Wait(ctx, next)
			if err != nil {
				t.Fatalf("wait request after lifecycle replay: %v", err)
			}
			if nextResult.Version != 2 || nextResult.Applied != 1 || nextResult.Failed != 0 {
				t.Fatalf("next result = %#v, want version 2 with one applied mutation", nextResult)
			}
		})
	}
}

func TestIngestServicePreservesTenantFIFOWhenFirstAcceptLoggerBlocks(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	logger := &gatedIngestLogger{
		firstAcceptedStarted: make(chan struct{}),
		releaseFirstAccepted: make(chan struct{}),
	}
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	config.Logger = logger
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open ingest service: %v", err)
	}
	released := false
	defer func() {
		if !released {
			close(logger.releaseFirstAccepted)
		}
		closeIngestService(t, service)
	}()

	upsert := ingestEntityRequest("batch-wal-order-upsert", "host:wal-order")
	deleteRequest := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-wal-order-delete",
		FullSync:    true,
		Items: []IngestItem{{
			ExternalID:   "host:wal-order",
			DeleteEntity: &graph.EntityDeleteRequest{ID: "host:wal-order", Source: "agent"},
		}},
	}
	type acceptResult struct {
		acceptance IngestAcceptance
		err        error
	}
	firstDone := make(chan acceptResult, 1)
	go func() {
		acceptance, err := service.Accept(ctx, "tenant-a", upsert)
		firstDone <- acceptResult{acceptance: acceptance, err: err}
	}()
	select {
	case <-logger.firstAcceptedStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first accepted logger callback did not block")
	}

	secondDone := make(chan acceptResult, 1)
	go func() {
		acceptance, err := service.Accept(ctx, "tenant-a", deleteRequest)
		secondDone <- acceptResult{acceptance: acceptance, err: err}
	}()
	var second acceptResult
	select {
	case second = <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second Accept did not complete while first logger callback was blocked")
	}
	if second.err != nil {
		t.Fatalf("second Accept: %v", second.err)
	}

	close(logger.releaseFirstAccepted)
	released = true
	var first acceptResult
	select {
	case first = <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first Accept did not complete after logger release")
	}
	if first.err != nil {
		t.Fatalf("first Accept: %v", first.err)
	}
	if first.acceptance.acceptedLSN >= second.acceptance.acceptedLSN {
		t.Fatalf("accepted LSNs = %d, %d; want upsert before delete", first.acceptance.acceptedLSN, second.acceptance.acceptedLSN)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	firstResult, err := service.Wait(waitCtx, first.acceptance)
	if err != nil {
		t.Fatalf("wait first WAL-order request: %v", err)
	}
	secondResult, err := service.Wait(waitCtx, second.acceptance)
	if err != nil {
		t.Fatalf("wait second WAL-order request: %v", err)
	}
	if firstResult.Version != 1 || secondResult.Version != 2 {
		t.Fatalf("versions = %d, %d; want 1, 2 in accepted LSN order", firstResult.Version, secondResult.Version)
	}
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load final graph: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("final graph version = %d, want 2", manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:wal-order"); ok {
		t.Fatal("final graph contains entity after upsert then delete; accepted WAL order was not preserved")
	}
}

func TestIngestServicePruneRetainsAcceptedWALBeforeActiveRegistration(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	observer := &blockingFirstIngestWALAppendObserver{
		firstAcceptedStarted: make(chan struct{}),
		releaseFirstAccepted: make(chan struct{}),
	}
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 256
	config.WAL.SegmentBytes = 4096
	config.Observer = observer
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open ingest service: %v", err)
	}
	released := false
	crashed := false
	defer func() {
		if !released {
			close(observer.releaseFirstAccepted)
		}
		if !crashed {
			closeIngestService(t, service)
		}
	}()

	firstRequest := ingestWALSegmentRequest("batch-prune-window-first", "host:prune-window-first")
	secondRequest := ingestWALSegmentRequest("batch-prune-window-second", "host:prune-window-second")
	type acceptResult struct {
		acceptance IngestAcceptance
		err        error
	}
	firstDone := make(chan acceptResult, 1)
	go func() {
		acceptance, err := service.Accept(ctx, "tenant-a", firstRequest)
		firstDone <- acceptResult{acceptance: acceptance, err: err}
	}()
	select {
	case <-observer.firstAcceptedStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first accepted WAL observer callback did not block")
	}

	secondDone := make(chan acceptResult, 1)
	go func() {
		acceptance, err := service.Accept(ctx, "tenant-a", secondRequest)
		secondDone <- acceptResult{acceptance: acceptance, err: err}
	}()
	var second acceptResult
	select {
	case second = <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second Accept did not complete while first append observer was blocked")
	}
	if second.err != nil {
		t.Fatalf("second Accept: %v", second.err)
	}

	records, segments, _, _, err := recoverIngestWAL(config.WAL.Dir)
	if err != nil {
		t.Fatalf("inspect WAL before prune: %v", err)
	}
	if len(records) != 2 || len(segments) < 2 || records[0].Type != IngestWALAccepted || records[1].Type != IngestWALAccepted {
		t.Fatalf("WAL before prune records=%#v segments=%#v; want two accepted records in separate segments", records, segments)
	}
	firstLSN := records[0].LSN
	if records[1].LSN != second.acceptance.acceptedLSN || firstLSN >= records[1].LSN {
		t.Fatalf("WAL/accept LSNs = %d, %d, %d; want increasing first and second accepted records", firstLSN, records[1].LSN, second.acceptance.acceptedLSN)
	}

	if err := service.prune(ctx); err != nil {
		t.Fatalf("prune while first Accept is returning from WAL append: %v", err)
	}
	records, _, _, _, err = recoverIngestWAL(config.WAL.Dir)
	if err != nil {
		t.Fatalf("inspect WAL after prune: %v", err)
	}
	if len(records) == 0 || records[0].LSN != firstLSN {
		t.Fatalf("WAL after prune records=%#v; accepted LSN %d was pruned before active registration", records, firstLSN)
	}

	close(observer.releaseFirstAccepted)
	released = true
	var first acceptResult
	select {
	case first = <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first Accept did not complete after observer release")
	}
	if first.err != nil {
		t.Fatalf("first Accept: %v", first.err)
	}
	crashIngestService(t, service)
	crashed = true

	recoveryConfig := config
	// Recovery may combine both accepted records into one prepared frame, so use
	// a larger segment after preserving the initial 4 KiB prune window.
	recoveryConfig.WAL.SegmentBytes = 16 * 1024
	reopened, err := OpenIngestService(store, recoveryConfig)
	if err != nil {
		t.Fatalf("reopen ingest service after crash: %v", err)
	}
	defer closeIngestService(t, reopened)
	recovered, err := reopened.Accept(ctx, "tenant-a", firstRequest)
	if err != nil {
		t.Fatalf("accept recovered first WAL record: %v", err)
	}
	if recovered.acceptedLSN != firstLSN {
		t.Fatalf("recovered first accepted LSN = %d, want original LSN %d", recovered.acceptedLSN, firstLSN)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := reopened.FlushTenant(waitCtx, "tenant-a"); err != nil {
		t.Fatalf("flush recovered WAL records: %v", err)
	}
	result, err := reopened.Wait(waitCtx, recovered)
	if err != nil {
		t.Fatalf("wait recovered first WAL record: %v", err)
	}
	if result.Applied != 1 || result.Failed != 0 {
		t.Fatalf("recovered first WAL result = %#v; want one applied mutation", result)
	}
}

func TestIngestServiceCloseDrainsQueuedTenantAfterBusyFlush(t *testing.T) {
	ctx := context.Background()
	store := &blockingIngestDurableBatchStore{
		TenantStore:  NewTenantStore(NewMemoryStore(), "test"),
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	logger := &gatedIngestLogger{shutdownStarted: make(chan struct{})}
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	config.FlushWorkers = 1
	config.WAL.AppendQueue = 1
	config.Logger = logger
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open ingest service: %v", err)
	}
	released := false
	defer func() {
		if !released {
			close(store.releaseFirst)
		}
	}()

	firstRequest := ingestEntityRequest("batch-close-busy-first", "host:close-busy-first")
	firstRequest.FullSync = true
	first, err := service.Accept(ctx, "tenant-a", firstRequest)
	if err != nil {
		t.Fatalf("accept first busy-flush request: %v", err)
	}
	select {
	case <-store.firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first flush did not reach blocking store")
	}
	secondRequest := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-close-busy-second",
		FullSync:    true,
		Items: []IngestItem{{
			ExternalID:   "host:close-busy-first",
			DeleteEntity: &graph.EntityDeleteRequest{ID: "host:close-busy-first", Source: "agent"},
		}},
	}
	second, err := service.Accept(ctx, "tenant-a", secondRequest)
	if err != nil {
		t.Fatalf("accept second busy-flush request: %v", err)
	}
	// A third accepted request makes the second enqueue send wait until the
	// scheduler has received the second item when the queue buffer is full.
	probeRequest := ingestEntityRequest("batch-close-busy-probe", "host:close-busy-probe")
	probeRequest.FullSync = true
	probe, err := service.Accept(ctx, "tenant-a", probeRequest)
	if err != nil {
		t.Fatalf("accept drain probe request: %v", err)
	}

	closeCtx, cancelClose := context.WithTimeout(ctx, 5*time.Second)
	defer cancelClose()
	closeDone := make(chan error, 1)
	go func() { closeDone <- service.Close(closeCtx) }()
	select {
	case <-logger.shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not start")
	}
	select {
	case <-service.shutdownCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not enter shutdown drain")
	}
	close(store.releaseFirst)
	released = true
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close ingest service while tenant flush was busy: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not drain the queued same-tenant flush after the busy flush completed")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	firstResult, err := service.Wait(waitCtx, first)
	if err != nil {
		t.Fatalf("wait first request after close: %v", err)
	}
	secondResult, err := service.Wait(waitCtx, second)
	if err != nil {
		t.Fatalf("wait second request after close: %v", err)
	}
	probeResult, err := service.Wait(waitCtx, probe)
	if err != nil {
		t.Fatalf("wait drain probe request after close: %v", err)
	}
	if firstResult.Version != 1 || secondResult.Version != 2 || probeResult.Version != 3 {
		t.Fatalf("closed-service versions = %d, %d, %d; want 1, 2, 3", firstResult.Version, secondResult.Version, probeResult.Version)
	}
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after close drain: %v", err)
	}
	if manifest.Version != 3 {
		t.Fatalf("graph version after close drain = %d, want 3", manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:close-busy-first"); ok {
		t.Fatal("queued same-tenant delete was not drained after the busy flush")
	}
	if _, ok := loaded.GetEntity("host:close-busy-probe"); !ok {
		t.Fatal("drain probe request was not completed before Close returned")
	}
}

func TestIngestServiceCloseDrainsManyTenantsWithoutChannelDeadlock(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	tenantIDs := []string{
		"tenant-a", "tenant-b", "tenant-c", "tenant-d", "tenant-e", "tenant-f",
		"tenant-g", "tenant-h", "tenant-i", "tenant-j", "tenant-k", "tenant-l",
	}
	for _, tenantID := range tenantIDs {
		if _, err := store.CreateTenant(ctx, tenantID, TenantCreateOptions{}); err != nil {
			t.Fatalf("create tenant %s: %v", tenantID, err)
		}
	}
	config := testIngestServiceConfig(t)
	config.FlushInterval = time.Hour
	config.FlushWorkers = 1
	config.WAL.AppendQueue = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open ingest service: %v", err)
	}
	defer closeIngestService(t, service)

	acceptances := make([]IngestAcceptance, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		acceptance, err := service.Accept(ctx, tenantID, ingestEntityRequest("batch-drain-"+tenantID, "host:drain-"+tenantID))
		if err != nil {
			t.Fatalf("accept request for %s: %v", tenantID, err)
		}
		acceptances = append(acceptances, acceptance)
	}

	closeCtx, cancelClose := context.WithTimeout(ctx, 5*time.Second)
	defer cancelClose()
	if err := service.Close(closeCtx); err != nil {
		t.Fatalf("close service with many tenant queues: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	for index, acceptance := range acceptances {
		result, err := service.Wait(waitCtx, acceptance)
		if err != nil {
			t.Fatalf("wait tenant %s after close: %v", tenantIDs[index], err)
		}
		if result.Applied != 1 || result.Failed != 0 || result.Version != 1 {
			t.Fatalf("tenant %s result after close = %#v; want one applied mutation at version 1", tenantIDs[index], result)
		}
	}
}

type gatedIngestLogger struct {
	firstAcceptedStarted chan struct{}
	releaseFirstAccepted chan struct{}
	acceptedOnce         sync.Once
	shutdownStarted      chan struct{}
	shutdownOnce         sync.Once
}

func (l *gatedIngestLogger) Info(event string, _ map[string]any) {
	switch event {
	case "ingest_wal_accepted":
		if l.firstAcceptedStarted == nil || l.releaseFirstAccepted == nil {
			return
		}
		first := false
		l.acceptedOnce.Do(func() {
			first = true
			close(l.firstAcceptedStarted)
		})
		if first {
			<-l.releaseFirstAccepted
		}
	case "ingest_wal_shutdown_started":
		if l.shutdownStarted != nil {
			l.shutdownOnce.Do(func() { close(l.shutdownStarted) })
		}
	}
}

func (l *gatedIngestLogger) Error(string, map[string]any) {}

type blockingFirstIngestWALAppendObserver struct {
	firstAcceptedStarted chan struct{}
	releaseFirstAccepted chan struct{}
	acceptedOnce         sync.Once
}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestWALAppend(recordType string, _ string, _ int, _ time.Duration) {
	if recordType != "accepted" || o.firstAcceptedStarted == nil || o.releaseFirstAccepted == nil {
		return
	}
	first := false
	o.acceptedOnce.Do(func() {
		first = true
		close(o.firstAcceptedStarted)
	})
	if first {
		<-o.releaseFirstAccepted
	}
}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestWALSync(string, int, int, time.Duration) {
}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestWALState(int, int64, uint64, uint64) {
}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestQueue(int, int64, time.Duration) {}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestQueueCache(string) {}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestFlush(string, time.Duration, int, int, int, int, int, bool) {
}

func (o *blockingFirstIngestWALAppendObserver) RecordIngestRecovery(string, int, int, int, time.Duration) {
}

func ingestWALSegmentRequest(batchID string, entityID string) IngestRequest {
	request := ingestEntityRequest(batchID, entityID)
	request.Items[0].Entity.Fields["padding"] = strings.Repeat("x", 3000)
	return request
}

type blockingIngestDurableBatchStore struct {
	*TenantStore
	firstStarted chan struct{}
	releaseFirst chan struct{}
	firstOnce    sync.Once
}

func (s *blockingIngestDurableBatchStore) IngestDurableBatchWithHooks(
	ctx context.Context,
	tenantID string,
	entries []IngestBatchEntry,
	hooks IngestBatchHooks,
) ([]IngestResult, error) {
	first := false
	s.firstOnce.Do(func() {
		first = true
		close(s.firstStarted)
	})
	if first {
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.TenantStore.IngestDurableBatchWithHooks(ctx, tenantID, entries, hooks)
}

type lifecycleFencingIngestStore struct {
	IngestStore
	err        error
	record     *IngestBatchRecord
	generation int64
}

type blockingGenerationIngestStore struct {
	*lifecycleFencingIngestStore
	started chan struct{}
	release chan struct{}
}

func (s *blockingGenerationIngestStore) CaptureIngestWALGeneration(ctx context.Context, tenantID string) (int64, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return s.lifecycleFencingIngestStore.CaptureIngestWALGeneration(ctx, tenantID)
}

func (s *lifecycleFencingIngestStore) CoordinationBackend() string {
	return CoordinationPostgres
}

func (s *lifecycleFencingIngestStore) SetIngestBarrier(func(context.Context, string) error) {}

func (s *lifecycleFencingIngestStore) CaptureIngestWALGeneration(context.Context, string) (int64, error) {
	if s.generation > 0 {
		return s.generation, nil
	}
	return 1, nil
}

func (s *lifecycleFencingIngestStore) GetIngestBatch(context.Context, string, string, string, string) (IngestBatchRecord, error) {
	if s.record != nil {
		return *s.record, nil
	}
	return IngestBatchRecord{}, ErrNotFound
}

func (s *lifecycleFencingIngestStore) PersistIngestFailure(
	_ context.Context,
	tenantID string,
	request IngestRequest,
	result IngestResult,
	started time.Time,
	finished time.Time,
) error {
	s.record = &IngestBatchRecord{
		TenantID: tenantID, Request: request, Result: result,
		StartedAt: started, FinishedAt: finished,
	}
	return nil
}

func (s *lifecycleFencingIngestStore) IngestDurableBatchWithHooks(
	_ context.Context,
	_ string,
	entries []IngestBatchEntry,
	_ IngestBatchHooks,
) ([]IngestResult, error) {
	if s.err != nil {
		return nil, s.err
	}
	results := make([]IngestResult, len(entries))
	for index, entry := range entries {
		if s.generation > 1 && entry.AcceptedGeneration == legacyUnboundIngestGeneration {
			return nil, ErrTenantDeleted
		}
		results[index] = IngestResult{
			BatchID: entry.Request.BatchID,
			Version: 1,
			Applied: len(entry.Request.Items),
		}
	}
	return results, nil
}

type generationCacheIngestStore struct {
	*lifecycleFencingIngestStore
	mu         sync.Mutex
	calls      int
	generation int64
	started    chan struct{}
	release    chan struct{}
	block      bool
}

func newGenerationCacheIngestStore(generation int64) *generationCacheIngestStore {
	return &generationCacheIngestStore{
		lifecycleFencingIngestStore: &lifecycleFencingIngestStore{},
		generation:                  generation,
		started:                     make(chan struct{}, 1),
		release:                     make(chan struct{}),
	}
}

func (s *generationCacheIngestStore) CaptureIngestWALGeneration(ctx context.Context, _ string) (int64, error) {
	s.mu.Lock()
	s.calls++
	generation := s.generation
	block := s.block
	s.mu.Unlock()
	select {
	case s.started <- struct{}{}:
	default:
	}
	if block {
		select {
		case <-s.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return generation, nil
}

func (s *generationCacheIngestStore) setGeneration(generation int64) {
	s.mu.Lock()
	s.generation = generation
	s.mu.Unlock()
}

func (s *generationCacheIngestStore) setBlock(block bool) {
	s.mu.Lock()
	s.block = block
	s.mu.Unlock()
}

func (s *generationCacheIngestStore) captureCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newGenerationCacheTestService(store *generationCacheIngestStore) *IngestService {
	return &IngestService{
		store:          store,
		runCtx:         context.Background(),
		generations:    map[string]ingestGenerationCacheEntry{},
		generationLoad: map[string]*ingestGenerationFlight{},
	}
}

func writeLegacyAcceptedWAL(
	t *testing.T,
	config IngestServiceConfig,
	tenantID string,
	request IngestRequest,
) IngestRequest {
	t.Helper()
	var err error
	request, err = PrepareIngestRequest(tenantID, request)
	if err != nil {
		t.Fatalf("prepare legacy WAL request: %v", err)
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal legacy WAL request: %v", err)
	}
	envelope := walIngestEnvelope{
		RecordID:   ingestRecordID(ingestRequestIdentity(tenantID, request)),
		WriterID:   config.OwnerID,
		TenantID:   tenantID,
		Request:    request,
		Digest:     sha256Sum(requestJSON),
		AcceptedAt: time.Now().UTC(),
		State:      IngestStateAccepted,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal legacy WAL envelope: %v", err)
	}
	wal, _, err := OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatalf("open legacy WAL: %v", err)
	}
	if _, err := wal.Append(context.Background(), IngestWALAccepted, payload); err != nil {
		_ = wal.Close()
		t.Fatalf("append legacy WAL record: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close legacy WAL: %v", err)
	}
	return request
}

var _ IngestStore = (*lifecycleFencingIngestStore)(nil)
