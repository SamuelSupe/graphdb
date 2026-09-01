package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	config.FlushMaxRequests = 1
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
