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

func TestPostgresCoordinatorReserveCommitBatchSemantics(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-reserve-semantics")

	initial := []CommitReservationRequest{
		{Key: "batch/fresh", RequestHash: "hash-fresh", OwnerToken: "owner-a"},
		{Key: "batch/expired", RequestHash: "hash-expired", OwnerToken: "owner-a"},
		{Key: "batch/committed", RequestHash: "hash-committed", OwnerToken: "owner-a"},
	}
	outcomes, err := coordinator.ReserveCommitBatch(ctx, "tenant-a", initial, time.Minute)
	if err != nil {
		t.Fatalf("initial batch reservation: %v", err)
	}
	if len(outcomes) != len(initial) {
		t.Fatalf("initial outcomes = %d, want %d", len(outcomes), len(initial))
	}
	for index, outcome := range outcomes {
		if outcome.Err != nil {
			t.Fatalf("initial outcome %d: %v", index, outcome.Err)
		}
		if outcome.Reservation.Key != initial[index].Key ||
			outcome.Reservation.RequestHash != initial[index].RequestHash ||
			outcome.Reservation.OwnerToken != initial[index].OwnerToken ||
			outcome.Reservation.Committed {
			t.Fatalf("initial outcome %d = %#v, want fresh pending reservation", index, outcome)
		}
	}

	assertReserve := func(t *testing.T, request CommitReservationRequest) CommitReservationOutcome {
		t.Helper()
		got, err := coordinator.ReserveCommitBatch(ctx, "tenant-a", []CommitReservationRequest{request}, time.Minute)
		if err != nil {
			t.Fatalf("reserve %q: %v", request.Key, err)
		}
		if len(got) != 1 {
			t.Fatalf("reserve %q outcomes = %d, want 1", request.Key, len(got))
		}
		return got[0]
	}

	sameOwner := assertReserve(t, CommitReservationRequest{
		Key: "batch/fresh", RequestHash: "hash-fresh", OwnerToken: "owner-a",
	})
	if sameOwner.Err != nil || sameOwner.Reservation.OwnerToken != "owner-a" || sameOwner.Reservation.Committed {
		t.Fatalf("same-owner pending reservation = %#v, want renewed pending reservation", sameOwner)
	}

	otherOwner := assertReserve(t, CommitReservationRequest{
		Key: "batch/fresh", RequestHash: "hash-fresh", OwnerToken: "owner-b",
	})
	if !errors.Is(otherOwner.Err, ErrIdempotencyInProgress) {
		t.Fatalf("active other-owner reservation err = %v, want ErrIdempotencyInProgress", otherOwner.Err)
	}
	if otherOwner.Reservation.OwnerToken != "owner-a" {
		t.Fatalf("active other-owner reservation owner = %q, want owner-a", otherOwner.Reservation.OwnerToken)
	}

	hashConflict := assertReserve(t, CommitReservationRequest{
		Key: "batch/fresh", RequestHash: "hash-different", OwnerToken: "owner-a",
	})
	if !errors.Is(hashConflict.Err, ErrIdempotencyConflict) {
		t.Fatalf("request-hash conflict err = %v, want ErrIdempotencyConflict", hashConflict.Err)
	}

	if _, err := coordinator.pool.Exec(ctx,
		`UPDATE `+coordinator.table("commit_idempotency")+`
		 SET updated_at = now() - interval '2 minutes'
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3`,
		coordinator.namespace, "tenant-a", "batch/expired",
	); err != nil {
		t.Fatalf("expire pending reservation: %v", err)
	}
	takeover := assertReserve(t, CommitReservationRequest{
		Key: "batch/expired", RequestHash: "hash-expired", OwnerToken: "owner-b",
	})
	if takeover.Err != nil || takeover.Reservation.OwnerToken != "owner-b" || takeover.Reservation.Committed {
		t.Fatalf("expired takeover = %#v, want owner-b pending reservation", takeover)
	}

	resultJSON, err := json.Marshal(IngestResult{BatchID: "committed", Version: 4, Applied: 1})
	if err != nil {
		t.Fatalf("marshal committed result: %v", err)
	}
	if _, err := coordinator.pool.Exec(ctx,
		`UPDATE `+coordinator.table("commit_idempotency")+`
		 SET status = 'committed', result_json = $4::jsonb, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3`,
		coordinator.namespace, "tenant-a", "batch/committed", resultJSON,
	); err != nil {
		t.Fatalf("mark reservation committed: %v", err)
	}
	committedReplay := assertReserve(t, CommitReservationRequest{
		Key: "batch/committed", RequestHash: "hash-committed", OwnerToken: "owner-b",
	})
	var replayResult IngestResult
	if err := json.Unmarshal(committedReplay.Reservation.Result, &replayResult); err != nil {
		t.Fatalf("decode committed replay result: %v", err)
	}
	if committedReplay.Err != nil || !committedReplay.Reservation.Committed ||
		committedReplay.Reservation.OwnerToken != "owner-a" ||
		replayResult.BatchID != "committed" || replayResult.Version != 4 || replayResult.Applied != 1 {
		t.Fatalf("committed replay = %#v, want original committed result", committedReplay)
	}
}

func TestPostgresCoordinatorReserveCommitBatchConcurrentOwnersHaveSingleWinner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-reserve-concurrent")
	requests := []CommitReservationRequest{
		{Key: "batch/concurrent", RequestHash: "hash-concurrent", OwnerToken: "owner-a"},
		{Key: "batch/concurrent", RequestHash: "hash-concurrent", OwnerToken: "owner-b"},
	}
	type result struct {
		owner    string
		outcomes []CommitReservationOutcome
		err      error
	}
	results := make(chan result, len(requests))
	var wait sync.WaitGroup
	wait.Add(len(requests))
	for _, request := range requests {
		request := request
		go func() {
			defer wait.Done()
			outcomes, err := coordinator.ReserveCommitBatch(ctx, "tenant-a", []CommitReservationRequest{request}, time.Minute)
			results <- result{owner: request.OwnerToken, outcomes: outcomes, err: err}
		}()
	}
	wait.Wait()
	close(results)

	winners := 0
	losers := 0
	winnerOwner := ""
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent reservation owner %s: %v", got.owner, got.err)
		}
		if len(got.outcomes) != 1 {
			t.Fatalf("concurrent reservation owner %s outcomes = %#v, want one outcome", got.owner, got.outcomes)
		}
		outcome := got.outcomes[0]
		switch {
		case outcome.Err == nil:
			winners++
			winnerOwner = outcome.Reservation.OwnerToken
		case errors.Is(outcome.Err, ErrIdempotencyInProgress):
			losers++
		default:
			t.Fatalf("concurrent reservation owner %s outcome = %#v, want winner or ErrIdempotencyInProgress", got.owner, outcome)
		}
	}
	if winners != 1 || losers != 1 || winnerOwner == "" {
		t.Fatalf("concurrent reservation winners=%d losers=%d winner=%q, want one each", winners, losers, winnerOwner)
	}
	reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", "batch/concurrent")
	if err != nil {
		t.Fatalf("load concurrent reservation: %v", err)
	}
	if reservation.OwnerToken != winnerOwner || reservation.Committed {
		t.Fatalf("concurrent reservation = %#v, want pending owner %q", reservation, winnerOwner)
	}
}

func TestPostgresCoordinatorAbortCommitBatchOnlyDeletesMatchingPendingReservations(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-abort")
	requests := []CommitReservationRequest{
		{Key: "batch/delete", RequestHash: "hash-delete", OwnerToken: "owner-a"},
		{Key: "batch/wrong-owner", RequestHash: "hash-owner", OwnerToken: "owner-a"},
		{Key: "batch/wrong-hash", RequestHash: "hash-original", OwnerToken: "owner-a"},
		{Key: "batch/committed", RequestHash: "hash-committed", OwnerToken: "owner-a"},
	}
	if _, err := coordinator.ReserveCommitBatch(ctx, "tenant-a", requests, time.Minute); err != nil {
		t.Fatalf("reserve abort batch: %v", err)
	}
	if _, err := coordinator.pool.Exec(ctx,
		`UPDATE `+coordinator.table("commit_idempotency")+`
		 SET status = 'committed', result_json = '{}'::jsonb, updated_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND idempotency_key = $3`,
		coordinator.namespace, "tenant-a", "batch/committed",
	); err != nil {
		t.Fatalf("mark committed reservation: %v", err)
	}

	if err := coordinator.AbortCommitBatch(ctx, "tenant-a", []CommitReservationRequest{
		requests[0],
		{Key: requests[1].Key, RequestHash: requests[1].RequestHash, OwnerToken: "owner-b"},
		{Key: requests[2].Key, RequestHash: "hash-different", OwnerToken: requests[2].OwnerToken},
		requests[3],
	}); err != nil {
		t.Fatalf("abort reservation batch: %v", err)
	}

	if _, err := coordinator.loadCommitReservation(ctx, "tenant-a", requests[0].Key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("matching pending reservation after abort err = %v, want ErrNotFound", err)
	}
	for _, request := range requests[1:3] {
		reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", request.Key)
		if err != nil {
			t.Fatalf("load unmatched reservation %q: %v", request.Key, err)
		}
		if reservation.Committed || reservation.OwnerToken != request.OwnerToken {
			t.Fatalf("unmatched reservation %q = %#v, want original pending owner", request.Key, reservation)
		}
	}
	committed, err := coordinator.loadCommitReservation(ctx, "tenant-a", requests[3].Key)
	if err != nil {
		t.Fatalf("load committed reservation: %v", err)
	}
	if !committed.Committed {
		t.Fatalf("committed reservation after abort = %#v, want retained", committed)
	}
}

func TestPostgresCoordinatorPublishIngestBatchDeduplicatesSameCollectorAtomically(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-collector-dedupe")
	seedCoordinatorHead(t, ctx, coordinator, "tenant-a")
	reservations := []CommitReservationRequest{
		{Key: "batch/item-1", RequestHash: "hash-1", OwnerToken: "owner-a"},
		{Key: "batch/item-2", RequestHash: "hash-2", OwnerToken: "owner-a"},
		{Key: "batch/item-3", RequestHash: "hash-3", OwnerToken: "owner-a"},
	}
	if _, err := coordinator.ReserveCommitBatch(ctx, "tenant-a", reservations, time.Minute); err != nil {
		t.Fatalf("reserve completion batch: %v", err)
	}
	items := make([]IngestBatchCompletion, 0, len(reservations))
	batchIDs := []string{"item-1", "item-3", "item-2"}
	versions := []int64{1, 3, 2}
	for index, reservation := range reservations {
		result, err := json.Marshal(IngestResult{
			BatchID: batchIDs[index], Version: versions[index], Applied: 1,
		})
		if err != nil {
			t.Fatalf("marshal result %d: %v", index, err)
		}
		items = append(items, IngestBatchCompletion{
			IdempotencyKey: reservation.Key,
			RequestHash:    reservation.RequestHash,
			OwnerToken:     reservation.OwnerToken,
			Result:         result,
			CollectorState: &CollectorStateUpdate{
				Source: "agent", CollectorID: "collector-shared",
				BatchID: batchIDs[index], Cursor: "cursor-" + batchIDs[index],
				Version: versions[index],
			},
		})
	}

	updated, published, err := coordinator.PublishIngestBatch(ctx, IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     "tenant-a",
			ExpectedRevision:             1,
			ExpectedGeneration:           1,
			ExpectedWriteContextRevision: 0,
			GraphVersion:                 3,
			ManifestKey:                  "manifests/3.json",
			ManifestHash:                 "manifest-hash-3",
			CommitID:                     "commit-3",
		},
		Items: items,
	})
	if err != nil || !published {
		t.Fatalf("publish collector-dedup batch published=%v err=%v", published, err)
	}
	if updated.Revision != 2 || updated.GraphVersion != 3 {
		t.Fatalf("updated head = %#v, want revision 2 graph version 3", updated)
	}
	for _, reservation := range reservations {
		stored, err := coordinator.loadCommitReservation(ctx, "tenant-a", reservation.Key)
		if err != nil {
			t.Fatalf("load completed reservation %q: %v", reservation.Key, err)
		}
		if !stored.Committed || stored.OwnerToken != reservation.OwnerToken {
			t.Fatalf("completed reservation %q = %#v, want committed", reservation.Key, stored)
		}
	}
	collector, exists, err := coordinator.CollectorState(ctx, "tenant-a", "agent", "collector-shared")
	if err != nil || !exists {
		t.Fatalf("collector state exists=%v err=%v", exists, err)
	}
	if collector.Version != 3 || collector.BatchID != "item-3" || collector.Cursor != "cursor-item-3" {
		t.Fatalf("collector state = %#v, want highest version item-3", collector)
	}
	assertCoordinatorRows(t, coordinator,
		`SELECT count(*) FROM `+coordinator.table("legacy_manifest_outbox")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND head_revision = 2`,
		1,
	)
}

func TestPostgresCoordinatorDuplicateIngestIdempotencyKeyIsItemLevelConflictWithinOneFlush(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-duplicate-key")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.InstanceID = "duplicate-key-writer"
	store.SetCoordinator(coordinator)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}
	before, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head before duplicate-key flush exists=%v err=%v", exists, err)
	}
	entries := []IngestBatchEntry{
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-duplicate", BatchID: "batch-1", IdempotencyKey: "same-key",
			Items: []IngestItem{{ExternalID: "host-1", Entity: &graph.Entity{ID: "host:duplicate-1", Kind: "host"}}},
		}},
		{Request: IngestRequest{
			Source: "agent", CollectorID: "collector-duplicate", BatchID: "batch-2", IdempotencyKey: "same-key",
			Items: []IngestItem{{ExternalID: "host-2", Entity: &graph.Entity{ID: "host:duplicate-2", Kind: "host"}}},
		}},
	}
	results, err := store.IngestDurableBatch(ctx, "tenant-a", entries)
	if err != nil {
		t.Fatalf("duplicate idempotency key flush err = %v, want item-level conflict", err)
	}
	if len(results) != len(entries) {
		t.Fatalf("duplicate-key results = %#v, want %d results", results, len(entries))
	}
	if results[0].Version != 1 || results[0].Applied != 1 || results[0].Failed != 0 {
		t.Fatalf("first duplicate-key result = %#v, want committed version 1", results[0])
	}
	if results[1].Version != 0 || results[1].Applied != 0 || results[1].Failed != 1 ||
		len(results[1].Failures) != 1 || !strings.Contains(results[1].Failures[0].Error, "idempotency conflict") {
		t.Fatalf("second duplicate-key result = %#v, want item-level idempotency conflict", results[1])
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after duplicate-key failure exists=%v err=%v", exists, err)
	}
	if head.Revision != before.Revision+1 || head.GraphVersion != before.GraphVersion+1 {
		t.Fatalf("head after duplicate-key conflict = %#v, want one committed item after %#v", head, before)
	}
	reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", "ingest/agent/collector-duplicate/same-key")
	if err != nil {
		t.Fatalf("load duplicate-key reservation: %v", err)
	}
	if !reservation.Committed {
		t.Fatalf("duplicate-key reservation = %#v, want committed first item", reservation)
	}
}

func TestPostgresCoordinatorDuplicateSamePayloadDoesNotPublishTwoVersions(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-duplicate-same-payload")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.InstanceID = "duplicate-payload-writer"
	store.SetCoordinator(coordinator)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}
	before, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head before duplicate-payload flush exists=%v err=%v", exists, err)
	}
	request := IngestRequest{
		Source: "agent", CollectorID: "collector-duplicate", BatchID: "same-batch", IdempotencyKey: "same-key",
		Items: []IngestItem{{ExternalID: "host-same", Entity: &graph.Entity{ID: "host:same", Kind: "host"}}},
	}
	_, err = store.IngestDurableBatch(ctx, "tenant-a", []IngestBatchEntry{
		{Request: request}, {Request: request},
	})
	if !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("duplicate same-payload flush err = %v, want safe batch rollback", err)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after duplicate-payload failure exists=%v err=%v", exists, err)
	}
	if head != before {
		t.Fatalf("head after duplicate-payload failure = %#v, want unchanged head %#v", head, before)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after duplicate-payload failure: %v", err)
	}
	if _, ok := loaded.GetEntity("host:same"); ok {
		t.Fatal("duplicate same-payload flush published an entity despite completion rollback")
	}
	reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", "ingest/agent/collector-duplicate/same-key")
	if err != nil {
		t.Fatalf("load duplicate-payload reservation: %v", err)
	}
	if reservation.Committed {
		t.Fatalf("duplicate-payload reservation = %#v, want pending after rolled-back completion", reservation)
	}
}

func TestPostgresIngestRecoveryReplaysCommittedBatchWithoutPreparedWAL(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-no-prepared-recovery")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "pg-recovery-writer"
	store.CoordinatorRetryLimit = 16
	store.SetCoordinator(coordinator)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create coordinated tenant: %v", err)
	}

	config := testIngestServiceConfig(t)
	config.OwnerID = store.InstanceID
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 1
	fault := &ingestWALFaultFile{failAfterWrite: 1}
	config.WAL.openWriterFile = ingestWALFaultOpener(fault)
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("open fault-injected ingest service: %v", err)
	}
	serviceStopped := false
	t.Cleanup(func() {
		if !serviceStopped {
			_ = service.Close(context.Background())
		}
	})
	request := ingestEntityRequest("batch-pg-no-prepared", "host:pg-no-prepared")
	accepted, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept PG WAL request: %v", err)
	}
	service.forceCh <- ingestForceRequest{tenantID: "tenant-a", throughLSN: accepted.acceptedLSN}

	deadline := time.Now().Add(10 * time.Second)
	for {
		status, statusErr := service.Status(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID)
		if statusErr == nil && status.State == IngestStateRetrying {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PG request did not reach terminal-WAL retry window: status=%#v err=%v", status, statusErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	crashIngestServiceAllowWALFailure(t, service)
	serviceStopped = true

	recoveryWALConfig := config.WAL
	recoveryWALConfig.openWriterFile = nil
	wal, records, err := OpenIngestWAL(recoveryWALConfig)
	if err != nil {
		t.Fatalf("open PG WAL after simulated crash: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close inspected PG WAL: %v", err)
	}
	if len(records) != 1 || records[0].Type != IngestWALAccepted {
		t.Fatalf("PG WAL after publish-before-terminal crash = %#v, want only one accepted record", records)
	}
	for _, record := range records {
		if record.Type == IngestWALPrepared {
			t.Fatal("PG WAL persisted PREPARED record despite PostgreSQL coordination")
		}
	}

	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after simulated crash exists=%v err=%v", exists, err)
	}
	if head.GraphVersion != 1 {
		t.Fatalf("head after simulated crash = %#v, want committed graph version 1", head)
	}
	reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", coordinatedIngestIdempotencyKey(request))
	if err != nil {
		t.Fatalf("load committed reservation after simulated crash: %v", err)
	}
	if !reservation.Committed {
		t.Fatalf("reservation after simulated crash = %#v, want committed", reservation)
	}

	recoveryConfig := config
	recoveryConfig.WAL.openWriterFile = nil
	recoveryConfig.FlushInterval = time.Hour
	reopened, err := OpenIngestService(store, recoveryConfig)
	if err != nil {
		t.Fatalf("reopen PG ingest service without PREPARED WAL: %v", err)
	}
	defer closeIngestService(t, reopened)
	recovered, err := reopened.Accept(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("accept recovered PG WAL request: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := reopened.FlushTenant(flushCtx, "tenant-a"); err != nil {
		t.Fatalf("flush recovered PG WAL request: %v", err)
	}
	result, err := reopened.Wait(flushCtx, recovered)
	if err != nil {
		t.Fatalf("wait recovered PG WAL request: %v", err)
	}
	if result.Version != 1 || result.Applied != 1 || result.Failed != 0 || !result.Skipped {
		t.Fatalf("recovered PG result = %#v, want idempotent replay at version 1", result)
	}
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recovered PG graph: %v", err)
	}
	if _, ok := loaded.GetEntity("host:pg-no-prepared"); !ok {
		t.Fatal("recovered PG graph is missing the committed entity")
	}
}

func crashIngestServiceAllowWALFailure(t *testing.T, service *IngestService) {
	t.Helper()
	service.cancel()
	select {
	case <-service.schedulerOK:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not stop")
	}
	service.workers.Wait()
	if err := service.wal.Close(); err != nil && !errors.Is(err, ErrIngestWALFailed) {
		t.Fatalf("close crashed WAL: %v", err)
	}
}
