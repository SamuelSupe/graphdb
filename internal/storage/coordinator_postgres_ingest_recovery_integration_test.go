package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresIngestReplayUsesCoordinatorWithoutPreparedWAL(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-replay-no-prepared")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.InstanceID = "pg-replay-writer"
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
		t.Fatalf("open PostgreSQL ingest service: %v", err)
	}
	request := ingestEntityRequest("batch-pg-replay-no-prepared", "host:pg-replay-no-prepared")
	accepted, err := service.Accept(ctx, "tenant-a", request)
	if err != nil {
		closeIngestService(t, service)
		t.Fatalf("accept coordinated request: %v", err)
	}
	if accepted.pending == nil || accepted.pending.envelope.AcceptedGeneration <= 0 {
		crashIngestService(t, service)
		t.Fatalf("accepted coordinated request = %#v, want bound generation", accepted)
	}

	entries := []IngestBatchEntry{{
		Request:            accepted.pending.envelope.Request,
		AcceptedAt:         accepted.pending.envelope.AcceptedAt,
		AcceptedGeneration: accepted.pending.envelope.AcceptedGeneration,
	}}
	committed, err := store.IngestDurableBatch(ctx, "tenant-a", entries)
	if err != nil {
		crashIngestService(t, service)
		t.Fatalf("commit request through coordinator: %v", err)
	}
	if len(committed) != 1 || committed[0].Version != 1 || committed[0].Applied != 1 {
		crashIngestService(t, service)
		t.Fatalf("coordinator commit result = %#v, want version 1", committed)
	}
	crashIngestService(t, service)

	wal, records, err := OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatalf("inspect accepted-only WAL after crash: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close accepted-only WAL: %v", err)
	}
	for _, record := range records {
		if record.Type == IngestWALPrepared {
			t.Fatalf("PostgreSQL WAL wrote Prepared before crash: %#v", record)
		}
	}

	recovered, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatalf("reopen PostgreSQL ingest service: %v", err)
	}
	recoveredAcceptance, err := recovered.Accept(ctx, "tenant-a", accepted.pending.envelope.Request)
	if err != nil {
		crashIngestService(t, recovered)
		t.Fatalf("attach to recovered request: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := recovered.FlushTenant(flushCtx, "tenant-a"); err != nil {
		closeIngestService(t, recovered)
		t.Fatalf("flush coordinator-authoritative replay: %v", err)
	}
	replayed, err := recovered.Wait(flushCtx, recoveredAcceptance)
	if err != nil {
		crashIngestService(t, recovered)
		t.Fatalf("wait coordinator-authoritative replay: %v", err)
	}
	if !replayed.Skipped || replayed.Version != 1 || replayed.Applied != 1 || replayed.Failed != 0 {
		crashIngestService(t, recovered)
		t.Fatalf("coordinator-authoritative replay result = %#v, want skipped version 1", replayed)
	}
	crashIngestService(t, recovered)

	wal, records, err = OpenIngestWAL(config.WAL)
	if err != nil {
		t.Fatalf("inspect replayed WAL: %v", err)
	}
	defer wal.Close()
	prepared, finalized := 0, 0
	for _, record := range records {
		switch record.Type {
		case IngestWALPrepared:
			prepared++
		case IngestWALFinalized:
			finalized++
		}
	}
	if prepared != 0 || finalized != 1 {
		t.Fatalf("replayed WAL prepared=%d finalized=%d records=%#v, want 0/1", prepared, finalized, records)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 1 {
		t.Fatalf("coordinator head after replay = %#v exists=%v err=%v, want graph version 1", head, exists, err)
	}
}

func TestPostgresCoordinatorInitialFailedIngestPublishesHead(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "initial-failed-ingest",
	)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	request := IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "batch-1",
		Items: []IngestItem{{ExternalID: "invalid"}},
	}

	result, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("initial failed ingest: %v", err)
	}
	if result.Failed != 1 || result.Applied != 0 || result.Version != 0 {
		t.Fatalf("result = %#v, want one failed item at version 0", result)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 0 {
		t.Fatalf("head = %#v exists=%v err=%v", head, exists, err)
	}
	status, err := store.GetCollectorStatus(
		ctx, "tenant-a", "agent", "collector-a",
	)
	if err != nil ||
		status.LastBatchID != "batch-1" ||
		status.LastVersion != 0 {
		t.Fatalf("collector status = %#v err=%v", status, err)
	}

	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay failed ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Failed != 1 || replayed.Version != 0 {
		t.Fatalf("replayed = %#v, want skipped failed result", replayed)
	}
}

func TestPostgresCoordinatorInitialNoopCommitPublishesHead(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "initial-noop-commit",
	)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	result, err := store.CommitWithReport(
		ctx, "tenant-a", graph.Mutations{}, CommitOptions{},
	)
	if err != nil {
		t.Fatalf("initial no-op commit: %v", err)
	}
	if !result.Skipped || result.Version != 0 || result.DataMD5 == "" {
		t.Fatalf("result = %#v, want skipped version 0", result)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.GraphVersion != 0 {
		t.Fatalf("head = %#v exists=%v err=%v", head, exists, err)
	}
}

func TestPostgresCoordinatorIngestReplayRepairsDeadLetter(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "ingest-deadletter-repair",
	)
	base := NewMemoryStore()
	objects := &failPutOnceStore{
		ObjectStore: base,
		contains:    "/deadletters/",
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	request := IngestRequest{
		Source: "agent", CollectorID: "collector-a", BatchID: "batch-1",
		Items: []IngestItem{{ExternalID: "invalid"}},
	}

	result, err := store.Ingest(ctx, "tenant-a", request)
	if err == nil || !strings.Contains(err.Error(), "save dead letter") {
		t.Fatalf("first ingest err = %v, want dead-letter persistence error", err)
	}
	if result.Failed != 1 || result.Version != 1 {
		t.Fatalf("first result = %#v, want failed no-op batch", result)
	}
	if _, err := base.Get(
		ctx,
		store.deadLetterKey("tenant-a", "agent", "collector-a/batch-1"),
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dead letter before replay err = %v, want not found", err)
	}

	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.Failed != 1 {
		t.Fatalf("replayed = %#v, want skipped failed result", replayed)
	}
	letters, err := store.ListDeadLetters(ctx, "tenant-a", "agent")
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(letters) != 1 || letters[0].BatchID != "batch-1" {
		t.Fatalf("dead letters = %#v, want repaired batch-1", letters)
	}
}
