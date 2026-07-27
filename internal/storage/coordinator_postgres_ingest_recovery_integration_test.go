package storage

import (
	"errors"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

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
