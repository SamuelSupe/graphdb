package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestPostgresCoordinatorPublishesIngestBatchAtomically(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-atomic")
	seedCoordinatorHead(t, ctx, coordinator, "tenant-a")

	reservations := []struct {
		key   string
		hash  string
		owner string
	}{
		{key: "ingest/agent/collector-a/batch-a", hash: "hash-a", owner: "writer-a"},
		{key: "ingest/agent/collector-b/batch-b", hash: "hash-b", owner: "writer-b"},
	}
	for _, item := range reservations {
		if _, err := coordinator.ReserveCommit(ctx, "tenant-a", item.key, item.hash, item.owner, 0); err != nil {
			t.Fatalf("reserve %s: %v", item.key, err)
		}
	}

	firstResult, _ := json.Marshal(IngestResult{BatchID: "batch-a", Version: 1, Applied: 1})
	secondResult, _ := json.Marshal(IngestResult{BatchID: "batch-b", Version: 2, Applied: 1})
	updated, published, err := coordinator.PublishIngestBatch(ctx, IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     "tenant-a",
			ExpectedRevision:             1,
			ExpectedGeneration:           1,
			ExpectedWriteContextRevision: 0,
			GraphVersion:                 2,
			ManifestKey:                  "tenants/tenant-a/manifests/2.json",
			ManifestHash:                 "manifest-hash-2",
			CommitID:                     "commit-2",
		},
		Items: []IngestBatchCompletion{
			{
				IdempotencyKey: reservations[0].key,
				RequestHash:    reservations[0].hash,
				OwnerToken:     reservations[0].owner,
				Result:         firstResult,
				CollectorState: &CollectorStateUpdate{
					Source: "agent", CollectorID: "collector-a", BatchID: "batch-a", Cursor: "cursor-a", Version: 1,
				},
			},
			{
				IdempotencyKey: reservations[1].key,
				RequestHash:    reservations[1].hash,
				OwnerToken:     reservations[1].owner,
				Result:         secondResult,
				CollectorState: &CollectorStateUpdate{
					Source: "agent", CollectorID: "collector-b", BatchID: "batch-b", Cursor: "cursor-b", Version: 2,
				},
			},
		},
	})
	if err != nil || !published {
		t.Fatalf("publish ingest batch published=%v err=%v", published, err)
	}
	if updated.Revision != 2 || updated.GraphVersion != 2 || updated.ManifestHash != "manifest-hash-2" {
		t.Fatalf("updated head = %#v", updated)
	}

	for _, item := range reservations {
		reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", item.key)
		if err != nil {
			t.Fatalf("load reservation %s: %v", item.key, err)
		}
		if !reservation.Committed || reservation.OwnerToken != item.owner {
			t.Fatalf("reservation %s = %#v, want committed", item.key, reservation)
		}
	}
	for _, want := range []struct {
		collector string
		batch     string
		version   int64
	}{
		{collector: "collector-a", batch: "batch-a", version: 1},
		{collector: "collector-b", batch: "batch-b", version: 2},
	} {
		state, exists, err := coordinator.CollectorState(ctx, "tenant-a", "agent", want.collector)
		if err != nil || !exists {
			t.Fatalf("collector state %s exists=%v err=%v", want.collector, exists, err)
		}
		if state.BatchID != want.batch || state.Version != want.version {
			t.Fatalf("collector state %s = %#v", want.collector, state)
		}
	}
	assertCoordinatorRows(t, coordinator,
		`SELECT count(*) FROM `+coordinator.table("legacy_manifest_outbox")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND head_revision = 2`,
		1,
	)
	assertCoordinatorRows(t, coordinator,
		`SELECT count(*) FROM `+coordinator.table("derived_tasks")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND target_version = 2`,
		1,
	)
}

func TestPostgresCoordinatorIngestBatchCompletionRollsBackHeadAndEarlierItems(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-batch-rollback")
	seedCoordinatorHead(t, ctx, coordinator, "tenant-a")

	if _, err := coordinator.ReserveCommit(ctx, "tenant-a", "ingest/first", "hash-first", "writer-a", 0); err != nil {
		t.Fatalf("reserve first item: %v", err)
	}
	result, _ := json.Marshal(IngestResult{BatchID: "first", Version: 1, Applied: 1})
	_, published, err := coordinator.PublishIngestBatch(ctx, IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     "tenant-a",
			ExpectedRevision:             1,
			ExpectedGeneration:           1,
			ExpectedWriteContextRevision: 0,
			GraphVersion:                 1,
			ManifestKey:                  "manifest-1",
			ManifestHash:                 "manifest-hash-1",
			CommitID:                     "commit-1",
		},
		Items: []IngestBatchCompletion{
			{
				IdempotencyKey: "ingest/first", RequestHash: "hash-first", OwnerToken: "writer-a", Result: result,
				CollectorState: &CollectorStateUpdate{Source: "agent", CollectorID: "collector-a", BatchID: "first", Version: 1},
			},
			{
				// The second key is intentionally not reserved. The transaction
				// must not leave the first completion or the head visible.
				IdempotencyKey: "ingest/missing", RequestHash: "hash-missing", OwnerToken: "writer-a", Result: result,
			},
		},
	})
	if published || !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("publish with missing reservation published=%v err=%v, want rollback", published, err)
	}

	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head after rollback exists=%v err=%v", exists, err)
	}
	if head.Revision != 1 || head.GraphVersion != 0 || head.ManifestKey != "manifest-0" {
		t.Fatalf("head after rollback = %#v", head)
	}
	reservation, err := coordinator.loadCommitReservation(ctx, "tenant-a", "ingest/first")
	if err != nil {
		t.Fatalf("load first reservation after rollback: %v", err)
	}
	if reservation.Committed {
		t.Fatalf("first reservation committed after rollback: %#v", reservation)
	}
	_, exists, err = coordinator.CollectorState(ctx, "tenant-a", "agent", "collector-a")
	if err != nil {
		t.Fatalf("collector state after rollback: %v", err)
	}
	if exists {
		t.Fatal("collector state from rolled-back batch remains")
	}
	assertCoordinatorRows(t, coordinator,
		`SELECT count(*) FROM `+coordinator.table("legacy_manifest_outbox")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND head_revision = 2`,
		0,
	)
	assertCoordinatorRows(t, coordinator,
		`SELECT count(*) FROM `+coordinator.table("derived_tasks")+` WHERE namespace = $1 AND tenant_id = 'tenant-a' AND target_version = 1`,
		0,
	)
}

func seedCoordinatorHead(t *testing.T, ctx context.Context, coordinator *PostgresCoordinator, tenantID string) {
	t.Helper()
	// This helper keeps the initial head explicit so the test exercises the
	// update/CAS path rather than the first-publish insert path.
	if err := coordinator.BootstrapHead(ctx, CoordinationHead{
		TenantID:     tenantID,
		Generation:   1,
		Status:       TenantStatusActive,
		Revision:     1,
		GraphVersion: 0,
		ManifestKey:  "manifest-0",
		ManifestHash: "manifest-hash-0",
	}, false); err != nil {
		t.Fatalf("seed coordinator head: %v", err)
	}
}
