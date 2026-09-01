package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
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

func TestPostgresCoordinatorFastIngestPublishSlotReturnsHeadAndReleasesOnCommit(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-publish-slot")
	seedCoordinatorHead(t, ctx, coordinator, "tenant-a")

	wantHead, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("seed head exists=%v err=%v", exists, err)
	}
	first, head, headExists, acquired, err := coordinator.AcquireIngestPublishSlot(
		ctx, "tenant-a", "writer-a", time.Second,
	)
	if err != nil || !acquired {
		t.Fatalf("first publish slot acquired=%v err=%v", acquired, err)
	}
	if first.TaskType != coordinatorIngestPublishTaskType {
		t.Fatalf("first publish slot task type = %q, want %q", first.TaskType, coordinatorIngestPublishTaskType)
	}
	if !headExists || head != wantHead {
		t.Fatalf("fast acquire head exists=%v head=%#v, want exists=%v head=%#v", headExists, head, true, wantHead)
	}
	if _, competingHead, competingHeadExists, competing, err := coordinator.AcquireIngestPublishSlot(
		ctx, "tenant-a", "writer-b", time.Second,
	); err != nil {
		t.Fatalf("competing publish slot: %v", err)
	} else if competing || competingHeadExists || competingHead != (CoordinationHead{}) {
		t.Fatalf("competing publish slot acquired=%v head exists=%v head=%#v, want no reservation", competing, competingHeadExists, competingHead)
	}

	updated, published, err := coordinator.PublishIngestBatch(ctx, IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     "tenant-a",
			ExpectedRevision:             wantHead.Revision,
			ExpectedGeneration:           wantHead.Generation,
			ExpectedWriteContextRevision: wantHead.WriteContextRevision,
			GraphVersion:                 wantHead.GraphVersion + 1,
			ManifestKey:                  "manifest-1",
			ManifestHash:                 "manifest-hash-1",
			CommitID:                     "commit-1",
		},
		PublishLease: &first,
	})
	if err != nil || !published {
		t.Fatalf("publish with slot lease published=%v err=%v", published, err)
	}
	if updated.Revision != wantHead.Revision+1 || updated.GraphVersion != wantHead.GraphVersion+1 {
		t.Fatalf("updated head = %#v", updated)
	}
	second, _, _, acquired, err := coordinator.AcquireIngestPublishSlot(
		ctx, "tenant-a", "writer-b", time.Second,
	)
	if err != nil || !acquired {
		t.Fatalf("next owner after publish acquired=%v err=%v", acquired, err)
	}
	if second.OwnerToken != "writer-b" || second.FenceEpoch <= first.FenceEpoch {
		t.Fatalf("next owner lease = %#v, first = %#v", second, first)
	}
	freshRequest := IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     "tenant-a",
			ExpectedRevision:             updated.Revision,
			ExpectedGeneration:           updated.Generation,
			ExpectedWriteContextRevision: updated.WriteContextRevision,
			GraphVersion:                 updated.GraphVersion + 1,
			ManifestKey:                  "manifest-stale-lease",
			ManifestHash:                 "manifest-hash-stale-lease",
			CommitID:                     "commit-stale-lease",
		},
		PublishLease: &first,
	}
	if _, published, err := coordinator.PublishIngestBatch(ctx, freshRequest); published || !errors.Is(err, ErrTaskLeaseHeld) {
		t.Fatalf("publish with stale lease published=%v err=%v, want ErrTaskLeaseHeld", published, err)
	}
	if current, _, err := coordinator.Head(ctx, "tenant-a"); err != nil || current != updated {
		t.Fatalf("head after stale lease publish = %#v err=%v, want %#v", current, err, updated)
	}
	currentLease, active, err := coordinator.TaskLease(ctx, "tenant-a", coordinatorIngestPublishTaskType)
	if err != nil || !active || currentLease.OwnerToken != second.OwnerToken || currentLease.FenceEpoch != second.FenceEpoch {
		t.Fatalf("lease after stale publish active=%v lease=%#v err=%v, want %#v", active, currentLease, err, second)
	}

	staleRequest := IngestBatchPublishRequest{
		Head: HeadPublishRequest{
			TenantID:                     "tenant-a",
			ExpectedRevision:             wantHead.Revision,
			ExpectedGeneration:           wantHead.Generation,
			ExpectedWriteContextRevision: wantHead.WriteContextRevision,
			GraphVersion:                 updated.GraphVersion + 1,
			ManifestKey:                  "manifest-stale",
			ManifestHash:                 "manifest-hash-stale",
			CommitID:                     "commit-stale",
		},
	}
	for name, lease := range map[string]*CoordinatorTaskLease{
		"other owner":   &first,
		"current owner": &second,
	} {
		staleRequest.PublishLease = lease
		if _, published, err := coordinator.PublishIngestBatch(ctx, staleRequest); err != nil || published {
			t.Fatalf("CAS conflict with %s published=%v err=%v, want no publish", name, published, err)
		}
		current, active, err := coordinator.TaskLease(ctx, "tenant-a", coordinatorIngestPublishTaskType)
		if err != nil || !active || current.OwnerToken != second.OwnerToken || current.FenceEpoch != second.FenceEpoch {
			t.Fatalf("lease after %s CAS conflict active=%v lease=%#v err=%v, want current owner %#v", name, active, current, err, second)
		}
	}
	if _, err := coordinator.pool.Exec(ctx,
		`UPDATE `+coordinator.table("task_leases")+`
		 SET expires_at = now() - interval '1 second'
		 WHERE namespace = $1 AND tenant_id = 'tenant-a' AND task_type = $2`,
		coordinator.namespace, coordinatorIngestPublishTaskType,
	); err != nil {
		t.Fatalf("expire current publish lease: %v", err)
	}
	freshRequest.PublishLease = &second
	if _, published, err := coordinator.PublishIngestBatch(ctx, freshRequest); published || !errors.Is(err, ErrTaskLeaseHeld) {
		t.Fatalf("publish with expired lease published=%v err=%v, want ErrTaskLeaseHeld", published, err)
	}
	if current, _, err := coordinator.Head(ctx, "tenant-a"); err != nil || current != updated {
		t.Fatalf("head after expired lease publish = %#v err=%v, want %#v", current, err, updated)
	}
}

func TestPostgresCoordinatorFastIngestPublishSlotReadsFreshHeadAfterLeaseWait(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "ingest-publish-slot-lock")
	seedCoordinatorHead(t, ctx, coordinator, "tenant-a")
	if _, acquired, err := coordinator.AcquireTaskLease(
		ctx, "tenant-a", coordinatorIngestPublishTaskType, "seed-owner", time.Minute,
	); err != nil || !acquired {
		t.Fatalf("seed publish slot acquired=%v err=%v", acquired, err)
	}

	tx, err := coordinator.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tag, err := tx.Exec(ctx,
		`UPDATE `+coordinator.table("task_leases")+`
		 SET owner_token = 'blocker-owner', expires_at = now() - interval '1 second', updated_at = now()
		 WHERE namespace = $1 AND tenant_id = 'tenant-a' AND task_type = $2`,
		coordinator.namespace, coordinatorIngestPublishTaskType,
	)
	if err != nil {
		t.Fatalf("lock publish slot row: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("locked publish slot rows = %d, want 1", tag.RowsAffected())
	}
	if _, err := tx.Exec(ctx,
		`UPDATE `+coordinator.table("tenant_heads")+`
		 SET head_revision = 2, graph_version = 1,
		     manifest_key = 'manifest-fresh', manifest_hash = 'manifest-hash-fresh',
		     commit_id = 'commit-fresh', updated_at = now()
		 WHERE namespace = $1 AND tenant_id = 'tenant-a'`,
		coordinator.namespace,
	); err != nil {
		t.Fatalf("update blocked head: %v", err)
	}

	type acquireResult struct {
		lease      CoordinatorTaskLease
		head       CoordinationHead
		headExists bool
		acquired   bool
		err        error
	}
	resultCh := make(chan acquireResult, 1)
	go func() {
		lease, head, headExists, acquired, err := coordinator.AcquireIngestPublishSlot(
			ctx, "tenant-a", "contender-owner", time.Minute,
		)
		resultCh <- acquireResult{
			lease: lease, head: head, headExists: headExists, acquired: acquired, err: err,
		}
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := waitForPostgresTaskLeaseLock(waitCtx, coordinator); err != nil {
		t.Fatalf("waiting for contender task-lease row lock: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit blocker transaction: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.err != nil || !result.acquired {
			t.Fatalf("contender publish slot acquired=%v err=%v", result.acquired, result.err)
		}
		if !result.headExists || result.head.Revision != 2 ||
			result.head.GraphVersion != 1 || result.head.ManifestKey != "manifest-fresh" ||
			result.head.ManifestHash != "manifest-hash-fresh" {
			t.Fatalf("contender head = %#v, want fresh revision 2", result.head)
		}
		if err := coordinator.ReleaseTaskLease(ctx, result.lease); err != nil {
			t.Fatalf("release contender publish slot: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for contender result: %v", ctx.Err())
	}
}

func waitForPostgresTaskLeaseLock(ctx context.Context, coordinator *PostgresCoordinator) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := coordinator.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%task_leases%'
			)`).Scan(&waiting); err != nil {
			return err
		}
		if waiting {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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
