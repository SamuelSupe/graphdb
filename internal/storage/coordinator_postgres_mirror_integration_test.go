package storage

import (
	"fmt"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorLegacyCompletionDoesNotRewriteDoneRows(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "mirror-done-rows")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	for revision := 1; revision <= 3; revision++ {
		_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: fmt.Sprintf("host:%d", revision), Kind: "host",
			}},
		}, CommitOptions{})
		if err != nil {
			t.Fatalf("commit revision %d: %v", revision, err)
		}
	}
	if synced, err := store.SyncLegacyManifests(ctx); err != nil || synced != 1 {
		t.Fatalf("initial mirror sync = %d, err=%v", synced, err)
	}

	sentinel := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	tag, err := coordinator.pool.Exec(ctx,
		`UPDATE `+coordinator.table("legacy_manifest_outbox")+`
		 SET updated_at = $3
		 WHERE namespace = $1 AND tenant_id = $2 AND status = 'done'`,
		coordinator.namespace, "tenant-a", sentinel,
	)
	if err != nil || tag.RowsAffected() != 3 {
		t.Fatalf("mark completed rows: affected=%d err=%v", tag.RowsAffected(), err)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:d", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit revision 4: %v", err)
	}
	if synced, err := store.SyncLegacyManifests(ctx); err != nil || synced != 1 {
		t.Fatalf("second mirror sync = %d, err=%v", synced, err)
	}

	var rewritten int
	if err := coordinator.pool.QueryRow(ctx,
		`SELECT count(*)
		 FROM `+coordinator.table("legacy_manifest_outbox")+`
		 WHERE namespace = $1 AND tenant_id = $2
		   AND head_revision <= 3 AND updated_at <> $3`,
		coordinator.namespace, "tenant-a", sentinel,
	).Scan(&rewritten); err != nil {
		t.Fatalf("count rewritten done rows: %v", err)
	}
	if rewritten != 0 {
		t.Fatalf("completed mirror rows rewritten = %d, want 0", rewritten)
	}
}

func TestPostgresCoordinatorRenewsLegacyManifestLease(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "mirror-renew")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	const (
		initialLeaseTTL = 30 * time.Second
		renewedLeaseTTL = 2 * time.Minute
	)
	job, ok, err := coordinator.ClaimLegacyManifest(
		ctx,
		"owner-a",
		initialLeaseTTL,
	)
	if err != nil || !ok {
		t.Fatalf("claim mirror job: ok=%v err=%v", ok, err)
	}
	readLeaseUntil := func() time.Time {
		t.Helper()
		var leaseUntil time.Time
		if err := coordinator.pool.QueryRow(ctx,
			`SELECT lease_until
			 FROM `+coordinator.table("legacy_manifest_outbox")+`
			 WHERE namespace = $1 AND tenant_id = $2 AND head_revision = $3`,
			coordinator.namespace, job.TenantID, job.HeadRevision,
		).Scan(&leaseUntil); err != nil {
			t.Fatalf("read legacy manifest lease_until: %v", err)
		}
		return leaseUntil
	}
	claimedUntil := readLeaseUntil()
	stale := job
	stale.OwnerToken = "owner-b"
	if renewed, err := coordinator.RenewLegacyManifest(
		ctx,
		stale,
		renewedLeaseTTL,
	); err != nil || renewed {
		t.Fatalf("renew stale owner: renewed=%v err=%v", renewed, err)
	}
	staleUntil := readLeaseUntil()
	if !staleUntil.Equal(claimedUntil) {
		t.Fatalf("stale owner changed lease_until from %s to %s", claimedUntil, staleUntil)
	}
	if renewed, err := coordinator.RenewLegacyManifest(
		ctx,
		job,
		renewedLeaseTTL,
	); err != nil || !renewed {
		t.Fatalf("renew owner: renewed=%v err=%v", renewed, err)
	}
	renewedUntil := readLeaseUntil()
	if !renewedUntil.After(staleUntil) {
		t.Fatalf("owner renewal lease_until = %s, want after prior lease_until %s", renewedUntil, staleUntil)
	}
	if _, claimed, err := coordinator.ClaimLegacyManifest(
		ctx,
		"owner-b",
		renewedLeaseTTL,
	); err != nil || claimed {
		t.Fatalf("claim renewed mirror job: claimed=%v err=%v", claimed, err)
	}
}
