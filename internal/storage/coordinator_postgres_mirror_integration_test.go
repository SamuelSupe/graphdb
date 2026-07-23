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
