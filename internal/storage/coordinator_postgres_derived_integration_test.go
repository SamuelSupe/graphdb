package storage

import (
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorDefersDerivedIndexesWhileTenantDisabled(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t,
		"derived-disabled",
	)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	committed, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID:   "host:a",
			Kind: "host",
		}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.SetTenantStatus(
		ctx,
		"tenant-a",
		TenantStatusDisabled,
	); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	time.Sleep(derivedTaskDebounce + 50*time.Millisecond)

	if processed, err := store.SyncDerivedTasks(ctx); err != nil || processed != 0 {
		t.Fatalf(
			"sync disabled tenant processed=%d err=%v, want 0 and nil",
			processed,
			err,
		)
	}
	assertDerivedTaskState(
		t,
		coordinator,
		"tenant-a",
		committed.Version,
		0,
		"pending",
	)

	if _, err := store.SetTenantStatus(
		ctx,
		"tenant-a",
		TenantStatusActive,
	); err != nil {
		t.Fatalf("enable tenant: %v", err)
	}
	if _, err := coordinator.pool.Exec(
		ctx,
		`UPDATE `+coordinator.table("derived_tasks")+`
		 SET next_attempt_at = now()
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3`,
		coordinator.namespace,
		"tenant-a",
		derivedTaskIndexes,
	); err != nil {
		t.Fatalf("make deferred task ready: %v", err)
	}
	if processed, err := store.SyncDerivedTasks(ctx); err != nil || processed != 1 {
		t.Fatalf(
			"sync enabled tenant processed=%d err=%v, want 1 and nil",
			processed,
			err,
		)
	}
	assertDerivedTaskState(
		t,
		coordinator,
		"tenant-a",
		committed.Version,
		committed.Version,
		"done",
	)
}

func assertDerivedTaskState(
	t *testing.T,
	coordinator *PostgresCoordinator,
	tenantID string,
	targetVersion int64,
	processedVersion int64,
	status string,
) {
	t.Helper()
	var gotTarget int64
	var gotProcessed int64
	var gotStatus string
	err := coordinator.pool.QueryRow(
		t.Context(),
		`SELECT target_version, processed_version, status
		 FROM `+coordinator.table("derived_tasks")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3`,
		coordinator.namespace,
		tenantID,
		derivedTaskIndexes,
	).Scan(&gotTarget, &gotProcessed, &gotStatus)
	if err != nil {
		t.Fatalf("load derived task state: %v", err)
	}
	if gotTarget != targetVersion ||
		gotProcessed != processedVersion ||
		gotStatus != status {
		t.Fatalf(
			"derived task target=%d processed=%d status=%q, want %d, %d, %q",
			gotTarget,
			gotProcessed,
			gotStatus,
			targetVersion,
			processedVersion,
			status,
		)
	}
}
