package storage

import (
	"testing"
	"time"
)

func TestPostgresCoordinatorCleanupIsBoundedAndWatermarkSafe(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "cleanup")
	_, err := coordinator.pool.Exec(ctx,
		`INSERT INTO `+coordinator.table("tenant_heads")+` (
			namespace, tenant_id, generation, status, head_revision, graph_version,
			manifest_key, manifest_hash, legacy_manifest_revision, updated_at
		) VALUES ($1,'tenant-a',1,'active',5,5,'manifest-5','hash-5',3,now())`,
		coordinator.namespace,
	)
	if err != nil {
		t.Fatalf("insert head: %v", err)
	}
	_, err = coordinator.pool.Exec(ctx,
		`INSERT INTO `+coordinator.table("commit_idempotency")+` (
			namespace, tenant_id, idempotency_key, request_hash, owner_token,
			status, started_at, updated_at
		) VALUES
			($1,'tenant-a','abandoned','hash','owner','pending',now()-interval '96 hours',now()-interval '96 hours'),
			($1,'tenant-a','old-a','hash','owner','committed',now()-interval '72 hours',now()-interval '72 hours'),
			($1,'tenant-a','old-b','hash','owner','committed',now()-interval '48 hours',now()-interval '48 hours'),
			($1,'tenant-a','fresh','hash','owner','committed',now(),now()),
			($1,'tenant-a','pending','hash','owner','pending',now(),now())`,
		coordinator.namespace,
	)
	if err != nil {
		t.Fatalf("insert idempotency rows: %v", err)
	}
	_, err = coordinator.pool.Exec(ctx,
		`INSERT INTO `+coordinator.table("legacy_manifest_outbox")+` (
			namespace, tenant_id, head_revision, graph_version, manifest_key,
			manifest_hash, status, created_at, updated_at
		) VALUES
			($1,'tenant-a',1,1,'manifest-1','hash-1','done',now()-interval '3 hours',now()-interval '3 hours'),
			($1,'tenant-a',2,2,'manifest-2','hash-2','done',now(),now()),
			($1,'tenant-a',3,3,'manifest-3','hash-3','pending',now()-interval '3 hours',now()-interval '3 hours'),
			($1,'tenant-a',4,4,'manifest-4','hash-4','done',now()-interval '3 hours',now()-interval '3 hours')`,
		coordinator.namespace,
	)
	if err != nil {
		t.Fatalf("insert outbox rows: %v", err)
	}

	config := CoordinatorCleanupConfig{
		IdempotencyRetention: 24 * time.Hour,
		OutboxRetention:      time.Hour,
		BatchSize:            1,
	}
	first, err := coordinator.Cleanup(ctx, config)
	if err != nil {
		t.Fatalf("first cleanup: %v", err)
	}
	if first.IdempotencyDeleted != 1 || first.OutboxDeleted != 1 {
		t.Fatalf("first cleanup report = %#v", first)
	}
	second, err := coordinator.Cleanup(ctx, config)
	if err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if second.IdempotencyDeleted != 1 || second.OutboxDeleted != 0 {
		t.Fatalf("second cleanup report = %#v", second)
	}
	third, err := coordinator.Cleanup(ctx, config)
	if err != nil {
		t.Fatalf("third cleanup: %v", err)
	}
	if third.IdempotencyDeleted != 1 || third.OutboxDeleted != 0 {
		t.Fatalf("third cleanup report = %#v", third)
	}

	assertCoordinatorRows(t, coordinator, `
		SELECT count(*) FROM `+coordinator.table("commit_idempotency")+`
		WHERE namespace = $1`, 2)
	assertCoordinatorRows(t, coordinator, `
		SELECT count(*) FROM `+coordinator.table("commit_idempotency")+`
		WHERE namespace = $1 AND idempotency_key IN ('fresh','pending')`, 2)
	assertCoordinatorRows(t, coordinator, `
		SELECT count(*) FROM `+coordinator.table("legacy_manifest_outbox")+`
		WHERE namespace = $1`, 3)
	assertCoordinatorRows(t, coordinator, `
		SELECT count(*) FROM `+coordinator.table("legacy_manifest_outbox")+`
		WHERE namespace = $1 AND head_revision IN (2,3,4)`, 3)

	_, err = coordinator.pool.Exec(ctx,
		`INSERT INTO `+coordinator.table("commit_idempotency")+` (
			namespace, tenant_id, idempotency_key, request_hash, owner_token,
			status, started_at, updated_at
		) VALUES
			($1,'tenant-a','short-committed','hash','owner','committed',now()-interval '2 minutes',now()-interval '2 minutes'),
			($1,'tenant-a','active-pending','hash','owner','pending',now()-interval '2 minutes',now()-interval '2 minutes')`,
		coordinator.namespace,
	)
	if err != nil {
		t.Fatalf("insert short-retention rows: %v", err)
	}
	short, err := coordinator.Cleanup(ctx, CoordinatorCleanupConfig{
		IdempotencyRetention: time.Minute,
		BatchSize:            10,
	})
	if err != nil {
		t.Fatalf("short-retention cleanup: %v", err)
	}
	if short.IdempotencyDeleted != 1 {
		t.Fatalf("short-retention report = %#v", short)
	}
	assertCoordinatorRows(t, coordinator, `
		SELECT count(*) FROM `+coordinator.table("commit_idempotency")+`
		WHERE namespace = $1 AND idempotency_key = 'active-pending'`, 1)
}

func assertCoordinatorRows(
	t *testing.T,
	coordinator *PostgresCoordinator,
	query string,
	want int,
) {
	t.Helper()
	var count int
	if err := coordinator.pool.QueryRow(
		t.Context(),
		query,
		coordinator.namespace,
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != want {
		t.Fatalf("row count = %d, want %d", count, want)
	}
}
