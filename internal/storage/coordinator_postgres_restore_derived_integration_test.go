package storage

import (
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresRestoreAcknowledgesSynchronousIndexRebuild(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "restore-derived")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	if _, err := store.CreateTenant(ctx, "tenant-source", TenantCreateOptions{}); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit source: %v", err)
	}

	backup, err := store.StartTask(ctx, "tenant-source", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-source", backup.ID)
	if backup.Status != TaskStatusSucceeded {
		t.Fatalf("backup task=%#v", backup)
	}
	backupKey, _ := backup.Result["backup_manifest_key"].(string)
	if backupKey == "" {
		t.Fatalf("backup key missing: %#v", backup.Result)
	}

	restore, err := store.StartTask(ctx, "tenant-restored", TaskTypeTenantRestore, map[string]any{
		"backup_key": backupKey,
	})
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(t, ctx, store, "tenant-restored", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore task=%#v", restore)
	}

	var targetVersion, processedVersion int64
	var status string
	err = coordinator.pool.QueryRow(ctx,
		`SELECT target_version, processed_version, status
		 FROM `+coordinator.table("derived_tasks")+`
		 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3`,
		coordinator.namespace, "tenant-restored", derivedTaskIndexes,
	).Scan(&targetVersion, &processedVersion, &status)
	if err != nil {
		t.Fatalf("read restored derived task: %v", err)
	}
	if processedVersion != targetVersion || status != "done" {
		t.Fatalf(
			"restored derived task target=%d processed=%d status=%q, want acknowledged",
			targetVersion, processedVersion, status,
		)
	}
}
