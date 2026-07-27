package storage

import (
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresRestoreDoesNotOverwriteConcurrentCommit(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "restore-concurrent-commit",
	)
	base := NewMemoryStore()
	objects := &blockingCloneSnapshotStore{
		ObjectStore: base,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	defer func() {
		select {
		case <-objects.release:
		default:
			close(objects.release)
		}
	}()
	restoreStore := NewTenantStore(objects, "test")
	restoreStore.SetCoordinator(coordinator)
	if _, err := restoreStore.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:backup", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed backup source: %v", err)
	}
	backup, err := restoreStore.StartTask(
		ctx, "tenant-source", TaskTypeTenantBackup, nil,
	)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, restoreStore, "tenant-source", backup.ID)
	backupKey, _ := backup.Result["backup_manifest_key"].(string)
	if backup.Status != TaskStatusSucceeded || backupKey == "" {
		t.Fatalf("backup task=%#v", backup)
	}

	objects.arm("test/tenants/tenant-target/snapshots/sharded/")
	restore, err := restoreStore.StartTask(
		ctx,
		"tenant-target",
		TaskTypeTenantRestore,
		map[string]any{"backup_key": backupKey},
	)
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	select {
	case <-objects.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not reach snapshot publication")
	}

	concurrent := NewTenantStore(base, "test")
	concurrent.SetCoordinator(coordinator)
	if _, err := concurrent.Commit(ctx, "tenant-target", graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:concurrent", Kind: "host",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("concurrent commit: %v", err)
	}
	close(objects.release)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		restore, err = restoreStore.GetTask(
			ctx, "tenant-target", restore.ID,
		)
		if err != nil {
			t.Fatalf("get restore task: %v", err)
		}
		if taskTerminal(restore.Status) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if restore.Status != TaskStatusFailed ||
		!strings.Contains(restore.Error, "object write conflict") {
		t.Fatalf("restore task=%#v, want CAS conflict", restore)
	}
	g, manifest, err := concurrent.Load(ctx, "tenant-target")
	if err != nil {
		t.Fatalf("load concurrent target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("target version=%d, want concurrent version 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:concurrent"); !ok {
		t.Fatal("restore lost the concurrent commit")
	}
	if _, ok := g.GetEntity("host:backup"); ok {
		t.Fatal("failed restore replaced the concurrent graph")
	}
}
