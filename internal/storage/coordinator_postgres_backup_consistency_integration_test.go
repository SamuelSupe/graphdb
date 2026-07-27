package storage

import (
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresBackupCapturesOneGraphAndWriteContextRevision(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "backup-context-snapshot",
	)
	base := NewMemoryStore()
	objects := newBlockingManifestReadStore(
		base, "test/tenants/tenant-source/metadata.parquet",
	)
	defer func() {
		select {
		case <-objects.release:
		default:
			close(objects.release)
		}
	}()
	backupStore := NewTenantStore(objects, "test")
	backupStore.SetCoordinator(coordinator)
	if _, err := backupStore.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	objects.arm()
	backup, err := backupStore.StartTask(
		ctx, "tenant-source", TaskTypeTenantBackup, nil,
	)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	select {
	case <-objects.started:
	case <-time.After(5 * time.Second):
		t.Fatal("backup did not reach metadata capture")
	}

	concurrent := NewTenantStore(base, "test")
	concurrent.SetCoordinator(coordinator)
	if _, err := concurrent.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "depends_custom", FromKind: "document",
			ToKind: "document", Directed: true,
		}},
		UpsertEdges: []graph.Edge{{
			Type: "depends_custom", From: "document:a", To: "document:b",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance source graph: %v", err)
	}
	if _, err := concurrent.PutRelationSchema(
		ctx,
		"tenant-source",
		RelationSchema{RelationType: "depends_custom"},
	); err != nil {
		t.Fatalf("advance source write context: %v", err)
	}
	close(objects.release)

	backup = waitForTask(
		t, ctx, backupStore, "tenant-source", backup.ID,
	)
	backupKey, _ := backup.Result["backup_manifest_key"].(string)
	if backupKey == "" {
		t.Fatalf("backup manifest key missing: %#v", backup.Result)
	}
	input, err := backupStore.loadTenantBackupInput(ctx, backupKey)
	if err != nil {
		t.Fatalf("load backup input: %v", err)
	}
	if input.Record.Version != 2 ||
		len(input.Record.RelationSchemas) != 1 ||
		input.Record.RelationSchemas[0].RelationType != "depends_custom" {
		t.Fatalf("backup record=%#v", input.Record)
	}
	hasRelationType := false
	for _, relationType := range input.Record.Snapshot.RelationTypes {
		if relationType.Name == "depends_custom" {
			hasRelationType = true
			break
		}
	}
	if !hasRelationType {
		t.Fatal("backup mixed the new schema with the old graph snapshot")
	}

	restore, err := backupStore.StartTask(
		ctx,
		"tenant-restored",
		TaskTypeTenantRestore,
		map[string]any{"backup_key": backupKey},
	)
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(
		t, ctx, backupStore, "tenant-restored", restore.ID,
	)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore task=%#v", restore)
	}
}
