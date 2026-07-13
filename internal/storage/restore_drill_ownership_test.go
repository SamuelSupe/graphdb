package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestRestoreDrillRejectsExistingTargetWithoutChangingIt(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")
	target := NewTenantStore(objects, "drill")
	seedRestoreDrillTenant(t, ctx, target, "tenant-existing", "host:sentinel")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-existing",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed || !strings.Contains(task.Error, "not empty") {
		t.Fatalf("restore drill task = %#v", task)
	}
	g, manifest, err := target.Load(ctx, "tenant-existing")
	if err != nil {
		t.Fatalf("load existing target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("existing target version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:sentinel"); !ok {
		t.Fatal("restore drill changed the existing target")
	}
}

func TestRestoreDrillRejectsLiveTenantNamespace(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, store, "tenant-a", "host:source")
	seedRestoreDrillTenant(t, ctx, store, "tenant-live", "host:sentinel")

	task, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "test",
		"target_tenant_id": "tenant-live",
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed || !strings.Contains(task.Error, "isolated") {
		t.Fatalf("restore drill task = %#v", task)
	}
	g, manifest, err := store.Load(ctx, "tenant-live")
	if err != nil {
		t.Fatalf("load live target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("live target version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:sentinel"); !ok {
		t.Fatal("restore drill changed the live target")
	}
}

func TestRestoreDrillCleanupRejectsChangedOwnedTarget(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "drill")
	seedRestoreDrillTenant(t, ctx, store, "tenant-target", "host:a")
	now := time.Now().UTC()
	marker := Task{
		ID:         "drill-restore",
		TenantID:   "tenant-target",
		Type:       TaskTypeTenantRestore,
		Status:     TaskStatusSucceeded,
		Phase:      "done",
		OwnerID:    store.InstanceID,
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, marker); err != nil {
		t.Fatalf("save ownership marker: %v", err)
	}
	ownership, err := store.captureRestoreDrillOwnership(ctx, "tenant-target", marker.ID)
	if err != nil {
		t.Fatalf("capture ownership: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-target", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("change target after capture: %v", err)
	}
	if _, err := store.cleanupRestoreDrillTarget(ctx, "tenant-target", ownership); !errors.Is(err, ErrConflict) {
		t.Fatalf("cleanup err = %v, want ErrConflict", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-target")
	if err != nil {
		t.Fatalf("load changed target: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("changed target version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("rejected cleanup deleted the changed target")
	}
}

func TestRestoreDrillOwnedCleanupRemovesOnlyTargetTenant(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-cleanup",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded || task.Result["status"] != "passed" {
		t.Fatalf("restore drill task = %#v", task)
	}
	target := NewTenantStore(objects, "drill")
	if _, err := target.GetTenantInfo(ctx, "tenant-cleanup"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cleaned target err = %v, want ErrNotFound", err)
	}
	objectsLeft, err := objects.List(ctx, target.tenantObjectPrefix("tenant-cleanup"))
	if err != nil {
		t.Fatalf("list cleaned target: %v", err)
	}
	if len(objectsLeft) != 0 {
		t.Fatalf("cleaned target objects = %#v", objectsLeft)
	}
	purged, err := target.tenantPurgeTombstoneExists(ctx, "tenant-cleanup")
	if err != nil || purged {
		t.Fatalf("cleanup tombstone exists=%v err=%v", purged, err)
	}
}

func seedRestoreDrillTenant(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, entityID string) {
	t.Helper()
	if _, err := store.CreateTenant(ctx, tenantID, TenantCreateOptions{}); err != nil {
		t.Fatalf("create %s: %v", tenantID, err)
	}
	if _, err := store.Commit(ctx, tenantID, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: entityID, Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit %s: %v", tenantID, err)
	}
}

func waitForRestoreDrillTerminalTask(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, taskID string) Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(ctx, tenantID, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if task.Status != TaskStatusQueued && task.Status != TaskStatusRunning {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish", taskID)
	return Task{}
}
