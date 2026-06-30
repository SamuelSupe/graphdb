package storage

import (
	"context"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCopyTenantObjectsDryRunAndCopy(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	target := NewTenantStore(NewMemoryStore(), "target")
	if _, err := source.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := source.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}

	dryRun, err := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry-run migration: %v", err)
	}
	if !dryRun.DryRun || dryRun.Objects == 0 || dryRun.Copied != 0 || dryRun.Skipped != dryRun.Objects {
		t.Fatalf("dry-run report = %#v", dryRun)
	}
	if exists, err := target.tenantRestoreDataExists(ctx, "tenant-a"); err != nil || exists {
		t.Fatalf("dry-run target exists=%v err=%v", exists, err)
	}

	report, err := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{})
	if err != nil {
		t.Fatalf("copy migration: %v", err)
	}
	if report.Copied == 0 || report.Objects == 0 || report.Bytes == 0 {
		t.Fatalf("copy report = %#v", report)
	}
	g, manifest, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated tenant: %v", err)
	}
	if manifest.TenantID != "tenant-a" {
		t.Fatalf("migrated manifest tenant = %q", manifest.TenantID)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatalf("migrated graph missing entity")
	}
}

func TestCopyTenantObjectsRejectsTenantRename(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	target := NewTenantStore(NewMemoryStore(), "target")
	if _, err := source.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	if _, err := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-b", TenantMigrationOptions{}); err == nil || !strings.Contains(err.Error(), "backup/restore") {
		t.Fatalf("rename migration err = %v", err)
	}
}
