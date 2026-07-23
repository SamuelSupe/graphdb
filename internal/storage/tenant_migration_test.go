package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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
		UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}},
			{ID: "host:z", Kind: "host", Fields: graph.Fields{"name": "z"}},
		},
		UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:z"}},
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
	lease, err := target.GetWriterLease(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get target writer lease: %v", err)
	}
	if manifest.WriterFence != lease.FenceToken || manifest.WriterFenceEpoch != lease.FenceEpoch {
		t.Fatalf("migrated fence = (%q,%d), lease = (%q,%d)", manifest.WriterFence, manifest.WriterFenceEpoch, lease.FenceToken, lease.FenceEpoch)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatalf("migrated graph missing entity")
	}
	indexCatalog, err := target.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get migrated index catalog: %v", err)
	}
	reverseCatalog, err := target.GetReverseIndexCatalog(ctx, "tenant-a", indexCatalog.Version)
	if err != nil {
		t.Fatalf("get migrated reverse catalog: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: target, TenantID: "tenant-a", Version: indexCatalog.Version, Catalog: indexCatalog, ReverseCatalog: &reverseCatalog}
	incoming, ok, err := lookup.InEdges(ctx, "host:z", map[string]struct{}{"links": {}})
	if err != nil || !ok || len(incoming) != 1 || incoming[0].From != "host:a" {
		t.Fatalf("migrated reverse edges=%#v ok=%v err=%v", incoming, ok, err)
	}
	if _, err := target.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit after migration: %v", err)
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

func TestCopyTenantObjectsRequiresTargetWriterFence(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit source: %v", err)
	}
	targetObjects := NewMemoryStore()
	owner := NewTenantStore(targetObjects, "target")
	owner.LeaseTTL = time.Hour
	if _, err := owner.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:target", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit target: %v", err)
	}

	migrator := NewTenantStore(targetObjects, "target")
	_, err := CopyTenantObjects(ctx, source, "tenant-a", migrator, "tenant-a", TenantMigrationOptions{Overwrite: true})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("migration err = %v, want ErrLeaseHeld", err)
	}
	g, _, err := owner.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load target: %v", err)
	}
	if _, ok := g.GetEntity("host:target"); !ok {
		t.Fatal("fenced migration changed active target")
	}
}
