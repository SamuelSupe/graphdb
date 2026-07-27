package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIntegrityAuditValidatesCurrentObjectChain(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	seedIntegrityTenant(t, ctx, store)

	report, err := store.AuditIntegrity(ctx, "tenant-a", IntegrityAuditOptions{Deep: true})
	if err != nil {
		t.Fatalf("audit integrity: %v", err)
	}
	if report.Status != "ok" || len(report.Issues) != 0 {
		t.Fatalf("audit report = %#v", report)
	}
	if report.Objects == 0 || report.Bytes == 0 || !hasIntegrityCheck(report.Checks, "entity_record") {
		t.Fatalf("audit checks = %#v", report.Checks)
	}
}

func TestIntegrityAuditAllowsDisabledOptionalEntityRecords(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	store.WriteEntityRecords = false
	seedIntegrityTenant(t, ctx, store)

	report, err := store.AuditIntegrity(
		ctx, "tenant-a", IntegrityAuditOptions{Deep: true},
	)
	if err != nil {
		t.Fatalf("audit integrity: %v", err)
	}
	if report.Status != "ok" || len(report.Issues) != 0 {
		t.Fatalf("audit report = %#v", report)
	}
	if hasIntegrityCheck(report.Checks, "entity_record") {
		t.Fatalf("unexpected entity record check: %#v", report.Checks)
	}
}

func TestIntegrityAuditReportsCorruptSnapshotPage(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	seedIntegrityTenant(t, ctx, store)
	catalog, _, err := store.CurrentShardedSnapshotCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("snapshot catalog: %v", err)
	}
	if len(catalog.EntityPages) == 0 {
		t.Fatal("snapshot catalog has no entity pages")
	}
	if err := store.Objects.Put(ctx, catalog.EntityPages[0].Key, []byte("not parquet")); err != nil {
		t.Fatalf("corrupt entity page: %v", err)
	}

	report, err := store.AuditIntegrity(ctx, "tenant-a", IntegrityAuditOptions{Deep: true})
	if err != nil {
		t.Fatalf("audit integrity: %v", err)
	}
	if report.Status != "error" || !hasIntegrityIssue(report.Issues, "snapshot_entity_page_decode_failed") {
		t.Fatalf("audit report = %#v", report)
	}
}

func TestRepairPlanAndVerification(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	seedIntegrityTenant(t, ctx, store)
	if err := store.Objects.Delete(ctx, store.indexCatalogKey("tenant-a")); err != nil {
		t.Fatalf("delete index catalog: %v", err)
	}

	dryRun, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{})
	if err != nil {
		t.Fatalf("repair dry run: %v", err)
	}
	if !hasRepairPlanStep(dryRun.Plan, "rebuild_indexes") {
		t.Fatalf("repair plan = %#v", dryRun.Plan)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if !hasRepairAction(applied.Actions, "rebuild_indexes") {
		t.Fatalf("actions = %#v", applied.Actions)
	}
	if applied.Verification == nil || applied.Verification.Status != "ok" {
		t.Fatalf("verification = %#v", applied.Verification)
	}
}

func seedIntegrityTenant(t *testing.T, ctx context.Context, store *TenantStore) {
	t.Helper()
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
}

func hasIntegrityCheck(checks []IntegrityObjectCheck, role string) bool {
	for _, check := range checks {
		if check.Role == role {
			return true
		}
	}
	return false
}

func hasIntegrityIssue(issues []IntegrityAuditIssue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func hasRepairPlanStep(plan []RepairPlanStep, actionType string) bool {
	for _, step := range plan {
		if step.Type == actionType {
			return true
		}
	}
	return false
}
