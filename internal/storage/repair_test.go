package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"graphdb/internal/graph"
)

func TestJSONManifestIsRejectedAndRepairRebuildsParquet(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	manifest, _, err := store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	putRawJSON(t, store, store.manifestKey("tenant-a"), map[string]any{
		"layout_version":   CurrentObjectLayoutVersion,
		"tenant_id":        manifest.TenantID,
		"version":          manifest.Version,
		"head_commit_id":   manifest.HeadCommitID,
		"commit_keys":      manifest.CommitKeys,
		"snapshot_key":     manifest.SnapshotKey,
		"snapshot_version": manifest.SnapshotVersion,
		"updated_at":       manifest.UpdatedAt,
	})
	store.deleteWriteCache("tenant-a")

	if _, _, err = store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "only parquet manifests") {
		t.Fatalf("load err = %v, want parquet-only manifest rejection", err)
	}
	dryRun, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{})
	if err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if !hasRepairIssue(dryRun.Issues, "manifest_unreadable") {
		t.Fatalf("dry-run issues = %#v, want manifest_unreadable", dryRun.Issues)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if !hasRepairAction(applied.Actions, "rebuild_manifest") {
		t.Fatalf("repair actions = %#v, want rebuild_manifest", applied.Actions)
	}
	if len(applied.RemainingIssues) != 0 {
		t.Fatalf("remaining issues = %#v, want none", applied.RemainingIssues)
	}
	manifest, _, err = store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("manifest layout = %d, want %d", manifest.LayoutVersion, CurrentObjectLayoutVersion)
	}
}

func TestLoadRejectsNonParquetManifest(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	putRawJSON(t, store, store.manifestKey("tenant-a"), map[string]any{
		"layout_version": CurrentObjectLayoutVersion,
		"tenant_id":      "tenant-a",
	})
	_, _, err := store.Load(ctx, "tenant-a")
	if err == nil || !strings.Contains(err.Error(), "only parquet manifests") {
		t.Fatalf("load err = %v, want parquet-only manifest rejection", err)
	}
}

func TestNewObjectsStampCurrentLayout(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}},
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	manifest, _, err := store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("manifest layout = %d", manifest.LayoutVersion)
	}
	commit, err := store.getCommitObject(ctx, manifest.CommitKeys[0])
	if err != nil {
		t.Fatalf("commit object: %v", err)
	}
	if commit.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("commit layout = %d", commit.LayoutVersion)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if catalog.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("catalog layout = %d", catalog.LayoutVersion)
	}
	index, _ := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	if index.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("index layout = %d", index.LayoutVersion)
	}
}

func TestRepairRebuildsMissingIndexCatalog(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
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
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := store.Objects.Delete(ctx, store.indexCatalogKey("tenant-a")); err != nil {
		t.Fatalf("delete parquet catalog: %v", err)
	}

	dryRun, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{})
	if err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if !hasRepairIssue(dryRun.Issues, "index_health_issue") {
		t.Fatalf("dry-run issues = %#v, want missing index health", dryRun.Issues)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if len(applied.RemainingIssues) != 0 {
		t.Fatalf("remaining issues = %#v, want none", applied.RemainingIssues)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if catalog.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("catalog layout = %d", catalog.LayoutVersion)
	}
	index, _ := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	if index.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("index layout = %d", index.LayoutVersion)
	}
}

func TestRepairReportsGraphConsistencyIssues(t *testing.T) {
	g := graph.New()
	g.CITypes["host"] = graph.CIType{Name: "host", IdentityKeys: []graph.IdentityKey{{Name: "hostname", Fields: []string{"hostname"}}}}
	g.RelationTypes["runs_on"] = graph.RelationType{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}
	g.Entities["host:a"] = graph.Entity{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "same"},
		Sources:         []graph.EntitySource{{Source: "agent", ExternalID: "a", Stale: true}},
		FieldSources:    map[string]graph.FieldSource{"hostname": {Source: "agent"}},
		ExistenceSource: &graph.FieldSource{Source: "agent"},
		MergedFrom:      []string{"legacy:shared"},
	}
	g.Entities["host:b"] = graph.Entity{ID: "host:b", Kind: "host", Fields: graph.Fields{"hostname": "same"}, MergedFrom: []string{"legacy:shared"}}
	g.Edges["edge:bad"] = graph.Edge{ID: "edge:bad", Type: "runs_on", From: "host:a", To: "missing"}
	g.Edges["edge:kind"] = graph.Edge{ID: "edge:kind", Type: "runs_on", From: "host:a", To: "host:b"}

	issues := graphConsistencyIssues(g)
	for _, code := range []string{
		"alias_conflict",
		"duplicate_ci_identity",
		"orphan_edge_to",
		"relation_endpoint_mismatch",
		"stale_source_owns_entity_existence",
		"stale_source_owns_entity_field",
	} {
		if !hasRepairIssue(issues, code) {
			t.Fatalf("issues missing %s: %#v", code, issues)
		}
	}
}

func TestRepairApplyRebuildsMissingIndexCatalog(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
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
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := store.Objects.Delete(ctx, store.indexCatalogKey("tenant-a")); err != nil {
		t.Fatalf("delete catalog: %v", err)
	}
	report, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if !hasRepairAction(report.Actions, "rebuild_indexes") {
		t.Fatalf("actions = %#v, want rebuild_indexes", report.Actions)
	}
	if len(report.RemainingIssues) != 0 {
		t.Fatalf("remaining issues = %#v, want none", report.RemainingIssues)
	}
}

func TestRepairRebuildsTenantMetadataAndRegistry(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := store.Objects.Delete(ctx, store.tenantRegistryKey()); err != nil {
		t.Fatalf("delete registry: %v", err)
	}
	dryRun, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{})
	if err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if !hasRepairIssue(dryRun.Issues, "tenant_metadata_missing") || !hasRepairIssue(dryRun.Issues, "tenant_registry_missing") {
		t.Fatalf("issues = %#v, want metadata and registry repair issues", dryRun.Issues)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if !hasRepairAction(applied.Actions, "rebuild_tenant_metadata") || !hasRepairAction(applied.Actions, "rebuild_tenant_registry") {
		t.Fatalf("actions = %#v, want metadata and registry rebuild", applied.Actions)
	}
	if len(applied.RemainingIssues) != 0 {
		t.Fatalf("remaining issues = %#v, want none", applied.RemainingIssues)
	}
	if _, configured, _, err := store.getTenantMetadataWithMeta(ctx, "tenant-a"); err != nil || !configured {
		t.Fatalf("metadata configured=%v err=%v", configured, err)
	}
	tenants, err := store.ListManagedTenants(ctx)
	if err != nil || !stringSliceContains(tenants, "tenant-a") {
		t.Fatalf("registry tenants=%#v err=%v, want tenant-a", tenants, err)
	}
}

func TestRepairRebuildsCorruptManifestFromCommitObjects(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := store.Objects.Put(ctx, store.manifestKey("tenant-a"), []byte("{bad manifest")); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}
	dryRun, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{})
	if err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if !hasRepairIssue(dryRun.Issues, "manifest_unreadable") {
		t.Fatalf("issues = %#v, want manifest_unreadable", dryRun.Issues)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if !hasRepairAction(applied.Actions, "rebuild_manifest") {
		t.Fatalf("actions = %#v, want rebuild_manifest", applied.Actions)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load repaired tenant: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatalf("repaired graph missing entity")
	}
}

func TestRepairRebuildsCorruptManifestFromCommitSegment(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	before, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(before.CommitSegments) != 1 || len(before.CommitKeys) != 0 {
		t.Fatalf("tail before gc segments=%#v keys=%#v", before.CommitSegments, before.CommitKeys)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if err := store.Objects.Put(ctx, store.manifestKey("tenant-a"), []byte("{bad manifest")); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if !hasRepairAction(applied.Actions, "rebuild_manifest") {
		t.Fatalf("actions = %#v, want rebuild_manifest", applied.Actions)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load repaired tenant: %v", err)
	}
	if manifest.Version != int64(commitSegmentTargetCount) {
		t.Fatalf("repaired manifest = %#v", manifest)
	}
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, ok := g.GetEntity(fmt.Sprintf("host:%03d", i)); !ok {
			t.Fatalf("repaired graph missing host:%03d", i)
		}
	}
}

func TestRepairRebuildsBrokenEntityPageAndRecordIndexes(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if len(catalog.EntityPages) == 0 {
		t.Fatalf("catalog missing entity pages: %#v", catalog)
	}
	pageKey := firstIndexObjectKey(catalog.EntityPages[0].Objects, "page", store.parquetEntityPageVersionKey("tenant-a", catalog.Version, catalog.EntityPages[0].Shard))
	if err := store.Objects.Delete(ctx, pageKey); err != nil {
		t.Fatalf("delete entity page: %v", err)
	}
	if err := store.Objects.Delete(ctx, store.entityRecordKey("tenant-a", "host:a")); err != nil {
		t.Fatalf("delete entity record: %v", err)
	}
	dryRun, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{})
	if err != nil {
		t.Fatalf("repair dry-run: %v", err)
	}
	if !hasRepairIssue(dryRun.Issues, "index_health_issue") {
		t.Fatalf("issues = %#v, want index health issue", dryRun.Issues)
	}
	applied, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true})
	if err != nil {
		t.Fatalf("repair apply: %v", err)
	}
	if !hasRepairAction(applied.Actions, "rebuild_indexes") {
		t.Fatalf("actions = %#v, want rebuild_indexes", applied.Actions)
	}
	if len(applied.RemainingIssues) != 0 {
		t.Fatalf("remaining issues = %#v, want none", applied.RemainingIssues)
	}
}

func putRawJSON(t *testing.T, store *TenantStore, key string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal raw JSON: %v", err)
	}
	if err := store.Objects.Put(context.Background(), key, data); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func stripLayoutVersion(t *testing.T, store *TenantStore, key string) {
	t.Helper()
	data, err := store.Objects.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatalf("unmarshal %s: %v", key, err)
	}
	delete(object, "layout_version")
	putRawJSON(t, store, key, object)
}

func hasRepairIssue(issues []RepairIssue, code string) bool {
	for _, issue := range issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func hasRepairAction(actions []RepairAction, actionType string) bool {
	for _, action := range actions {
		if action.Type == actionType && action.Status == "applied" {
			return true
		}
	}
	return false
}
