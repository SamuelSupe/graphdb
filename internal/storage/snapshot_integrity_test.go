package storage

import (
	"context"
	"testing"
)

func TestIndexHealthReportsSnapshotEntityPageContentHashMismatch(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	catalog, _, err := store.CurrentShardedSnapshotCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("snapshot catalog: %v", err)
	}
	spec := requireSnapshotEntityPageSpec(t, catalog)
	page, err := store.loadSnapshotEntityPage(ctx, "tenant-a", catalog.Version, spec)
	if err != nil {
		t.Fatalf("load snapshot entity page: %v", err)
	}
	if len(page.Entities) == 0 {
		t.Fatal("snapshot page has no entities")
	}
	page.Entities[0].Kind = "corrupt"
	writeParquetEntityPageForTest(t, ctx, store, spec.Key, page)

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	want := "snapshot entity page " + spec.Shard + " content hash mismatch"
	if health.Status != "error" || !healthIssueContains(health, want) {
		t.Fatalf("health = %#v, want issue %q", health, want)
	}
}

func TestIndexHealthReportsMissingSnapshotSchema(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	catalog, _, err := store.CurrentShardedSnapshotCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("snapshot catalog: %v", err)
	}
	if err := store.Objects.Delete(ctx, catalog.Schema.Key); err != nil {
		t.Fatalf("delete schema: %v", err)
	}

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "snapshot schema is missing") {
		t.Fatalf("health = %#v", health)
	}
}

func requireSnapshotEntityPageSpec(t *testing.T, catalog ShardedSnapshotCatalog) SnapshotEntityPageSpec {
	t.Helper()
	for _, spec := range catalog.EntityPages {
		if spec.EntityCount > 0 {
			return spec
		}
	}
	t.Fatalf("missing non-empty snapshot entity page in %#v", catalog.EntityPages)
	return SnapshotEntityPageSpec{}
}
