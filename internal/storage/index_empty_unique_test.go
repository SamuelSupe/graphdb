package storage

import (
	"context"
	"errors"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPersistedEmptyUniqueIndexIsAvailable(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", uniqueIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	assertEmptyUniqueIndexAvailable(t, ctx, store, catalog)
}

func TestPersistedUniqueIndexRemainsAvailableAfterLastEntryDeleted(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	mutations := uniqueIndexMutations()
	mutations.UpsertEntities = []graph.Entity{{
		ID:     "host:app-01",
		Kind:   "host",
		Fields: graph.Fields{"hostname": "app-01"},
	}}
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}

	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		DeleteEntities: []string{"host:app-01"},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("delete last indexed entity: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get index catalog: %v", err)
	}
	assertEmptyUniqueIndexAvailable(t, ctx, store, catalog)
}

func TestGCDeletesObsoleteEmptyUniqueIndexObject(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", uniqueIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	initial, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	oldKey := requireAnyIndexObjectKey(
		t,
		requireFieldIndexSpec(t, initial, "host", "hostname").Objects,
	)

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("remove unique field: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("replacement rebuild: %v", err)
	}
	if _, err := store.Objects.Get(ctx, oldKey); err != nil {
		t.Fatalf("obsolete empty index should remain until GC: %v", err)
	}

	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{
		KeepSnapshots:       1,
		CleanupIndexOrphans: true,
	}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if _, err := store.Objects.Get(ctx, oldKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("obsolete empty unique index err=%v, want ErrNotFound", err)
	}
}

func TestParquetSecondaryIndexIdentityFromKey(t *testing.T) {
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	const (
		kind  = "host/type"
		field = "serial number/序号"
	)
	keys := []string{
		store.parquetSecondaryIndexVersionKey("tenant-a", 7, kind, field),
		store.parquetSecondaryIndexShardVersionKey("tenant-a", 7, kind, field, "s:00"),
	}
	for _, key := range keys {
		gotKind, gotField, ok := store.parquetSecondaryIndexIdentityFromKey("tenant-a", key)
		if !ok || gotKind != kind || gotField != field {
			t.Fatalf("identity from %q = (%q, %q, %v)", key, gotKind, gotField, ok)
		}
	}
	if _, _, ok := store.parquetSecondaryIndexIdentityFromKey(
		"tenant-a",
		store.parquetEdgeShardVersionKey("tenant-a", 7, "runs_on", "00"),
	); ok {
		t.Fatal("edge shard key parsed as a secondary index")
	}
}

func uniqueIndexMutations() graph.Mutations {
	return graph.Mutations{UpsertCITypes: []graph.CIType{{
		Name: "host",
		Fields: map[string]graph.FieldSpec{
			"hostname": {Type: "string", Unique: true},
		},
	}}}
}

func assertEmptyUniqueIndexAvailable(t *testing.T, ctx context.Context, store *TenantStore, catalog IndexCatalog) {
	t.Helper()
	spec := requireFieldIndexSpec(t, catalog, "host", "hostname")
	if spec.Type != "unique-field" || spec.EntryCount != 0 || spec.DistinctValues != 0 {
		t.Fatalf("unique index spec = %#v", spec)
	}

	lookup := &PersistedIndexLookup{
		Store:    store,
		TenantID: "tenant-a",
		Version:  catalog.Version,
		Catalog:  catalog,
	}
	ids, available, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"missing"})
	if err != nil || !available || len(ids) != 0 {
		t.Fatalf("empty unique lookup ids=%#v available=%v err=%v", ids, available, err)
	}
	values, available, err := lookup.ScanFieldIndex(ctx, "host", "hostname")
	if err != nil || !available || len(values) != 0 {
		t.Fatalf("empty unique scan values=%#v available=%v err=%v", values, available, err)
	}

	report := IntegrityAuditReport{}
	store.auditSecondaryIndexObjects(ctx, "tenant-a", catalog.Version, spec, &report)
	if len(report.Issues) != 0 {
		t.Fatalf("empty unique index audit issues = %#v", report.Issues)
	}

	inspection, err := store.InspectIndex(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("inspect empty unique index: %v", err)
	}
	key := requireAnyIndexObjectKey(t, spec.Objects)
	for _, object := range inspection.Objects {
		if object.Key != key {
			continue
		}
		if object.RowCount != 0 || !object.HashMatches || object.InspectionError != "" {
			t.Fatalf("empty unique index inspection = %#v", object)
		}
		return
	}
	t.Fatalf("empty unique index %q missing from inspection", key)
}
