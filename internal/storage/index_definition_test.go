package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCreateIndexBackfillsSchemalessFieldAndDropRemovesIt(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "platform"}},
		{ID: "host:2", Kind: "host", Fields: graph.Fields{"owner": "collector"}},
	}}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	result, err := store.CreateIndex(ctx, "tenant-a", IndexDefinition{Kind: "host", Field: "owner"})
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	definitionsKey := store.indexDefinitionsKey("tenant-a")
	definitionsData, err := store.Objects.Get(ctx, definitionsKey)
	if err != nil {
		t.Fatalf("get index definitions: %v", err)
	}
	if !isParquetBytes(definitionsData) {
		t.Fatalf("index definitions key=%q is not parquet", definitionsKey)
	}
	if result.Task.ID == "" || result.Task.Status != "running" || result.Task.ProgressTotal == 0 {
		t.Fatalf("task = %#v", result.Task)
	}
	catalog := waitIndexCatalog(t, store, "tenant-a", 1)
	if len(catalog.Indexes) != 1 || catalog.Indexes[0].Name != "host.owner" {
		t.Fatalf("catalog = %#v", catalog)
	}
	droppedKey := requireAnyIndexObjectKey(t, catalog.Indexes[0].Objects)
	lookup := PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "owner", []any{"platform"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:1" {
		t.Fatalf("ids=%#v ok=%v err=%v", ids, ok, err)
	}
	dropped, err := store.DropIndex(ctx, "tenant-a", "host.owner")
	if err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if dropped.Task.ID == "" {
		t.Fatalf("drop result = %#v", dropped)
	}
	catalog = waitIndexCatalogIndexes(t, store, "tenant-a", 0)
	if len(catalog.Indexes) != 0 {
		t.Fatalf("catalog after drop = %#v", catalog)
	}
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if _, err := store.Objects.Get(ctx, droppedKey); err != nil {
		t.Fatalf("current-version index object needed by cursors was removed: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:3", Kind: "host", Fields: graph.Fields{"owner": "platform"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("advance graph version: %v", err)
	}
	waitIndexCatalog(t, store, "tenant-a", 2)
	if _, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1, CleanupIndexOrphans: true}); err != nil {
		t.Fatalf("gc after version advance: %v", err)
	}
	if _, err := store.Objects.Get(ctx, droppedKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old secondary index object after version advance = %v, want ErrNotFound", err)
	}
}

func TestDropIndexStartsNewRebuildWhenPreviousTaskRunning(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "host:1", Kind: "host", Fields: graph.Fields{"owner": "platform"}},
	}}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	now := time.Now().UTC()
	record := IndexDefinitionRecord{TenantID: "tenant-a", Indexes: []IndexDefinition{{
		Name:      "host.owner",
		Kind:      "host",
		Field:     "owner",
		CreatedAt: now,
		UpdatedAt: now,
	}}}
	if err := store.putIndexDefinitionsWithMeta(ctx, "tenant-a", record, ObjectMeta{Key: store.indexDefinitionsKey("tenant-a")}); err != nil {
		t.Fatalf("put index definitions: %v", err)
	}
	oldTask := IndexTask{
		ID:        "old-running",
		TenantID:  "tenant-a",
		Type:      "rebuild",
		Status:    "running",
		StartedAt: now,
		UpdatedAt: now,
	}
	store.taskMu.Lock()
	store.indexTasks["tenant-a"] = oldTask
	store.taskMu.Unlock()

	dropped, err := store.DropIndex(ctx, "tenant-a", "host.owner")
	if err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if dropped.Task.ID == oldTask.ID {
		t.Fatalf("drop reused stale rebuild task %q", oldTask.ID)
	}
	catalog := waitIndexCatalogIndexes(t, store, "tenant-a", 0)
	if len(catalog.Indexes) != 0 {
		t.Fatalf("catalog after drop = %#v", catalog)
	}
}

func TestIndexDefinitionsRejectNonParquetBytes(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	key := store.indexDefinitionsKey("tenant-a")
	if err := store.Objects.Put(ctx, key, []byte(`{"tenant_id":"tenant-a","indexes":[{"name":"host.owner","kind":"host","field":"owner"}]}`)); err != nil {
		t.Fatalf("put non-parquet definitions: %v", err)
	}
	if _, err := store.ListIndexDefinitions(ctx, "tenant-a"); err == nil {
		t.Fatal("non-parquet index definitions loaded unexpectedly")
	}
}

func waitIndexCatalog(t *testing.T, store *TenantStore, tenantID string, version int64) IndexCatalog {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		catalog, err := store.GetIndexCatalog(context.Background(), tenantID)
		if err == nil && catalog.Version == version {
			return catalog
		}
		if time.Now().After(deadline) {
			t.Fatalf("index catalog did not reach version %d, last err=%v catalog=%#v", version, err, catalog)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitIndexCatalogIndexes(t *testing.T, store *TenantStore, tenantID string, count int) IndexCatalog {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		catalog, err := store.GetIndexCatalog(context.Background(), tenantID)
		if err == nil && len(catalog.Indexes) == count {
			return catalog
		}
		if time.Now().After(deadline) {
			t.Fatalf("index catalog did not reach index count %d, last err=%v catalog=%#v", count, err, catalog)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
