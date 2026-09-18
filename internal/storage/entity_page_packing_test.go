package storage

import (
	"context"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIncrementalPackedPagesRemainReadableAndBackedUp(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteEntityRecords = false
	store.EntityPagePackMaxBytes = 4096
	entities := []graph.Entity{
		{ID: "host:a", Kind: "host", Fields: graph.Fields{"payload": "small"}},
		{ID: "host:b", Kind: "host", Fields: graph.Fields{"payload": strings.Repeat("b", 8192)}},
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	for i := range entities {
		entities[i].Fields["revision"] = "updated"
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{})
	if err != nil || len(result.IndexWarnings) != 0 {
		t.Fatalf("incremental commit: %v, warnings: %v", err, result.IndexWarnings)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range catalog.EntityPages {
		for _, object := range page.Objects {
			if _, err := store.Objects.Get(ctx, object.Key); err != nil {
				t.Fatalf("published page %s is unreadable: %v", object.Key, err)
			}
		}
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	if backup.Status != TaskStatusSucceeded {
		t.Fatalf("backup failed: %#v", backup)
	}
	restore, err := store.StartTask(ctx, "tenant-b", TaskTypeTenantRestore, map[string]any{"backup_key": backup.ResultKey})
	if err != nil {
		t.Fatal(err)
	}
	restore = waitForTask(t, ctx, store, "tenant-b", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore failed: %#v", restore)
	}
	g, _, err := store.Load(ctx, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range entities {
		got, ok := g.GetEntity(want.ID)
		if !ok || got.Fields["payload"] != want.Fields["payload"] || got.Fields["revision"] != "updated" {
			t.Fatalf("restored entity %s differs", want.ID)
		}
	}
}

func TestEntityRecordModeUsesLogicalEntityPageObjects(t *testing.T) {
	pages := []EntityPageSpec{
		{Shard: "00", EntityCount: 10, ContentHash: "hash-00"},
		{Shard: "01", EntityCount: 10, ContentHash: "hash-01"},
		{Shard: "02", EntityCount: 10, ContentHash: "hash-02"},
	}

	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteEntityRecords = true
	catalog := IndexCatalog{Version: 1, EntityPages: append([]EntityPageSpec(nil), pages...)}
	store.decorateIndexCatalog(&catalog, "tenant-a")
	seen := map[string]struct{}{}
	for _, page := range catalog.EntityPages {
		key := firstIndexObjectKey(page.Objects, "page", "")
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("entity-record mode reused packed page key %q", key)
		}
		seen[key] = struct{}{}
	}

	store.WriteEntityRecords = false
	packed := IndexCatalog{Version: 1, EntityPages: append([]EntityPageSpec(nil), pages...)}
	store.decorateIndexCatalog(&packed, "tenant-a")
	packedKeys := map[string]struct{}{}
	for _, page := range packed.EntityPages {
		packedKeys[firstIndexObjectKey(page.Objects, "page", "")] = struct{}{}
	}
	if len(packedKeys) >= len(packed.EntityPages) {
		t.Fatalf("default mode did not pack small entity pages: %d keys for %d pages", len(packedKeys), len(packed.EntityPages))
	}
}

func TestEntityPagePackingRespectsByteBudget(t *testing.T) {
	pages := []EntityPageData{
		{Shard: "00", Entities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"payload": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}},
		{Shard: "01", Entities: []graph.Entity{{ID: "host:b", Kind: "host", Fields: graph.Fields{"payload": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}}},
	}
	firstBytes := estimateEntityPageBytes(pages[0])
	secondBytes := estimateEntityPageBytes(pages[1])
	budget := firstBytes + secondBytes - 1

	groups := entityPageDataPackGroups(pages, true, budget)
	if len(groups) != 2 {
		t.Fatalf("byte-limited entity page groups = %d, want 2", len(groups))
	}
	specs := []EntityPageSpec{
		{Shard: pages[0].Shard, EntityCount: len(pages[0].Entities), estimatedBytes: firstBytes},
		{Shard: pages[1].Shard, EntityCount: len(pages[1].Entities), estimatedBytes: secondBytes},
	}
	packIDs := entityPagePackIDs(specs, true, budget)
	if packIDs["entities\x00"+pages[0].Shard] == packIDs["entities\x00"+pages[1].Shard] {
		t.Fatalf("byte-limited entity page specs shared pack %q", packIDs["entities\x00"+pages[0].Shard])
	}
}

func TestEntityPageWriteDoesNotRetainDecodedWriterCopy(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	page := EntityPageData{
		LayoutVersion: CurrentObjectLayoutVersion,
		Shard:         "00",
		Version:       1,
		Entities:      []graph.Entity{{ID: "host:a", Kind: "host"}},
	}
	if _, err := store.putParquetEntityPage(context.Background(), "tenant-a", page); err != nil {
		t.Fatalf("put entity page: %v", err)
	}
	store.entityPageCache.mu.Lock()
	cached := len(store.entityPageCache.data)
	store.entityPageCache.mu.Unlock()
	if cached != 0 {
		t.Fatalf("writer-populated decoded pages = %d, want 0", cached)
	}
}
