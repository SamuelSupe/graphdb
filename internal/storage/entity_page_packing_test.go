package storage

import (
	"context"
	"testing"

	"graphdb/internal/graph"
)

func TestEntityRecordModeUsesLogicalEntityPageObjects(t *testing.T) {
	pages := []EntityPageSpec{
		{Shard: "00", EntityCount: 10, ContentHash: "hash-00"},
		{Shard: "01", EntityCount: 10, ContentHash: "hash-01"},
		{Shard: "02", EntityCount: 10, ContentHash: "hash-02"},
	}

	store := NewTenantStore(NewMemoryStore(), "test")
	store.WriteEntityRecords = true
	catalog := IndexCatalog{Version: 1, EntityPages: append([]EntityPageSpec(nil), pages...)}
	store.decorateIndexCatalog(&catalog, "tenant-a", IndexFormatParquet)
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
	store.decorateIndexCatalog(&packed, "tenant-a", IndexFormatParquet)
	packedKeys := map[string]struct{}{}
	for _, page := range packed.EntityPages {
		packedKeys[firstIndexObjectKey(page.Objects, "page", "")] = struct{}{}
	}
	if len(packedKeys) >= len(packed.EntityPages) {
		t.Fatalf("default mode did not pack small entity pages: %d keys for %d pages", len(packedKeys), len(packed.EntityPages))
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
