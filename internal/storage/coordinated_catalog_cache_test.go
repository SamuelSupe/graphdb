package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestPostgresIndexDefinitionsIgnoreStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	now := time.Now().UTC()
	putIndexDefinitionsFixture(t, ctx, store, objects, []IndexDefinition{{
		Name: "host.name", Kind: "host", Field: "name",
		CreatedAt: now, UpdatedAt: now,
	}})
	if definitions, err := store.ListIndexDefinitions(
		ctx,
		"tenant-a",
	); err != nil || len(definitions) != 1 {
		t.Fatalf("prime index definitions = %#v, err %v", definitions, err)
	}

	putIndexDefinitionsFixture(t, ctx, store, objects, []IndexDefinition{
		{
			Name: "host.name", Kind: "host", Field: "name",
			CreatedAt: now, UpdatedAt: now,
		},
		{
			Name: "service.name", Kind: "service", Field: "name",
			CreatedAt: now, UpdatedAt: now,
		},
	})
	definitions, err := store.ListIndexDefinitions(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list updated index definitions: %v", err)
	}
	if len(definitions) != 2 {
		t.Fatalf("updated index definitions = %#v", definitions)
	}
}

func TestPostgresIndexCatalogIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	store.LifecycleCacheTTL = time.Millisecond
	putIndexCatalogCacheFixture(t, ctx, store, objects, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
	})
	if catalog, err := store.GetIndexCatalogAtVersion(
		ctx,
		"tenant-a",
		1,
	); err != nil || len(catalog.Indexes) != 0 {
		t.Fatalf("prime index catalog = %#v, err %v", catalog, err)
	}

	putIndexCatalogCacheFixture(t, ctx, store, objects, IndexCatalog{
		TenantID: "tenant-a",
		Version:  1,
		Indexes: []IndexSpec{{
			Name: "host.name", Kind: "host", Field: "name",
			Type: "string", Status: "ready",
		}},
	})
	time.Sleep(2 * time.Millisecond)
	catalog, err := store.GetIndexCatalogAtVersion(ctx, "tenant-a", 1)
	if err != nil {
		t.Fatalf("get updated index catalog: %v", err)
	}
	if len(catalog.Indexes) != 1 {
		t.Fatalf("updated index catalog = %#v", catalog)
	}
}

func TestPostgresReverseCatalogIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	store.LifecycleCacheTTL = time.Millisecond
	putReverseCatalogFixture(t, ctx, store, objects, 1)
	if catalog, err := store.GetReverseIndexCatalog(
		ctx,
		"tenant-a",
		0,
	); err != nil || catalog.Version != 1 {
		t.Fatalf("prime reverse catalog = %#v, err %v", catalog, err)
	}

	putReverseCatalogFixture(t, ctx, store, objects, 2)
	time.Sleep(2 * time.Millisecond)
	catalog, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 0)
	if err != nil {
		t.Fatalf("get updated reverse catalog: %v", err)
	}
	if catalog.Version != 2 {
		t.Fatalf("updated reverse catalog = %#v", catalog)
	}
}

func newCoordinatedCachedStore() (*TenantStore, *MemoryStore) {
	objects := NewMemoryStore()
	store := NewTenantStore(
		NewWriterObjectCache(objects, WriterObjectCacheConfig{
			MaxBytes:    1 << 20,
			MaxKeys:     100,
			NegativeTTL: time.Hour,
		}),
		"test",
	)
	store.SetCoordinator(newTaskLeaseTestCoordinator())
	return store, objects
}

func putIndexDefinitionsFixture(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	objects ObjectStore,
	definitions []IndexDefinition,
) {
	t.Helper()
	data, err := marshalParquetIndexDefinitions(ctx, IndexDefinitionRecord{
		TenantID: "tenant-a",
		Indexes:  definitions,
	})
	if err != nil {
		t.Fatalf("marshal index definitions: %v", err)
	}
	if err := objects.Put(
		ctx,
		store.indexDefinitionsKey("tenant-a"),
		data,
	); err != nil {
		t.Fatalf("put index definitions: %v", err)
	}
}

func putReverseCatalogFixture(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	objects ObjectStore,
	version int64,
) {
	t.Helper()
	data, err := json.Marshal(ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      "tenant-a",
		Version:       version,
		UpdatedAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal reverse catalog: %v", err)
	}
	if err := objects.Put(
		ctx,
		store.reverseIndexCatalogKey("tenant-a"),
		data,
	); err != nil {
		t.Fatalf("put reverse catalog: %v", err)
	}
}
