package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

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
