package storage

import (
	"context"
	"testing"
	"time"
)

func TestPostgresTenantMetadataIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	key := store.tenantMetadataKey("tenant-a")
	putTenantMetadataFixture(t, ctx, objects, key, TenantMetadata{
		TenantID:  "tenant-a",
		Status:    TenantStatusActive,
		Name:      "old",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
	if metadata, _, _, err := store.getTenantMetadataWithMeta(
		ctx,
		"tenant-a",
	); err != nil || metadata.Name != "old" {
		t.Fatalf("prime tenant metadata = %#v, err %v", metadata, err)
	}

	putTenantMetadataFixture(t, ctx, objects, key, TenantMetadata{
		TenantID:  "tenant-a",
		Status:    TenantStatusActive,
		Name:      "new",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
	metadata, _, _, err := store.getTenantMetadataWithMeta(
		ctx,
		"tenant-a",
	)
	if err != nil {
		t.Fatalf("get updated tenant metadata: %v", err)
	}
	if metadata.Name != "new" {
		t.Fatalf("updated tenant metadata = %#v", metadata)
	}
}

func putTenantMetadataFixture(
	t *testing.T,
	ctx context.Context,
	objects ObjectStore,
	key string,
	metadata TenantMetadata,
) {
	t.Helper()
	data, err := marshalParquetTenantMetadata(ctx, metadata)
	if err != nil {
		t.Fatalf("marshal tenant metadata: %v", err)
	}
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put tenant metadata: %v", err)
	}
}
