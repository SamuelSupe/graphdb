package storage

import (
	"context"
	"testing"
)

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
