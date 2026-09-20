package storage

import (
	"context"
	"testing"
)

func putTenantRegistryFixture(
	t *testing.T,
	ctx context.Context,
	objects ObjectStore,
	key string,
	tenantIDs []string,
) {
	t.Helper()
	data, err := marshalParquetTenantRegistry(ctx, tenantRegistry{
		TenantIDs: tenantIDs,
	})
	if err != nil {
		t.Fatalf("marshal tenant registry: %v", err)
	}
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put tenant registry: %v", err)
	}
}
