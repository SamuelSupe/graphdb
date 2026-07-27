package storage

import (
	"context"
	"testing"
)

func TestPostgresTenantRegistryAddIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	key := store.tenantRegistryKey()
	putTenantRegistryFixture(
		t,
		ctx,
		objects,
		key,
		[]string{"tenant-a"},
	)
	if tenants, _, err := store.getTenantRegistry(
		ctx,
	); err != nil || len(tenants) != 1 {
		t.Fatalf("prime tenant registry = %v, err %v", tenants, err)
	}

	putTenantRegistryFixture(t, ctx, objects, key, nil)
	if err := store.addTenantToRegistry(ctx, "tenant-a"); err != nil {
		t.Fatalf("re-add tenant to registry: %v", err)
	}
	verifier := NewTenantStore(objects, "test")
	tenants, _, err := verifier.getTenantRegistry(ctx)
	if err != nil {
		t.Fatalf("get updated tenant registry: %v", err)
	}
	if len(tenants) != 1 || tenants[0] != "tenant-a" {
		t.Fatalf("updated tenant registry = %v", tenants)
	}
}

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
