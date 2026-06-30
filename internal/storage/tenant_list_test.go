package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"graphdb/internal/graph"
)

func TestListTenantsDiscoversTenantPrefixes(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-b", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit tenant-b: %v", err)
	}
	if _, err := store.PutTenantConfig(ctx, "tenant-a", TenantConfig{}); err != nil {
		t.Fatalf("put config tenant-a: %v", err)
	}
	if err := store.Objects.Put(ctx, "test/tenants/../manifest.parquet", []byte("{}")); err != nil {
		t.Fatalf("put invalid tenant object: %v", err)
	}
	tenants, err := store.ListTenants(ctx)
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if want := []string{"tenant-a", "tenant-b"}; !reflect.DeepEqual(tenants, want) {
		t.Fatalf("tenants = %#v, want %#v", tenants, want)
	}
}

func TestListTenantsUsesRegistryWhenPresent(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	store.Objects = noListTenantObjectStore{ObjectStore: base}
	tenants, err := store.ListTenants(ctx)
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if want := []string{"tenant-a"}; !reflect.DeepEqual(tenants, want) {
		t.Fatalf("tenants = %#v, want %#v", tenants, want)
	}
}

func TestCommitRegistersManagedTenant(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	store.Objects = noListTenantObjectStore{ObjectStore: base}
	tenants, err := store.ListManagedTenants(ctx)
	if err != nil {
		t.Fatalf("ListManagedTenants: %v", err)
	}
	if want := []string{"tenant-a"}; !reflect.DeepEqual(tenants, want) {
		t.Fatalf("tenants = %#v, want %#v", tenants, want)
	}
}

func TestListManagedTenantsDoesNotScanLegacyPrefixes(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.Objects = noListTenantObjectStore{ObjectStore: base}
	tenants, err := store.ListManagedTenants(ctx)
	if err != nil {
		t.Fatalf("ListManagedTenants: %v", err)
	}
	if len(tenants) != 0 {
		t.Fatalf("tenants = %#v, want empty", tenants)
	}
}

func TestListTenantsIncludingLegacyMergesRegistryAndPrefixes(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "managed-tenant", TenantCreateOptions{}); err != nil {
		t.Fatalf("create managed tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "legacy-tenant", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:legacy", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit legacy tenant: %v", err)
	}
	tenants, err := store.ListTenantsIncludingLegacy(ctx)
	if err != nil {
		t.Fatalf("ListTenantsIncludingLegacy: %v", err)
	}
	if want := []string{"legacy-tenant", "managed-tenant"}; !reflect.DeepEqual(tenants, want) {
		t.Fatalf("tenants = %#v, want %#v", tenants, want)
	}
}

type noListTenantObjectStore struct {
	ObjectStore
}

func (s noListTenantObjectStore) List(context.Context, string) ([]ObjectInfo, error) {
	return nil, errors.New("list should not be used")
}
