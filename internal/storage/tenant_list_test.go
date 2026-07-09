package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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

func TestTenantRegistryUpdateRetriesCASConflict(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &tenantRegistryConflictOnceStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	store.MaxRetries = 2
	if err := store.addTenantToRegistry(ctx, "tenant-a"); err != nil {
		t.Fatalf("addTenantToRegistry: %v", err)
	}
	if objects.conflictCount() != 1 {
		t.Fatalf("conflicts = %d, want 1", objects.conflictCount())
	}
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

type tenantRegistryConflictOnceStore struct {
	ObjectStore
	mu        sync.Mutex
	conflicts int
}

func (s *tenantRegistryConflictOnceStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.HasSuffix(key, "/_registry.parquet") {
		s.mu.Lock()
		if s.conflicts == 0 {
			s.conflicts++
			s.mu.Unlock()
			return ObjectMeta{Key: key}, ErrConflict
		}
		s.mu.Unlock()
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *tenantRegistryConflictOnceStore) conflictCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conflicts
}
