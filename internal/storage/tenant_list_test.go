package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
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

func TestInitTenantReportsRegistryFailure(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(
		tenantRegistryFailStore{ObjectStore: NewMemoryStore()},
		"test",
	)

	if _, err := store.InitTenant(ctx, "tenant-a"); !errors.Is(err, errTenantRegistryUnavailable) {
		t.Fatalf("InitTenant error = %v, want %v", err, errTenantRegistryUnavailable)
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

var errTenantRegistryUnavailable = errors.New("tenant registry unavailable")

type tenantRegistryFailStore struct {
	ObjectStore
}

func (s tenantRegistryFailStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if strings.HasSuffix(key, "/_registry.parquet") {
		return nil, ObjectMeta{Key: key}, errTenantRegistryUnavailable
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
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

type tenantRegistryReadStore struct {
	ObjectStore
	mu    sync.Mutex
	reads int
}

func (s *tenantRegistryReadStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	if strings.HasSuffix(key, "/_registry.parquet") {
		s.mu.Lock()
		s.reads++
		s.mu.Unlock()
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *tenantRegistryReadStore) registryReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

func TestTenantInfoPagesBoundDetailReadsAndResumeAfterDeletion(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	ids := make([]string, 10000)
	for i := range ids {
		ids[i] = fmt.Sprintf("tenant-%05d", i)
	}
	if err := store.putTenantRegistryWithMeta(ctx, tenantRegistry{TenantIDs: ids}, ObjectMeta{}); err != nil {
		t.Fatal(err)
	}
	for _, id := range append(append([]string{}, ids[:50]...), ids[9990:]...) {
		data, err := marshalParquetManifest(ctx, Manifest{TenantID: id})
		if err != nil {
			t.Fatal(err)
		}
		if err := base.Put(ctx, store.manifestKey(id), data); err != nil {
			t.Fatal(err)
		}
	}
	objects := &tenantPageReadStore{ObjectStore: noListTenantObjectStore{ObjectStore: base}}
	store.Objects = objects
	page, err := store.ListTenantInfoPage(ctx, TenantListOptions{Limit: 25})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tenants) != 25 || page.Total != 10000 || page.NextCursor != "tenant-00024" || objects.detailReads != 50 {
		t.Fatalf("page=%+v detail reads=%d", page, objects.detailReads)
	}
	// Removing earlier IDs must not shift the next page past unseen records.
	_, meta, err := base.GetWithMeta(ctx, store.tenantRegistryKey())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.putTenantRegistryWithMeta(ctx, tenantRegistry{TenantIDs: ids[25:]}, meta); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListTenantInfoPage(ctx, TenantListOptions{Limit: 25, Cursor: page.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if page.Tenants[0].TenantID != "tenant-00025" || page.Total != 9975 {
		t.Fatalf("resumed page=%+v", page)
	}
	page, err = store.ListTenantInfoPage(ctx, TenantListOptions{Limit: 25, Query: "TENANT-0999"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tenants) != 10 || page.Total != 10 || page.NextCursor != "" || page.Tenants[9].TenantID != "tenant-09999" {
		t.Fatalf("filtered page=%+v", page)
	}
	page, err = store.ListTenantInfoPage(ctx, TenantListOptions{Limit: 25, Query: "absent"})
	if err != nil || page.Tenants == nil || len(page.Tenants) != 0 || page.Total != 0 {
		t.Fatalf("empty page=%+v err=%v", page, err)
	}
}

type tenantPageReadStore struct {
	ObjectStore
	detailReads int
}

func (s *tenantPageReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if strings.HasSuffix(key, "/metadata.parquet") || strings.HasSuffix(key, "/manifest.parquet") {
		s.detailReads++
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}
