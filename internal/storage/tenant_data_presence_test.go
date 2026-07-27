package storage

import (
	"context"
	"fmt"
	"testing"
)

type pagingOnlyStore struct {
	ObjectStore
	listCalls int
	pageCalls int
}

func (s *pagingOnlyStore) List(context.Context, string) ([]ObjectInfo, error) {
	s.listCalls++
	return nil, fmt.Errorf("unbounded list must not be used")
}

func (s *pagingOnlyStore) ListPage(
	ctx context.Context,
	prefix string,
	after string,
	limit int,
) ([]ObjectInfo, string, error) {
	s.pageCalls++
	return listObjectPage(ctx, s.ObjectStore, prefix, after, limit)
}

func TestTenantDataExistsUsesBoundedPages(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(NewSingleWriterObjectStore(paged), "test")
	prefix := store.tenantObjectPrefix("tenant-a")
	for i := 0; i < objectPrefixProbePageSize+1; i++ {
		key := fmt.Sprintf("%scoordination/item-%03d", prefix, i)
		if err := base.Put(ctx, key, nil); err != nil {
			t.Fatalf("put coordination object: %v", err)
		}
	}
	if err := base.Put(ctx, prefix+"snapshots/data.parquet", nil); err != nil {
		t.Fatalf("put data object: %v", err)
	}

	exists, err := store.tenantDataExists(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("tenant data exists: %v", err)
	}
	if !exists {
		t.Fatal("tenant data was not found")
	}
	if paged.listCalls != 0 || paged.pageCalls != 2 {
		t.Fatalf("list calls=%d page calls=%d, want 0 and 2", paged.listCalls, paged.pageCalls)
	}
}

func TestTenantDataExistsScansControlOnlyTenantWithoutUnboundedList(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(paged, "test")
	prefix := store.tenantObjectPrefix("tenant-a")
	for i := 0; i < objectPrefixProbePageSize+1; i++ {
		key := fmt.Sprintf("%stasks/task-%03d.parquet", prefix, i)
		if err := base.Put(ctx, key, nil); err != nil {
			t.Fatalf("put task object: %v", err)
		}
	}

	exists, err := store.tenantDataExists(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("tenant data exists: %v", err)
	}
	if exists {
		t.Fatal("task objects counted as tenant data")
	}
	if paged.listCalls != 0 || paged.pageCalls != 2 {
		t.Fatalf("list calls=%d page calls=%d, want 0 and 2", paged.listCalls, paged.pageCalls)
	}
}

func TestTenantGraphObjectsExistStopsAfterFirstBoundedPage(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(paged, "test")
	if err := base.Put(
		ctx,
		store.snapshotPrefix("tenant-a")+"00000000000000000001.parquet",
		nil,
	); err != nil {
		t.Fatalf("put snapshot object: %v", err)
	}

	exists, err := store.tenantGraphObjectsExist(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("tenant graph objects exist: %v", err)
	}
	if !exists {
		t.Fatal("snapshot object was not found")
	}
	if paged.listCalls != 0 || paged.pageCalls != 1 {
		t.Fatalf(
			"list calls=%d page calls=%d, want 0 and 1",
			paged.listCalls,
			paged.pageCalls,
		)
	}
}
