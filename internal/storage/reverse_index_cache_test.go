package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestReverseIndexCatalogCacheRevalidatesAfterTTL(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = time.Minute
	key := store.reverseIndexCatalogKey("tenant-a")
	put := func(version int64) {
		t.Helper()
		data, err := json.Marshal(ReverseIndexCatalog{
			LayoutVersion: reverseIndexLayoutVersion,
			TenantID:      "tenant-a",
			Version:       version,
			UpdatedAt:     time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("marshal catalog: %v", err)
		}
		if err := base.Put(ctx, key, data); err != nil {
			t.Fatalf("put catalog: %v", err)
		}
	}

	put(1)
	first, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 0)
	if err != nil || first.Version != 1 {
		t.Fatalf("first catalog = %#v, err %v", first, err)
	}
	put(2)
	cached, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 0)
	if err != nil || cached.Version != 1 {
		t.Fatalf("cached catalog = %#v, err %v", cached, err)
	}
	if reads := objects.countContains("/reverse-index/catalog.json"); reads != 1 {
		t.Fatalf("catalog reads before TTL = %d, want 1", reads)
	}

	store.lockMu.Lock()
	expired := store.reverseIndexCatalogCache["tenant-a"]
	expired.checked = time.Now().Add(-2 * time.Minute)
	store.reverseIndexCatalogCache["tenant-a"] = expired
	store.lockMu.Unlock()
	refreshed, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 0)
	if err != nil || refreshed.Version != 2 {
		t.Fatalf("refreshed catalog = %#v, err %v", refreshed, err)
	}
	if reads := objects.countContains("/reverse-index/catalog.json"); reads != 2 {
		t.Fatalf("catalog reads after TTL = %d, want 2", reads)
	}
}

func TestReverseIndexCatalogVersionMismatchCachesCurrentVersion(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = time.Minute
	key := store.reverseIndexCatalogKey("tenant-a")
	data, err := json.Marshal(ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      "tenant-a",
		Version:       2,
		UpdatedAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := base.Put(ctx, key, data); err != nil {
		t.Fatalf("put catalog: %v", err)
	}

	for range 2 {
		if _, err := store.GetReverseIndexCatalog(
			ctx, "tenant-a", 1,
		); !errors.Is(err, ErrNotFound) {
			t.Fatalf("stale catalog error = %v, want ErrNotFound", err)
		}
	}
	current, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 2)
	if err != nil || current.Version != 2 {
		t.Fatalf("current catalog = %#v, err %v", current, err)
	}
	if reads := objects.countContains("/reverse-index/catalog.json"); reads != 1 {
		t.Fatalf("catalog reads = %d, want one cached read", reads)
	}
}
