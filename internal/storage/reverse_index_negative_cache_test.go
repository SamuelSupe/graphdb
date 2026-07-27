package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestReverseIndexCatalogNegativeCacheRevalidatesAfterTTL(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newPathCountingStore(base)
	store := NewTenantStore(objects, "test")
	store.LifecycleCacheTTL = time.Minute

	for range 2 {
		if _, err := store.GetReverseIndexCatalog(
			ctx, "tenant-a", 1,
		); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing catalog error = %v, want ErrNotFound", err)
		}
	}
	if reads := objects.countContains("/reverse-index/catalog.json"); reads != 1 {
		t.Fatalf("missing catalog reads = %d, want 1", reads)
	}

	data, err := json.Marshal(ReverseIndexCatalog{
		LayoutVersion: reverseIndexLayoutVersion,
		TenantID:      "tenant-a",
		Version:       1,
		UpdatedAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := base.Put(
		ctx, store.reverseIndexCatalogKey("tenant-a"), data,
	); err != nil {
		t.Fatalf("put catalog: %v", err)
	}
	store.lockMu.Lock()
	expired := store.reverseIndexCatalogCache["tenant-a"]
	expired.checked = time.Now().Add(-2 * time.Minute)
	store.reverseIndexCatalogCache["tenant-a"] = expired
	store.lockMu.Unlock()

	catalog, err := store.GetReverseIndexCatalog(ctx, "tenant-a", 1)
	if err != nil || catalog.Version != 1 {
		t.Fatalf("revalidated catalog = %#v, err %v", catalog, err)
	}
	if reads := objects.countContains("/reverse-index/catalog.json"); reads != 2 {
		t.Fatalf("revalidated catalog reads = %d, want 2", reads)
	}
}
