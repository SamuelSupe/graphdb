package storage

import (
	"context"
	"testing"
	"time"
)

func TestCoordinatedObjectKeyClearKeepsSiblingCached(t *testing.T) {
	ctx := context.Background()
	base := newCountingObjectStore(NewMemoryStore())
	cache := NewWriterObjectCache(base, WriterObjectCacheConfig{
		MaxBytes:    1 << 20,
		MaxKeys:     100,
		NegativeTTL: time.Hour,
	})
	store := NewTenantStore(cache, "test")
	store.SetCoordinator(newTaskLeaseTestCoordinator())

	if err := cache.Put(ctx, "objects/a", []byte("a")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := cache.Put(ctx, "objects/b", []byte("b")); err != nil {
		t.Fatalf("put b: %v", err)
	}
	base.reset()

	store.clearCoordinatedWriterObjectKey("objects/a")
	if _, err := cache.Get(ctx, "objects/b"); err != nil {
		t.Fatalf("get cached sibling: %v", err)
	}
	if base.getCalls != 0 {
		t.Fatalf("sibling underlying reads = %d, want 0", base.getCalls)
	}
	if _, err := cache.Get(ctx, "objects/a"); err != nil {
		t.Fatalf("get cleared key: %v", err)
	}
	if base.getCalls != 1 {
		t.Fatalf("cleared key underlying reads = %d, want 1", base.getCalls)
	}
}
