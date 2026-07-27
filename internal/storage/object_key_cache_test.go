package storage

import (
	"context"
	"strconv"
	"testing"
)

func TestObjectKeyCacheResetsAtCapacity(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	store.lockMu.Lock()
	for i := 0; i < maxObjectKeyCacheEntries; i++ {
		store.objectKeyCache["objects/"+strconv.Itoa(i)+".parquet"] = struct{}{}
	}
	store.lockMu.Unlock()

	store.markObjectKeyCached("objects/new.parquet")
	store.lockMu.Lock()
	defer store.lockMu.Unlock()
	if len(store.objectKeyCache) != 1 {
		t.Fatalf("object key cache entries = %d, want 1 after bounded reset", len(store.objectKeyCache))
	}
	if _, ok := store.objectKeyCache["objects/new.parquet"]; !ok {
		t.Fatal("new object key missing after cache reset")
	}
}

func TestObjectKeyMayExistUsesExactProbeWithoutNegativePrefixCache(
	t *testing.T,
) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newCountingObjectStore(base)
	store := NewTenantStore(objects, "test")
	key := "objects/item.parquet"

	exists, err := store.objectKeyMayExist(ctx, key)
	if err != nil || exists {
		t.Fatalf("missing key exists=%v err=%v", exists, err)
	}
	if objects.headCalls != 1 || objects.listCalls != 0 {
		t.Fatalf(
			"missing probe head=%d list=%d, want one exact HEAD",
			objects.headCalls, objects.listCalls,
		)
	}
	if err := base.Put(ctx, key, []byte("created-by-another-writer")); err != nil {
		t.Fatalf("put external object: %v", err)
	}
	exists, err = store.objectKeyMayExist(ctx, key)
	if err != nil || !exists {
		t.Fatalf("external key exists=%v err=%v", exists, err)
	}
	if objects.headCalls != 2 || objects.listCalls != 0 {
		t.Fatalf(
			"external probe head=%d list=%d, want exact HEAD without listing",
			objects.headCalls, objects.listCalls,
		)
	}
}
