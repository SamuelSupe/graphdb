package storage

import (
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
