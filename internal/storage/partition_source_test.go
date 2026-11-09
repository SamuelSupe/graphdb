package storage

import (
	"context"
	"errors"
	"testing"
)

func TestRetirePartitionSourceCleansLegacyIndexData(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "graphdb")
	if _, err := store.CreateTenant(ctx, "t", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	prefix := store.tenantObjectPrefix("t")
	stale := []string{
		prefix + "indexs/legacy.parquet",
		store.secondaryIndexPrefix("t") + "host/name.parquet",
		store.edgeShardPrefix("t") + "calls.parquet",
		store.reverseIndexPrefix("t") + "v1/calls.parquet",
		store.indexCatalogKey("t"),
	}
	retained := []string{store.indexDefinitionsKey("t"), store.indexTaskKey("t", "completed")}
	for _, key := range append(append([]string(nil), stale...), retained...) {
		if err := store.Objects.Put(ctx, key, []byte("index fixture")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; ; i++ {
		if i == 16 {
			t.Fatal("retirement did not complete")
		}
		done, err := store.RetirePartitionSource(ctx, "t", true)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	for _, key := range stale {
		if _, err := store.Objects.Get(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("obsolete index still present: %s: %v", key, err)
		}
	}
	for _, key := range retained {
		if _, err := store.Objects.Get(ctx, key); err != nil {
			t.Fatalf("retired source lost management metadata: %s: %v", key, err)
		}
	}
}
