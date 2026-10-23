package storage

import (
	"context"
	"errors"
	"testing"
)

type fenceLostAfterPut struct {
	ObjectStore
	target, lease string
}

func (s fenceLostAfterPut) PutConditional(ctx context.Context, key string, data []byte, c PutCondition) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, c)
	if err == nil && key == s.target {
		s.ObjectStore.Delete(ctx, s.lease)
	}
	return meta, err
}
func TestFenceLossDoesNotDeleteOverwrittenObject(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	bound, err := store.acquireAndBindWriterFence(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	key := "test/tenants/tenant-a/control/value"
	meta, err := base.PutConditional(ctx, key, []byte("old"), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	store.Objects = fenceLostAfterPut{ObjectStore: base, target: key, lease: store.writerLeaseKey("tenant-a")}
	_, err = store.putTenantConditional(bound, "tenant-a", key, []byte("new"), PutCondition{IfMatch: meta.ETag})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("fence loss: %v", err)
	}
	data, err := base.Get(ctx, key)
	if err != nil || string(data) != "old" {
		t.Fatalf("overwrite rollback lost previous value: %q %v", data, err)
	}
}
