package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestLegacyLifecycleMirrorAllowsGraphHeadAdvance(t *testing.T) {
	ctx := context.Background()
	expected := CoordinationHead{
		TenantID: "tenant-a", Generation: 2, Status: TenantStatusActive, Revision: 4,
	}
	coordinator := &mutableHeadCoordinator{head: expected}
	objects := &afterPutObjectStore{
		ObjectStore: NewMemoryStore(),
		suffix:      "/metadata.parquet",
		after: func() {
			coordinator.update(func(head *CoordinationHead) {
				head.Revision++
				head.GraphVersion++
			})
		},
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)

	if err := store.putLegacyLifecycleMirrorObject(
		ctx,
		"tenant-a",
		store.tenantMetadataKey("tenant-a"),
		[]byte("metadata"),
		expected,
	); err != nil {
		t.Fatalf("mirror lifecycle metadata: %v", err)
	}
	if _, err := objects.Get(ctx, store.tenantMetadataKey("tenant-a")); err != nil {
		t.Fatalf("load mirrored lifecycle metadata: %v", err)
	}
}

func TestLegacyLifecycleMirrorRejectsGenerationChange(t *testing.T) {
	ctx := context.Background()
	expected := CoordinationHead{
		TenantID: "tenant-a", Generation: 2, Status: TenantStatusActive, Revision: 4,
	}
	coordinator := &mutableHeadCoordinator{head: expected}
	objects := &afterPutObjectStore{
		ObjectStore: NewMemoryStore(),
		suffix:      "/metadata.parquet",
		after: func() {
			coordinator.update(func(head *CoordinationHead) {
				head.Generation++
				head.Status = TenantStatusDisabled
			})
		},
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)

	err := store.putLegacyLifecycleMirrorObject(
		ctx,
		"tenant-a",
		store.tenantMetadataKey("tenant-a"),
		[]byte("metadata"),
		expected,
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("mirror err = %v, want ErrConflict", err)
	}
	if _, err := objects.Get(ctx, store.tenantMetadataKey("tenant-a")); err == nil {
		t.Fatal("stale lifecycle metadata was not rolled back")
	}
}

type mutableHeadCoordinator struct {
	WriteCoordinator
	mu   sync.Mutex
	head CoordinationHead
}

func (c *mutableHeadCoordinator) Backend() string   { return CoordinationPostgres }
func (c *mutableHeadCoordinator) Namespace() string { return "test" }

func (c *mutableHeadCoordinator) Head(
	_ context.Context,
	_ string,
) (CoordinationHead, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head, true, nil
}

func (c *mutableHeadCoordinator) update(update func(*CoordinationHead)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update(&c.head)
}

type afterPutObjectStore struct {
	ObjectStore
	mu     sync.Mutex
	suffix string
	after  func()
	fired  bool
}

func (s *afterPutObjectStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err != nil {
		return meta, err
	}
	s.mu.Lock()
	fire := !s.fired && strings.HasSuffix(key, s.suffix)
	if fire {
		s.fired = true
	}
	s.mu.Unlock()
	if fire {
		s.after()
	}
	return meta, nil
}
