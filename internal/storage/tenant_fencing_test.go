package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestExpiredWriterCannotPublishAfterLifecycleTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newBlockingManifestStore(base)
	stale := NewTenantStore(objects, "test")
	stale.LeaseTTL = 20 * time.Millisecond
	if _, err := stale.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	entered, release := objects.blockNextManifest()
	done := make(chan error, 1)
	go func() {
		_, err := stale.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:late", Kind: "host"}}}, CommitOptions{})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stale writer did not reach manifest publication")
	}

	time.Sleep(30 * time.Millisecond)
	owner := NewTenantStore(base, "test")
	owner.LeaseTTL = time.Hour
	if _, err := owner.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete after takeover: %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("stale commit err = %v, want ErrLeaseHeld", err)
	}

	manifest, err := owner.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current manifest: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("manifest version = %d, stale commit became visible", manifest.Version)
	}
}

func TestDelayedLeaseAcquirerCannotOverwriteNewerFence(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	seed := NewTenantStore(base, "test")
	seed.LeaseTTL = 20 * time.Millisecond
	if _, err := seed.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	objects := newBlockingManifestStore(base)
	delayed := NewTenantStore(objects, "test")
	delayed.LeaseTTL = 20 * time.Millisecond
	entered, release := objects.blockNextManifest()
	done := make(chan error, 1)
	go func() {
		_, err := delayed.SetTenantStatus(ctx, "tenant-a", TenantStatusActive)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delayed acquirer did not reach fence publication")
	}
	time.Sleep(30 * time.Millisecond)

	owner := NewTenantStore(base, "test")
	owner.LeaseTTL = time.Hour
	if _, err := owner.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("new owner takeover: %v", err)
	}
	ownerLease, err := owner.GetWriterLease(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get owner lease: %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("delayed acquirer err = %v, want ErrLeaseHeld", err)
	}

	manifest, err := owner.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current manifest: %v", err)
	}
	if manifest.WriterFence != ownerLease.FenceToken || manifest.WriterFenceEpoch != ownerLease.FenceEpoch {
		t.Fatalf("manifest fence = (%q,%d), owner = (%q,%d)", manifest.WriterFence, manifest.WriterFenceEpoch, ownerLease.FenceToken, ownerLease.FenceEpoch)
	}
}

func TestPurgeMarkerBlocksRecreateUntilDeletionCompletes(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newBlockingTenantDeleteStore(base, "test/tenants/tenant-a/")
	purger := NewTenantStore(objects, "test")
	if _, err := purger.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := purger.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	entered, release := objects.blockNextDelete()
	done := make(chan error, 1)
	go func() {
		_, err := purger.PurgeTenant(ctx, "tenant-a", false)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("purge did not reach object deletion")
	}

	recreator := NewTenantStore(base, "test")
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("create during purge err = %v, want ErrTenantDeleted", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate after purge: %v", err)
	}
	if _, err := recreator.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatalf("commit to recreated tenant: %v", err)
	}
	g, manifest, err := recreator.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recreated tenant: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:new"); !ok {
		t.Fatal("new tenant data is missing")
	}
}

func TestRunningPurgeBlocksLeaseAcquisitionWithStaleLifecycleCache(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newBlockingTenantDeleteStore(base, "test/tenants/tenant-a/")
	purger := NewTenantStore(objects, "test")
	purger.LeaseTTL = 20 * time.Millisecond
	if _, err := purger.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	stale := NewTenantStore(base, "test")
	stale.LifecycleCacheTTL = time.Hour
	if status, err := stale.TenantStatus(ctx, "tenant-a"); err != nil || status != TenantStatusActive {
		t.Fatalf("cache active status = %q err=%v", status, err)
	}
	entered, release := objects.blockNextDelete()
	done := make(chan error, 1)
	go func() {
		_, err := purger.PurgeTenant(ctx, "tenant-a", true)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("purge did not reach object deletion")
	}

	_, err := stale.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:stale", Kind: "host"}},
	}, CommitOptions{})
	if !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("commit during purge err = %v, want ErrTenantDeleted", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("purge: %v", err)
	}
}

func TestPurgeReleasesLeaseWithoutConditionalDelete(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(conditionalDeleteUnsupportedStore{ObjectStore: base}, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	before, err := store.GetWriterLease(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get initial lease: %v", err)
	}
	if _, err := store.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("purge: %v", err)
	}
	recreator := NewTenantStore(base, "test")
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	after, err := recreator.GetWriterLease(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get recreated lease: %v", err)
	}
	if after.FenceEpoch <= before.FenceEpoch {
		t.Fatalf("recreated fence epoch = %d, want > %d", after.FenceEpoch, before.FenceEpoch)
	}
}

type blockingTenantDeleteStore struct {
	ObjectStore
	prefix string

	mu      sync.Mutex
	block   bool
	entered chan struct{}
	release chan struct{}
}

type conditionalDeleteUnsupportedStore struct {
	ObjectStore
}

func (s conditionalDeleteUnsupportedStore) DeleteConditional(context.Context, string, PutCondition) error {
	return ErrConditionalDeleteUnsupported
}

func newBlockingTenantDeleteStore(inner ObjectStore, prefix string) *blockingTenantDeleteStore {
	return &blockingTenantDeleteStore{ObjectStore: inner, prefix: prefix}
}

func (s *blockingTenantDeleteStore) blockNextDelete() (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = true
	s.entered = make(chan struct{})
	s.release = make(chan struct{})
	return s.entered, s.release
}

func (s *blockingTenantDeleteStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	block := s.block && strings.HasPrefix(key, s.prefix)
	if block {
		s.block = false
	}
	entered, release := s.entered, s.release
	s.mu.Unlock()
	if block {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.ObjectStore.Delete(ctx, key)
}
