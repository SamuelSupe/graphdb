package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestTenantDisableWaitsForPublishingCommit(t *testing.T) {
	ctx := context.Background()
	objects := newBlockingManifestStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	entered, release := objects.blockNextManifest()
	commitDone := make(chan error, 1)
	go func() {
		_, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{})
		commitDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("commit did not reach manifest publication")
	}

	disableDone := make(chan error, 1)
	go func() {
		_, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled)
		disableDone <- err
	}()
	select {
	case err := <-disableDone:
		t.Fatalf("disable returned before publishing commit completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-commitDone; err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := <-disableDone; err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}}, CommitOptions{}); !errors.Is(err, ErrTenantDisabled) {
		t.Fatalf("commit after disable err = %v, want ErrTenantDisabled", err)
	}
}

func TestPurgedTenantRejectsCommitThatPassedAnEarlierStatusCheck(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	lifecycle := NewTenantStore(objects, "test")
	if _, err := lifecycle.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	blocking := newBlockFirstMetadataReadStore(objects, lifecycle.tenantMetadataKey("tenant-a"))
	writer := NewTenantStore(blocking, "test")
	commitDone := make(chan error, 1)
	go func() {
		_, err := writer.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:late", Kind: "host"}}}, CommitOptions{})
		commitDone <- err
	}()
	select {
	case <-blocking.entered:
	case <-time.After(time.Second):
		t.Fatal("commit did not read the active tenant metadata")
	}

	if _, err := lifecycle.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}
	if _, err := lifecycle.PurgeTenant(ctx, "tenant-a", false); err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	close(blocking.release)
	if err := <-commitDone; !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("stale commit err = %v, want ErrTenantDeleted", err)
	}
	if _, err := lifecycle.GetTenantInfo(ctx, "tenant-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant after purge err = %v, want ErrNotFound", err)
	}
	status, err := writer.TenantStatus(ctx, "tenant-a")
	if err != nil || status != TenantStatusDeleted {
		t.Fatalf("writer status after purge = %q err=%v, want deleted", status, err)
	}
	purged, err := lifecycle.tenantPurgeTombstoneExists(ctx, "tenant-a")
	if err != nil || !purged {
		t.Fatalf("purge tombstone exists=%v err=%v", purged, err)
	}
	tenants, err := lifecycle.ListManagedTenants(ctx)
	if err != nil {
		t.Fatalf("list managed tenants: %v", err)
	}
	if len(tenants) != 0 {
		t.Fatalf("managed tenants after stale commit = %#v, want empty", tenants)
	}
}

func TestDisabledTenantRejectsStorageMutations(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}

	checks := map[string]func() error{
		"compact":       func() error { _, err := store.Compact(ctx, "tenant-a"); return err },
		"gc":            func() error { _, err := store.RunGC(ctx, "tenant-a", GCOptions{}); return err },
		"recover":       func() error { _, err := store.RecoverTenant(ctx, "tenant-a"); return err },
		"cleanup":       func() error { _, err := store.CleanupCommits(ctx, "tenant-a"); return err },
		"repair":        func() error { _, err := store.RepairTenant(ctx, "tenant-a", RepairOptions{Apply: true}); return err },
		"rebuild":       func() error { _, err := store.RebuildIndexes(ctx, "tenant-a"); return err },
		"task":          func() error { _, err := store.StartTask(ctx, "tenant-a", TaskTypeCompact, nil); return err },
		"config":        func() error { _, err := store.PutTenantConfig(ctx, "tenant-a", TenantConfig{}); return err },
		"source policy": func() error { _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{}); return err },
		"saved query": func() error {
			_, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{Name: "hosts", Request: query.Request{Op: "match"}})
			return err
		},
		"index": func() error {
			_, err := store.CreateIndex(ctx, "tenant-a", IndexDefinition{Name: "host-name", Kind: "host", Field: "name"})
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, ErrTenantDisabled) {
				t.Fatalf("err = %v, want ErrTenantDisabled", err)
			}
		})
	}
}

type blockFirstMetadataReadStore struct {
	ObjectStore
	key     string
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	blocked bool
}

func newBlockFirstMetadataReadStore(inner ObjectStore, key string) *blockFirstMetadataReadStore {
	return &blockFirstMetadataReadStore{
		ObjectStore: inner,
		key:         key,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (s *blockFirstMetadataReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	block := key == s.key && !s.blocked
	if block {
		s.blocked = true
	}
	s.mu.Unlock()
	data, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	if !block {
		return data, meta, err
	}
	close(s.entered)
	select {
	case <-s.release:
		return data, meta, err
	case <-ctx.Done():
		return nil, ObjectMeta{Key: key}, ctx.Err()
	}
}
