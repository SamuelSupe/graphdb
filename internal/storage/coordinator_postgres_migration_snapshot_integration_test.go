package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorMigrationListsAfterManifestSnapshot(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "migration-snapshot")
	sourceBase := NewMemoryStore()
	sourceObjects := newBlockingManifestReadStore(
		sourceBase, "source/tenants/tenant-a/manifest.parquet",
	)
	source := NewTenantStore(sourceObjects, "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	sourceObjects.arm()

	target := NewTenantStore(NewMemoryStore(), "target")
	target.SetCoordinator(coordinator)
	result := make(chan error, 1)
	go func() {
		_, err := CopyTenantObjects(
			ctx,
			source,
			"tenant-a",
			target,
			"tenant-a",
			TenantMigrationOptions{},
		)
		result <- err
	}()
	select {
	case <-sourceObjects.started:
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not reach manifest snapshot")
	}
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:second", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance source during migration: %v", err)
	}
	close(sourceObjects.release)
	if err := <-result; err != nil {
		t.Fatalf("migrate concurrent source snapshot: %v", err)
	}

	g, manifest, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated graph: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("migrated version = %d, want 2", manifest.Version)
	}
	for _, id := range []string{"host:first", "host:second"} {
		if _, ok := g.GetEntity(id); !ok {
			t.Fatalf("migrated graph is missing %s", id)
		}
	}
}

type blockingManifestReadStore struct {
	ObjectStore
	key     string
	mu      sync.Mutex
	armed   bool
	blocked bool
	started chan struct{}
	release chan struct{}
}

func newBlockingManifestReadStore(
	inner ObjectStore,
	key string,
) *blockingManifestReadStore {
	return &blockingManifestReadStore{
		ObjectStore: inner,
		key:         key,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (s *blockingManifestReadStore) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

func (s *blockingManifestReadStore) GetWithMeta(
	ctx context.Context,
	key string,
) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	block := key == s.key && s.armed && !s.blocked
	if block {
		s.blocked = true
	}
	s.mu.Unlock()
	if block {
		close(s.started)
		select {
		case <-ctx.Done():
			return nil, ObjectMeta{Key: key}, ctx.Err()
		case <-s.release:
		}
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

var _ ObjectStore = (*blockingManifestReadStore)(nil)
