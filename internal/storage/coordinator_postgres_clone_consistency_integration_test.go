package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCloneDoesNotMixGraphAndWriteContextRevisions(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "clone-context-snapshot",
	)
	base := NewMemoryStore()
	objects := &blockingObjectGetStore{
		ObjectStore: base,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	defer func() {
		select {
		case <-objects.release:
		default:
			close(objects.release)
		}
	}()
	cloneStore := NewTenantStore(objects, "test")
	cloneStore.SetCoordinator(coordinator)
	quota := 100
	if _, err := cloneStore.CreateTenant(
		ctx,
		"tenant-source",
		TenantCreateOptions{Config: &TenantConfig{
			Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota},
		}},
	); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := cloneStore.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	head, _, err := coordinator.Head(ctx, "tenant-source")
	if err != nil || head.WriteContextKey == "" {
		t.Fatalf("load source head=%#v err=%v", head, err)
	}
	objects.arm(head.WriteContextKey)

	result := make(chan error, 1)
	go func() {
		_, err := cloneStore.CloneTenant(
			ctx,
			"tenant-source",
			TenantCloneOptions{TargetTenantID: "tenant-clone"},
		)
		result <- err
	}()
	select {
	case <-objects.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("clone did not reach write-context capture")
	}

	concurrent := NewTenantStore(base, "test")
	concurrent.SetCoordinator(coordinator)
	if _, err := concurrent.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "depends_custom", FromKind: "document",
			ToKind: "document", Directed: true,
		}},
		UpsertEdges: []graph.Edge{{
			Type: "depends_custom", From: "document:a", To: "document:b",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance source graph: %v", err)
	}
	if _, err := concurrent.PutRelationSchema(
		ctx,
		"tenant-source",
		RelationSchema{RelationType: "depends_custom"},
	); err != nil {
		t.Fatalf("advance source context: %v", err)
	}
	close(objects.release)
	if err := <-result; err != nil {
		t.Fatalf("clone tenant: %v", err)
	}

	g, _, err := cloneStore.Load(ctx, "tenant-clone")
	if err != nil {
		t.Fatalf("load clone: %v", err)
	}
	_, hasRelationType := g.RelationTypes["depends_custom"]
	schemas, err := cloneStore.GetRelationSchemas(ctx, "tenant-clone")
	if err != nil {
		t.Fatalf("load clone schemas: %v", err)
	}
	_, hasSchema := schemas.Schema("depends_custom")
	if hasRelationType != hasSchema {
		t.Fatalf(
			"clone mixed revisions: relation_type=%v schema=%v",
			hasRelationType, hasSchema,
		)
	}
}

type blockingObjectGetStore struct {
	ObjectStore
	mu      sync.Mutex
	key     string
	armed   bool
	blocked bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingObjectGetStore) arm(key string) {
	s.mu.Lock()
	s.key = key
	s.armed = true
	s.mu.Unlock()
}

func (s *blockingObjectGetStore) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	s.mu.Lock()
	block := s.armed && !s.blocked && key == s.key
	if block {
		s.blocked = true
	}
	s.mu.Unlock()
	if block {
		close(s.entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.release:
		}
	}
	return s.ObjectStore.Get(ctx, key)
}
