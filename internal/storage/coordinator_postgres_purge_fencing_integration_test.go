package storage

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorFencesTakenOverPurge(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "purge-takeover")
	base := NewMemoryStore()
	objects := newBlockingPurgeDeleteStore(
		base, "test/tenants/tenant-a/",
	)
	first := NewTenantStore(objects, "test")
	first.SetCoordinator(coordinator)
	first.LeaseTTL = 300 * time.Millisecond
	second := NewTenantStore(objects, "test")
	second.SetCoordinator(coordinator)
	second.LeaseTTL = first.LeaseTTL

	if _, err := first.CreateTenant(ctx, "tenant-a", TenantCreateOptions{
		Name: "old-generation",
	}); err != nil {
		t.Fatalf("create old generation: %v", err)
	}
	if _, err := first.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:old", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed old generation: %v", err)
	}
	if _, err := first.SetTenantStatus(
		ctx, "tenant-a", TenantStatusDeleted,
	); err != nil {
		t.Fatalf("soft delete old generation: %v", err)
	}

	firstResult := make(chan error, 1)
	go func() {
		_, err := first.PurgeTenant(ctx, "tenant-a", false)
		firstResult <- err
	}()
	select {
	case <-objects.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first purge did not reach blocked delete")
	}

	var takeoverEpoch int64
	err := coordinator.pool.QueryRow(ctx,
		`UPDATE `+coordinator.table("task_leases")+`
		 SET owner_token = 'takeover',
		     fence_epoch = fence_epoch + 1,
		     expires_at = now() + interval '1 minute',
		     updated_at = now()
			 WHERE namespace = $1 AND tenant_id = $2 AND task_type = $3
			 RETURNING fence_epoch`,
		coordinator.namespace, "tenant-a", coordinatorPurgeTaskType,
	).Scan(&takeoverEpoch)
	if err != nil {
		t.Fatalf("take over purge lease: %v", err)
	}
	time.Sleep(2 * first.LeaseTTL)
	if err := coordinator.ReleaseTaskLease(ctx, CoordinatorTaskLease{
		TenantID:   "tenant-a",
		TaskType:   coordinatorPurgeTaskType,
		OwnerToken: "takeover",
		FenceEpoch: takeoverEpoch,
	}); err != nil {
		t.Fatalf("release takeover lease: %v", err)
	}

	if _, err := second.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("complete takeover purge: %v", err)
	}
	if _, err := second.CreateTenant(ctx, "tenant-a", TenantCreateOptions{
		Name: "new-generation",
	}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}
	if _, err := second.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed new generation: %v", err)
	}
	close(objects.release)
	if err := <-firstResult; err == nil {
		t.Fatal("taken-over purge unexpectedly succeeded")
	}

	verifier := NewTenantStore(base, "test")
	verifier.SetCoordinator(coordinator)
	info, err := verifier.GetTenantInfo(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recreated tenant info: %v", err)
	}
	if info.Name != "new-generation" {
		t.Fatalf("recreated tenant name = %q, want new-generation", info.Name)
	}
	managed, err := verifier.ListManagedTenants(ctx)
	if err != nil {
		t.Fatalf("list managed tenants: %v", err)
	}
	if !containsString(managed, "tenant-a") {
		t.Fatalf("taken-over purge removed recreated tenant registry: %v", managed)
	}
	g, _, err := verifier.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recreated graph: %v", err)
	}
	if _, ok := g.GetEntity("host:new"); !ok {
		t.Fatal("taken-over purge removed recreated graph data")
	}
}

type blockingPurgeDeleteStore struct {
	ObjectStore
	prefix  string
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func newBlockingPurgeDeleteStore(
	inner ObjectStore,
	prefix string,
) *blockingPurgeDeleteStore {
	return &blockingPurgeDeleteStore{
		ObjectStore: inner,
		prefix:      prefix,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (s *blockingPurgeDeleteStore) Delete(
	ctx context.Context,
	key string,
) error {
	if err := s.blockFirstDelete(ctx, key); err != nil {
		return err
	}
	return s.ObjectStore.Delete(ctx, key)
}

func (s *blockingPurgeDeleteStore) DeleteConditional(
	ctx context.Context,
	key string,
	condition PutCondition,
) error {
	if err := s.blockFirstDelete(ctx, key); err != nil {
		return err
	}
	return s.ObjectStore.DeleteConditional(ctx, key, condition)
}

func (s *blockingPurgeDeleteStore) blockFirstDelete(
	ctx context.Context,
	key string,
) error {
	if !strings.HasPrefix(key, s.prefix) {
		return nil
	}
	block := false
	s.once.Do(func() {
		block = true
		close(s.started)
	})
	if !block {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return nil
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

var _ ObjectStore = (*blockingPurgeDeleteStore)(nil)
