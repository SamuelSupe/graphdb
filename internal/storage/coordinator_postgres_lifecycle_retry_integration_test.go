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

func TestPostgresCoordinatorCloneResumesPartialCandidate(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "clone-partial-resume",
	)
	base := NewMemoryStore()
	objects := &failOnceCandidatePutStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	objects.arm(
		"test/tenants/tenant-target/snapshots/sharded/", "", 2,
	)

	_, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	)
	if !errors.Is(err, ErrObjectStoreUnavailable) {
		t.Fatalf("first clone err=%v, want injected failure", err)
	}
	if exists, err := store.tenantDataExists(
		ctx, "tenant-target",
	); err != nil || !exists {
		t.Fatalf("partial target exists=%v err=%v", exists, err)
	}
	if head, exists, err := coordinator.Head(
		ctx, "tenant-target",
	); err != nil || (exists && head.Status == TenantStatusActive) {
		t.Fatalf("partial head exists=%v head=%#v err=%v", exists, head, err)
	}

	if _, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	); err != nil {
		t.Fatalf("resume clone: %v", err)
	}
	graph, _, err := store.Load(ctx, "tenant-target")
	if err != nil {
		t.Fatalf("load clone: %v", err)
	}
	if _, ok := graph.GetEntity("host:source"); !ok {
		t.Fatal("resumed clone is missing source data")
	}
}

func TestPostgresCoordinatorMigrationResumesPartialCandidate(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "migration-partial-resume",
	)
	source := NewTenantStore(NewMemoryStore(), "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := source.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact source: %v", err)
	}
	base := NewMemoryStore()
	objects := &failOnceCandidatePutStore{ObjectStore: base}
	objects.arm("target/tenants/tenant-a/", "/coordination/", 2)
	target := NewTenantStore(objects, "target")
	target.SetCoordinator(coordinator)

	_, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	)
	if !errors.Is(err, ErrObjectStoreUnavailable) {
		t.Fatalf("first migration err=%v, want injected failure", err)
	}
	if exists, err := target.tenantDataExists(
		ctx, "tenant-a",
	); err != nil || !exists {
		t.Fatalf("partial target exists=%v err=%v", exists, err)
	}

	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	graph, _, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migration: %v", err)
	}
	if _, ok := graph.GetEntity("host:source"); !ok {
		t.Fatal("resumed migration is missing source data")
	}
}

func TestPostgresCoordinatorCloneAndPurgeShareLifecycleLease(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "clone-purge-lifecycle-lease",
	)
	objects := &blockingCloneSnapshotStore{
		ObjectStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	objects.arm("test/tenants/tenant-target/snapshots/sharded/")
	cloneResult := make(chan error, 1)
	go func() {
		_, err := store.CloneTenant(
			ctx,
			"tenant-source",
			TenantCloneOptions{TargetTenantID: "tenant-target"},
		)
		cloneResult <- err
	}()
	select {
	case <-objects.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("clone did not reach candidate publication")
	}

	purger := NewTenantStore(objects, "test")
	purger.SetCoordinator(coordinator)
	if _, err := purger.PurgeTenant(
		ctx, "tenant-target", true,
	); !errors.Is(err, ErrTaskLeaseHeld) {
		t.Fatalf("concurrent purge err=%v, want ErrTaskLeaseHeld", err)
	}
	close(objects.release)
	if err := <-cloneResult; err != nil {
		t.Fatalf("clone tenant: %v", err)
	}
}

func TestPostgresCoordinatorForcePurgesHeadlessLifecycleCandidate(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "purge-headless-candidate",
	)
	base := NewMemoryStore()
	objects := &failOnceCandidatePutStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	objects.arm(
		"test/tenants/tenant-target/snapshots/sharded/", "", 2,
	)
	if _, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	); !errors.Is(err, ErrObjectStoreUnavailable) {
		t.Fatalf("partial clone err=%v, want injected failure", err)
	}
	if _, exists, err := coordinator.Head(
		ctx, "tenant-target",
	); err != nil || exists {
		t.Fatalf("partial target head exists=%v err=%v", exists, err)
	}
	if _, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx, "tenant-target",
	); err != nil || !exists {
		t.Fatalf("partial target candidate exists=%v err=%v", exists, err)
	}

	report, err := store.PurgeTenant(ctx, "tenant-target", true)
	if err != nil {
		t.Fatalf("purge partial target: %v", err)
	}
	if report.Deleted == 0 {
		t.Fatal("purge did not report deleted candidate objects")
	}
	objectsLeft, err := base.List(
		ctx, store.tenantObjectPrefix("tenant-target"),
	)
	if err != nil || len(objectsLeft) != 0 {
		t.Fatalf("partial target objects=%v err=%v", objectsLeft, err)
	}
	if _, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	); err != nil {
		t.Fatalf("reuse purged target: %v", err)
	}
}

func TestPostgresCoordinatorForcePurgePreservesHeadlessUnownedData(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "purge-headless-unowned",
	)
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	key := store.tenantObjectPrefix("tenant-target") + "unknown.bin"
	if err := objects.Put(ctx, key, []byte("unowned")); err != nil {
		t.Fatalf("seed unowned data: %v", err)
	}

	if _, err := store.PurgeTenant(
		ctx, "tenant-target", true,
	); !errors.Is(err, ErrCoordinatorHeadMissing) {
		t.Fatalf("purge unowned target err=%v, want missing head", err)
	}
	data, err := objects.Get(ctx, key)
	if err != nil || string(data) != "unowned" {
		t.Fatalf("unowned data=%q err=%v", data, err)
	}
}

func TestPostgresCoordinatorHeadlessCandidateBlocksOrdinaryHeadPublication(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "headless-candidate-blocks-head",
	)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	candidate := newCoordinatedTenantCandidate(
		"clone",
		"tenant-source",
		store.tenantObjectPrefix("tenant-source"),
		"tenant-target",
	)
	if _, err := store.prepareCoordinatedTenantCandidate(
		ctx, "tenant-target", candidate,
	); err != nil {
		t.Fatalf("prepare candidate: %v", err)
	}
	if err := store.putCoordinatedCandidateObject(
		ctx,
		store.tenantObjectPrefix("tenant-target")+"snapshots/partial.parquet",
		[]byte("partial"),
	); err != nil {
		t.Fatalf("put candidate object: %v", err)
	}

	if _, err := store.Commit(ctx, "tenant-target", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:ordinary", Kind: "host"}},
	}, CommitOptions{}); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("ordinary commit err=%v, want write conflict", err)
	}
	if _, err := store.CreateTenant(
		ctx, "tenant-target", TenantCreateOptions{},
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("ordinary create err=%v, want conflict", err)
	}
	if head, exists, err := coordinator.Head(
		ctx, "tenant-target",
	); err != nil || exists {
		t.Fatalf("target head=%#v exists=%v err=%v", head, exists, err)
	}
	if current, exists, _, err := store.getCoordinatedTenantCandidate(
		ctx, "tenant-target",
	); err != nil || !exists || current != candidate {
		t.Fatalf(
			"candidate=%#v exists=%v err=%v, want original",
			current, exists, err,
		)
	}
}

func TestPostgresCoordinatorCloneFinishesPublishedCandidateAfterSourceAdvances(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "clone-published-resume",
	)
	base := NewMemoryStore()
	objects := &failOnceCandidatePutStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	objects.arm(store.tenantRegistryKey(), "", 1)
	_, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	)
	if !errors.Is(err, ErrObjectStoreUnavailable) {
		t.Fatalf("first clone err=%v, want injected registry failure", err)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-target")
	if err != nil || !exists || head.Status != TenantStatusActive {
		t.Fatalf("published target head=%#v exists=%v err=%v", head, exists, err)
	}
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:second", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance source: %v", err)
	}
	if _, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	); err != nil {
		t.Fatalf("finish published clone: %v", err)
	}
	targetGraph, manifest, err := store.Load(ctx, "tenant-target")
	if err != nil {
		t.Fatalf("load target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("target version=%d, want published version 1", manifest.Version)
	}
	if _, ok := targetGraph.GetEntity("host:second"); ok {
		t.Fatal("resume replaced the already-published clone")
	}
	assertLifecycleCandidateRemoved(t, ctx, store, "tenant-target")
	if _, err := store.CloneTenant(
		ctx,
		"tenant-source",
		TenantCloneOptions{TargetTenantID: "tenant-target"},
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("new clone over completed target err=%v, want conflict", err)
	}
}

func TestPostgresCoordinatorMigrationFinishesPublishedCandidateAfterSourceAdvances(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "migration-published-resume",
	)
	source := NewTenantStore(NewMemoryStore(), "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	base := NewMemoryStore()
	objects := &failOnceCandidatePutStore{ObjectStore: base}
	target := NewTenantStore(objects, "target")
	target.SetCoordinator(coordinator)
	objects.arm(target.tenantRegistryKey(), "", 1)
	_, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	)
	if !errors.Is(err, ErrObjectStoreUnavailable) {
		t.Fatalf("first migration err=%v, want injected registry failure", err)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists || head.Status != TenantStatusActive {
		t.Fatalf("published target head=%#v exists=%v err=%v", head, exists, err)
	}
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:second", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance source: %v", err)
	}
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); err != nil {
		t.Fatalf("finish published migration: %v", err)
	}
	targetGraph, manifest, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("target version=%d, want published version 1", manifest.Version)
	}
	if _, ok := targetGraph.GetEntity("host:second"); ok {
		t.Fatal("resume replaced the already-published migration")
	}
	assertLifecycleCandidateRemoved(t, ctx, target, "tenant-a")
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("new migration over completed target err=%v, want conflict", err)
	}
}

func assertLifecycleCandidateRemoved(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	tenantID string,
) {
	t.Helper()
	_, exists, _, err := store.getCoordinatedTenantCandidate(ctx, tenantID)
	if err != nil || exists {
		t.Fatalf("candidate exists=%v err=%v", exists, err)
	}
}

type failOnceCandidatePutStore struct {
	ObjectStore
	mu      sync.Mutex
	prefix  string
	exclude string
	failAt  int
	matches int
	failed  bool
	armed   bool
}

func (s *failOnceCandidatePutStore) arm(
	prefix string,
	exclude string,
	failAt int,
) {
	s.mu.Lock()
	s.prefix = prefix
	s.exclude = exclude
	s.failAt = failAt
	s.matches = 0
	s.failed = false
	s.armed = true
	s.mu.Unlock()
}

func (s *failOnceCandidatePutStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	s.mu.Lock()
	match := s.armed &&
		!s.failed &&
		strings.HasPrefix(key, s.prefix) &&
		(s.exclude == "" || !strings.Contains(key, s.exclude))
	if match {
		s.matches++
		if s.matches == s.failAt {
			s.failed = true
		}
	}
	fail := match && s.failed && s.matches == s.failAt
	s.mu.Unlock()
	if fail {
		return ObjectMeta{Key: key}, ErrObjectStoreUnavailable
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}
