package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorConcurrentTenantCommits(t *testing.T) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	namespace := "concurrent-tenant"
	admin, err := NewPostgresCoordinator(ctx, dsn, schema, namespace)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	if err := admin.Migrate(ctx); err != nil {
		admin.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		admin.Close()
	})

	objects := NewMemoryStore()
	const writers = 8
	stores := make([]*TenantStore, 0, writers)
	coordinators := make([]*PostgresCoordinator, 0, writers)
	for i := 0; i < writers; i++ {
		coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, namespace)
		if err != nil {
			t.Fatalf("new writer coordinator %d: %v", i, err)
		}
		coordinators = append(coordinators, coordinator)
		store := NewTenantStore(objects, "test")
		store.InstanceID = fmt.Sprintf("writer-%d", i)
		store.CoordinatorRetryLimit = 8
		store.SetCoordinator(coordinator)
		stores = append(stores, store)
	}
	t.Cleanup(func() {
		for _, coordinator := range coordinators {
			coordinator.Close()
		}
	})

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i, store := range stores {
		i, store := i, store
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{
					ID:     fmt.Sprintf("host:%d", i),
					Kind:   "host",
					Fields: graph.Fields{"writer": i},
				}},
			}, CommitOptions{})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent commit: %v", err)
		}
	}

	g, manifest, err := stores[0].Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load coordinated graph: %v", err)
	}
	if manifest.Version != writers {
		t.Fatalf("manifest version = %d, want %d", manifest.Version, writers)
	}
	for i := 0; i < writers; i++ {
		if _, ok := g.GetEntity(fmt.Sprintf("host:%d", i)); !ok {
			t.Fatalf("missing host:%d", i)
		}
	}

	synced, err := stores[0].SyncLegacyManifests(ctx)
	if err != nil {
		t.Fatalf("sync legacy manifests: %v", err)
	}
	if synced != 1 {
		t.Fatalf("coalesced synced jobs = %d, want 1", synced)
	}
	legacy := NewTenantStore(objects, "test")
	legacyGraph, legacyManifest, err := legacy.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load legacy graph: %v", err)
	}
	if legacyManifest.Version != writers || len(legacyGraph.Entities) != writers {
		t.Fatalf("legacy graph version/entities = %d/%d", legacyManifest.Version, len(legacyGraph.Entities))
	}
}

func TestPostgresCoordinatorIdempotencyAndExpectedVersion(t *testing.T) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, "idempotency")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = coordinator.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		coordinator.Close()
	})
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}
	first, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{IdempotencyKey: "same"})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	replay, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{IdempotencyKey: "same"})
	if err != nil {
		t.Fatalf("replay commit: %v", err)
	}
	if !replay.IdempotentReplay || replay.Version != first.Version {
		t.Fatalf("replay = %#v, first = %#v", replay, first)
	}
	expected := int64(0)
	_, err = store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{ExpectedVersion: &expected, IdempotencyKey: "failed-version"})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected-version err = %v, want ErrVersionConflict", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "failed-version"}); err != nil {
		t.Fatalf("reuse idempotency key after definitive failure: %v", err)
	}

	firstReservation, err := coordinator.ReserveCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-a", 50*time.Millisecond,
	)
	if err != nil || firstReservation.OwnerToken != "owner-a" {
		t.Fatalf("first idempotency reservation=%#v err=%v", firstReservation, err)
	}
	if _, err := coordinator.ReserveCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-b", 50*time.Millisecond,
	); !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("active reservation takeover err = %v, want ErrIdempotencyInProgress", err)
	}
	time.Sleep(75 * time.Millisecond)
	secondReservation, err := coordinator.ReserveCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-b", 50*time.Millisecond,
	)
	if err != nil || secondReservation.OwnerToken != "owner-b" {
		t.Fatalf("expired reservation takeover=%#v err=%v", secondReservation, err)
	}
	if err := coordinator.AbortCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-a",
	); err != nil {
		t.Fatalf("stale idempotency abort: %v", err)
	}
	if _, err := coordinator.ReserveCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-c", time.Second,
	); !errors.Is(err, ErrIdempotencyInProgress) {
		t.Fatalf("stale abort released current owner, err=%v", err)
	}
	if err := coordinator.AbortCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-b",
	); err != nil {
		t.Fatalf("current idempotency abort: %v", err)
	}
	thirdReservation, err := coordinator.ReserveCommit(
		ctx, "tenant-a", "lease-key", "request-hash", "owner-c", time.Second,
	)
	if err != nil || thirdReservation.OwnerToken != "owner-c" {
		t.Fatalf("reservation after abort=%#v err=%v", thirdReservation, err)
	}
}

func TestPostgresCoordinatorWriteContextAndIngestState(t *testing.T) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, "write-context")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = coordinator.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		coordinator.Close()
	})
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "cites", Directed: true, FromKind: "document", ToKind: "document",
		}},
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	if _, err := store.PutRelationSchema(ctx, "tenant-a", RelationSchema{
		RelationType: "cites",
		Strict:       true,
		Fields: map[string]graph.FieldSpec{
			"weight": {Type: "number", Required: true},
		},
	}); err != nil {
		t.Fatalf("put relation schema: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEdges: []graph.Edge{{
			ID: "edge:missing-weight", Type: "cites",
			From: "document:a", To: "document:b",
		}},
	}, CommitOptions{}); err == nil {
		t.Fatal("strict coordinated relation schema accepted a missing required field")
	}
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "collector", Priority: 10}},
	}); err != nil {
		t.Fatalf("put source policy: %v", err)
	}
	if _, err := store.PutTenantConfig(ctx, "tenant-a", TenantConfig{
		Quota: TenantQuotaConfig{MaxEntitiesPerTenant: intPtr(100)},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head exists=%v err=%v", exists, err)
	}
	if head.WriteContextRevision != 3 || head.WriteContextKey == "" || head.WriteContextHash == "" {
		t.Fatalf("write-context head = %#v", head)
	}
	reader := NewTenantStore(objects, "test")
	reader.SetCoordinator(coordinator)
	if policy, ok, err := reader.GetSourcePolicy(ctx, "tenant-a"); err != nil || !ok ||
		len(policy.Sources) != 1 || policy.Sources[0].Priority != 10 {
		t.Fatalf("coordinated source policy ok=%v policy=%#v err=%v", ok, policy, err)
	}
	if catalog, err := reader.GetRelationSchemas(ctx, "tenant-a"); err != nil || len(catalog.RelationSchemas) != 1 {
		t.Fatalf("coordinated relation schemas=%#v err=%v", catalog, err)
	}

	request := IngestRequest{
		Source: "collector", CollectorID: "agent-a", BatchID: "batch-1", Cursor: "cursor-1",
		Items: []IngestItem{{ExternalID: "host-a", Entity: &graph.Entity{
			ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"},
		}}},
	}
	first, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest first batch: %v", err)
	}
	replay, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest replay: %v", err)
	}
	if !replay.Skipped || replay.Version != first.Version {
		t.Fatalf("ingest replay=%#v first=%#v", replay, first)
	}
	status, err := store.GetCollectorStatus(ctx, "tenant-a", "collector", "agent-a")
	if err != nil {
		t.Fatalf("collector status: %v", err)
	}
	if status.LastBatchID != "batch-1" || status.LastCursor != "cursor-1" || status.LastVersion != first.Version {
		t.Fatalf("collector status = %#v", status)
	}
	request.BatchID = "batch-2"
	request.Cursor = "cursor-2"
	second, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("ingest no-op batch: %v", err)
	}
	if !second.Skipped || second.Version != first.Version {
		t.Fatalf("no-op ingest=%#v first=%#v", second, first)
	}
	status, err = store.GetCollectorStatus(ctx, "tenant-a", "collector", "agent-a")
	if err != nil || status.LastBatchID != "batch-2" || status.LastCursor != "cursor-2" {
		t.Fatalf("no-op collector status=%#v err=%v", status, err)
	}
	if processed, err := store.SyncDerivedTasks(ctx); err != nil || processed != 0 {
		t.Fatalf("derived task ran before debounce processed=%d err=%v", processed, err)
	}
	time.Sleep(derivedTaskDebounce + 50*time.Millisecond)
	if processed, err := store.SyncDerivedTasks(ctx); err != nil || processed == 0 {
		t.Fatalf("sync derived tasks processed=%d err=%v", processed, err)
	}
	coordStatus, err := coordinator.Status(ctx)
	if err != nil || coordStatus.DerivedBacklog != 0 {
		t.Fatalf("coordinator status=%#v err=%v", coordStatus, err)
	}
}

func TestPostgresCoordinatorDerivedTaskLeaseRenews(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "derived-lease")
	_, err := coordinator.pool.Exec(ctx,
		`INSERT INTO `+coordinator.table("derived_tasks")+` (
			namespace, tenant_id, task_type, target_version, next_attempt_at
		) VALUES ($1,'tenant-a',$2,1,now())`,
		coordinator.namespace, derivedTaskIndexes,
	)
	if err != nil {
		t.Fatalf("insert derived task: %v", err)
	}
	ttl := 300 * time.Millisecond
	first, claimed, err := coordinator.ClaimDerivedTask(ctx, "worker-a", ttl)
	if err != nil || !claimed {
		t.Fatalf("first claim claimed=%v err=%v", claimed, err)
	}
	time.Sleep(150 * time.Millisecond)
	if renewed, err := coordinator.RenewDerivedTask(ctx, first, ttl); err != nil || !renewed {
		t.Fatalf("renewed=%v err=%v", renewed, err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, claimed, err := coordinator.ClaimDerivedTask(ctx, "worker-b", ttl); err != nil || claimed {
		t.Fatalf("claim during renewed lease claimed=%v err=%v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond)
	second, claimed, err := coordinator.ClaimDerivedTask(ctx, "worker-b", ttl)
	if err != nil || !claimed {
		t.Fatalf("claim after renewed lease expiry claimed=%v err=%v", claimed, err)
	}
	if renewed, err := coordinator.RenewDerivedTask(ctx, first, ttl); err != nil || renewed {
		t.Fatalf("stale renew renewed=%v err=%v", renewed, err)
	}
	if err := coordinator.CompleteDerivedTask(ctx, second, second.TargetVersion); err != nil {
		t.Fatalf("complete derived task: %v", err)
	}
}

func TestPostgresCoordinatorDerivedTaskSupersedePreservesRunningLease(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "derived-coalesce")
	_, err := coordinator.pool.Exec(ctx,
		`INSERT INTO `+coordinator.table("derived_tasks")+` (
			namespace, tenant_id, task_type, target_version, next_attempt_at
		) VALUES ($1,'tenant-a',$2,1,now())`,
		coordinator.namespace, derivedTaskIndexes,
	)
	if err != nil {
		t.Fatalf("insert derived task: %v", err)
	}
	first, claimed, err := coordinator.ClaimDerivedTask(ctx, "worker-a", 5*time.Second)
	if err != nil || !claimed {
		t.Fatalf("first claim claimed=%v err=%v", claimed, err)
	}
	if err := coordinator.enqueueDerivedIndexes(ctx, nil, "tenant-a", 1); err != nil {
		t.Fatalf("enqueue same derived target: %v", err)
	}
	if renewed, err := coordinator.RenewDerivedTask(
		ctx, first, 5*time.Second,
	); err != nil || !renewed {
		t.Fatalf("same-target enqueue revoked owner renewed=%v err=%v", renewed, err)
	}
	if err := coordinator.enqueueDerivedIndexes(ctx, nil, "tenant-a", 2); err != nil {
		t.Fatalf("supersede derived task: %v", err)
	}
	if renewed, err := coordinator.RenewDerivedTask(
		ctx, first, 5*time.Second,
	); err != nil || !renewed {
		t.Fatalf("supersede revoked active owner renewed=%v err=%v", renewed, err)
	}
	if _, claimed, err := coordinator.ClaimDerivedTask(
		ctx, "worker-b", time.Second,
	); err != nil || claimed {
		t.Fatalf("claim while first rebuild runs claimed=%v err=%v", claimed, err)
	}
	if err := coordinator.CompleteDerivedTask(ctx, first, 1); err != nil {
		t.Fatalf("complete first derived task: %v", err)
	}
	second, claimed, err := coordinator.ClaimDerivedTask(ctx, "worker-b", time.Second)
	if err != nil || !claimed || second.TargetVersion != 2 {
		t.Fatalf("claim coalesced follow-up job=%#v claimed=%v err=%v", second, claimed, err)
	}
}

func TestPostgresCoordinatorTaskLeaseFencing(t *testing.T) {
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, "task-leases")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = coordinator.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
		coordinator.Close()
	})
	first, acquired, err := coordinator.AcquireTaskLease(ctx, "tenant-a", "compact", "writer-a", time.Second)
	if err != nil || !acquired {
		t.Fatalf("first lease acquired=%v err=%v", acquired, err)
	}
	current, exists, err := coordinator.TaskLease(
		ctx, "tenant-a", "compact",
	)
	if err != nil || !exists ||
		current.OwnerToken != first.OwnerToken ||
		current.FenceEpoch != first.FenceEpoch {
		t.Fatalf(
			"read first lease exists=%v lease=%#v err=%v",
			exists,
			current,
			err,
		)
	}
	if _, acquired, err := coordinator.AcquireTaskLease(ctx, "tenant-a", "compact", "writer-b", time.Second); err != nil || acquired {
		t.Fatalf("competing lease acquired=%v err=%v", acquired, err)
	}
	if err := coordinator.ReleaseTaskLease(ctx, first); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	if _, exists, err := coordinator.TaskLease(
		ctx, "tenant-a", "compact",
	); err != nil || exists {
		t.Fatalf("released lease exists=%v err=%v", exists, err)
	}
	second, acquired, err := coordinator.AcquireTaskLease(ctx, "tenant-a", "compact", "writer-b", time.Second)
	if err != nil || !acquired || second.FenceEpoch <= first.FenceEpoch {
		t.Fatalf("takeover lease=%#v acquired=%v err=%v", second, acquired, err)
	}
	if err := coordinator.ReleaseTaskLease(ctx, first); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale release err = %v, want ErrConflict", err)
	}
	if _, renewed, err := coordinator.RenewTaskLease(ctx, first, time.Second); err != nil || renewed {
		t.Fatalf("stale renew renewed=%v err=%v", renewed, err)
	}
	if err := coordinator.ReleaseTaskLease(ctx, second); err != nil {
		t.Fatalf("release takeover lease: %v", err)
	}
}

func intPtr(value int) *int {
	return &value
}
