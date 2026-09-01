package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorCompactPurgeAndRecreate(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "lifecycle")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)

	first, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:old", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "reused-after-purge"})
	if err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	beforeCompact, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("head before compact exists=%v err=%v", exists, err)
	}
	compacted, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	afterCompact, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after compact: %v", err)
	}
	if compacted.Version != first.Version ||
		afterCompact.GraphVersion != beforeCompact.GraphVersion ||
		afterCompact.Revision != beforeCompact.Revision+1 {
		t.Fatalf("compact head before=%#v after=%#v manifest=%#v", beforeCompact, afterCompact, compacted)
	}

	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	softDeleted, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("soft-deleted head: %v", err)
	}
	if softDeleted.Revision != afterCompact.Revision ||
		softDeleted.Generation <= afterCompact.Generation ||
		softDeleted.Status != TenantStatusDeleted {
		t.Fatalf("soft-deleted head = %#v, compact head = %#v", softDeleted, afterCompact)
	}
	if _, err := store.PurgeTenant(ctx, "tenant-a", false); err != nil {
		t.Fatalf("purge: %v", err)
	}
	purged, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("purged head: %v", err)
	}
	status, err := coordinator.Status(ctx)
	if err != nil {
		t.Fatalf("coordinator status after purge: %v", err)
	}
	if purged.Status != TenantStatusDeleted ||
		purged.Generation <= softDeleted.Generation ||
		status.OutboxBacklog != 0 ||
		status.DerivedBacklog != 0 {
		t.Fatalf("purged head=%#v status=%#v", purged, status)
	}

	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	recreated, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recreated head: %v", err)
	}
	if recreated.Status != TenantStatusActive ||
		recreated.Generation <= purged.Generation ||
		recreated.GraphVersion != 0 ||
		recreated.Revision != purged.Revision+1 {
		t.Fatalf("recreated head=%#v purged=%#v", recreated, purged)
	}
	second, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}},
	}, CommitOptions{IdempotencyKey: "reused-after-purge"})
	if err != nil {
		t.Fatalf("reuse idempotency key in new generation: %v", err)
	}
	if second.IdempotentReplay || second.Version != 1 {
		t.Fatalf("new-generation commit = %#v", second)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recreated tenant: %v", err)
	}
	if _, ok := g.GetEntity("host:old"); ok {
		t.Fatal("recreated tenant retained an entity from the purged generation")
	}
	if _, ok := g.GetEntity("host:new"); !ok {
		t.Fatal("recreated tenant is missing the new-generation entity")
	}
}

func TestPostgresCoordinatorCompactSkipsIngestBarrierDuringRecovery(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "compact-ingest-recovery",
	)
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	barrierErr := errors.New("ingest barrier must not run during compact recovery")
	barrierCalls := 0
	store.SetIngestBarrier(func(context.Context, string) error {
		barrierCalls++
		return barrierErr
	})

	compacted, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("coordinated compact with pending ingest recovery: %v", err)
	}
	if barrierCalls != 0 {
		t.Fatalf("ingest barrier calls = %d, want 0", barrierCalls)
	}
	if compacted.SnapshotVersion != 1 || manifestCommitTailLength(compacted) != 0 {
		t.Fatalf(
			"compacted snapshot/tail = %d/%d, want 1/0",
			compacted.SnapshotVersion,
			manifestCommitTailLength(compacted),
		)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load compacted tenant: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("loaded version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:seed"); !ok {
		t.Fatal("compacted graph is missing host:seed")
	}
}

func TestPostgresCoordinatorForcePurgeFencesTenantBeforeIngestBarrier(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "purge-lifecycle-priority")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	barrierCalls := 0
	store.SetIngestBarrier(func(barrierCtx context.Context, tenantID string) error {
		barrierCalls++
		head, exists, err := coordinator.Head(barrierCtx, tenantID)
		if err != nil {
			return err
		}
		if !exists || head.Status != TenantStatusDeleted {
			return fmt.Errorf("ingest barrier observed unfenced head: exists=%v head=%#v", exists, head)
		}
		lockCtx, cancel := context.WithTimeout(barrierCtx, time.Second)
		defer cancel()
		unlock, err := store.lockTenantForeground(lockCtx, tenantID)
		if err != nil {
			return fmt.Errorf("ingest barrier blocked by purge maintenance lock: %w", err)
		}
		unlock()
		return nil
	})

	if _, err := store.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("force purge: %v", err)
	}
	if barrierCalls != 1 {
		t.Fatalf("ingest barrier calls = %d, want 1", barrierCalls)
	}
}

func TestPostgresCoordinatorBootstrapAndMarker(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "bootstrap")
	objects := NewMemoryStore()
	legacy := NewTenantStore(objects, "test")
	if _, err := legacy.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "document:a", Kind: "document"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed legacy tenant: %v", err)
	}
	if _, err := legacy.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "import", Priority: 7}},
	}); err != nil {
		t.Fatalf("seed source policy: %v", err)
	}

	dryRun, err := legacy.BootstrapCoordinator(ctx, coordinator, true)
	if err != nil {
		t.Fatalf("bootstrap dry-run: %v", err)
	}
	if len(dryRun.Tenants) != 1 || dryRun.Tenants[0].AlreadyExists {
		t.Fatalf("bootstrap dry-run = %#v", dryRun)
	}
	if _, exists, err := coordinator.Head(ctx, "tenant-a"); err != nil || exists {
		t.Fatalf("dry-run head exists=%v err=%v", exists, err)
	}
	if err := legacy.EnsureLocalWriterAllowed(ctx); err != nil {
		t.Fatalf("dry-run wrote a coordination marker: %v", err)
	}

	applied, err := legacy.BootstrapCoordinator(ctx, coordinator, false)
	if err != nil {
		t.Fatalf("bootstrap apply: %v", err)
	}
	if len(applied.Tenants) != 1 {
		t.Fatalf("bootstrap apply = %#v", applied)
	}
	if err := legacy.EnsureLocalWriterAllowed(ctx); err == nil {
		t.Fatal("local writer remained enabled after PostgreSQL bootstrap")
	}
	postgres := NewTenantStore(objects, "test")
	postgres.SetCoordinator(coordinator)
	if err := postgres.EnsurePostgresMarker(ctx); err != nil {
		t.Fatalf("postgres marker: %v", err)
	}
	g, manifest, err := postgres.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load bootstrapped tenant: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("bootstrapped manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("document:a"); !ok {
		t.Fatal("bootstrapped graph is missing document:a")
	}
	policy, configured, err := postgres.GetSourcePolicy(ctx, "tenant-a")
	if err != nil || !configured || len(policy.Sources) != 1 || policy.Sources[0].Priority != 7 {
		t.Fatalf("bootstrapped source policy configured=%v policy=%#v err=%v", configured, policy, err)
	}
	repeated, err := legacy.BootstrapCoordinator(ctx, coordinator, false)
	if err != nil || len(repeated.Tenants) != 1 || !repeated.Tenants[0].AlreadyExists {
		t.Fatalf("repeated bootstrap = %#v err=%v", repeated, err)
	}
}

func TestPostgresCoordinatorUnavailableKeepsCachedRead(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "cached-read")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	cache := NewReaderCache(store, time.Millisecond)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("prime reader cache: %v", err)
	}
	coordinator.Close()
	time.Sleep(2 * time.Millisecond)

	g, manifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cached read during coordinator outage: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("cached manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("cached graph is missing host:a")
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); !errors.Is(err, ErrCoordinatorUnavailable) {
		t.Fatalf("write during coordinator outage = %v, want ErrCoordinatorUnavailable", err)
	}
	if _, _, err := cache.LoadAtLeast(ctx, "tenant-a", 2); !errors.Is(err, ErrCoordinatorUnavailable) {
		t.Fatalf("uncached version floor error = %v, want ErrCoordinatorUnavailable", err)
	}
}

func TestPostgresCoordinatorReaderCacheTracksHeadRevisionAcrossRecreate(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "reader-cache-generation")
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	writer.SetCoordinator(coordinator)
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:old", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed old generation: %v", err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:old-2", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance old generation: %v", err)
	}

	reader := NewTenantStore(objects, "test")
	reader.SetCoordinator(coordinator)
	cache := NewReaderCache(reader, time.Hour)
	oldGraph, oldManifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cache old generation: %v", err)
	}
	if oldManifest.Version != 2 {
		t.Fatalf("old manifest version = %d, want 2", oldManifest.Version)
	}
	if _, ok := oldGraph.GetEntity("host:old"); !ok {
		t.Fatal("old generation entity is missing")
	}

	lifecycleWriter := NewTenantStore(objects, "test")
	lifecycleWriter.SetCoordinator(coordinator)
	if _, err := lifecycleWriter.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	if _, err := lifecycleWriter.PurgeTenant(ctx, "tenant-a", false); err != nil {
		t.Fatalf("purge old generation: %v", err)
	}
	if _, err := lifecycleWriter.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}
	if _, err := lifecycleWriter.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed new generation: %v", err)
	}

	writerGraph, writerManifest, err := writer.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recreated tenant through stale writer cache: %v", err)
	}
	if writerManifest.Version != 1 {
		t.Fatalf("writer manifest version = %d, want reset graph version 1",
			writerManifest.Version)
	}
	if _, ok := writerGraph.GetEntity("host:old"); ok {
		t.Fatal("writer cache retained an entity from the old tenant generation")
	}
	if _, ok := writerGraph.GetEntity("host:new"); !ok {
		t.Fatal("writer cache did not load the recreated tenant generation")
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("refresh stale writer cache through no-op commit: %v", err)
	}
	writerCached, ok := writer.getWriteCache("tenant-a")
	if !ok || writerCached.Manifest.Version != 1 {
		t.Fatalf("writer cache after generation reset = %#v, present=%v", writerCached.Manifest, ok)
	}
	if _, ok := writerCached.Graph.GetEntity("host:old"); ok {
		t.Fatal("monotonic writer cache kept the numerically newer old generation")
	}

	newGraph, newManifest, err := cache.Refresh(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("refresh recreated tenant: %v", err)
	}
	if newManifest.Version != 1 {
		t.Fatalf("recreated manifest version = %d, want reset graph version 1",
			newManifest.Version)
	}
	if _, ok := newGraph.GetEntity("host:old"); ok {
		t.Fatal("reader cache retained an entity from the old tenant generation")
	}
	if _, ok := newGraph.GetEntity("host:new"); !ok {
		t.Fatal("reader cache did not load the recreated tenant generation")
	}
}

func TestPostgresCoordinatorGCRemovesOnlyUnreachableCandidates(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "gc-roots")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := store.PutSourcePolicy(ctx, "tenant-a", graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "gc-test", Priority: 1}},
	}); err != nil {
		t.Fatalf("seed write context: %v", err)
	}
	if _, err := store.SyncLegacyManifests(ctx); err != nil {
		t.Fatalf("sync legacy manifest: %v", err)
	}
	head, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load head: %v", err)
	}
	current, _, err := store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load current manifest: %v", err)
	}
	candidate := current
	candidate.UpdatedAt = time.Now().UTC().Add(-2 * coordinatorCandidateGracePeriod)
	candidateData, err := marshalParquetManifest(ctx, candidate)
	if err != nil {
		t.Fatalf("marshal candidate manifest: %v", err)
	}
	candidateHash := objectContentHash(candidateData)
	candidateKey := store.coordinatorManifestKey(
		"tenant-a", candidate.Version, head.Revision, candidateHash,
	)
	if err := store.putImmutableCoordinatorObject(ctx, candidateKey, candidateData); err != nil {
		t.Fatalf("write candidate manifest: %v", err)
	}
	contextCandidate := emptyWriteContext("tenant-a")
	contextCandidate.Revision = head.WriteContextRevision
	contextCandidate.UpdatedAt = time.Now().UTC().Add(-2 * coordinatorCandidateGracePeriod)
	contextData, err := json.Marshal(contextCandidate)
	if err != nil {
		t.Fatalf("marshal context candidate: %v", err)
	}
	contextHash := objectContentHash(contextData)
	contextKey := store.coordinatorWriteContextKey("tenant-a", contextCandidate.Revision, contextHash)
	if err := store.putImmutableCoordinatorObject(ctx, contextKey, contextData); err != nil {
		t.Fatalf("write context candidate: %v", err)
	}
	futureManifestKey := store.coordinatorManifestKey(
		"tenant-a", candidate.Version, head.Revision+1, candidateHash,
	)
	if err := store.putImmutableCoordinatorObject(
		ctx, futureManifestKey, candidateData,
	); err != nil {
		t.Fatalf("write future manifest candidate: %v", err)
	}
	futureContext := contextCandidate
	futureContext.Revision = head.WriteContextRevision + 1
	futureContextData, err := json.Marshal(futureContext)
	if err != nil {
		t.Fatalf("marshal future context candidate: %v", err)
	}
	futureContextHash := objectContentHash(futureContextData)
	futureContextKey := store.coordinatorWriteContextKey(
		"tenant-a", futureContext.Revision, futureContextHash,
	)
	if err := store.putImmutableCoordinatorObject(
		ctx, futureContextKey, futureContextData,
	); err != nil {
		t.Fatalf("write future context candidate: %v", err)
	}

	report, err := store.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1})
	if err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if report.DeletedCoordinatorManifests != 1 || report.DeletedWriteContexts != 1 {
		t.Fatalf("gc report = %#v", report)
	}
	if _, err := objects.Get(ctx, candidateKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("candidate manifest remains: %v", err)
	}
	if _, err := objects.Get(ctx, contextKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("candidate write-context remains: %v", err)
	}
	if _, err := objects.Get(ctx, futureManifestKey); err != nil {
		t.Fatalf("GC removed publishable future manifest: %v", err)
	}
	if _, err := objects.Get(ctx, futureContextKey); err != nil {
		t.Fatalf("GC removed publishable future write-context: %v", err)
	}
	if _, err := objects.Get(ctx, head.ManifestKey); err != nil {
		t.Fatalf("gc removed authoritative head manifest: %v", err)
	}
}

func TestPostgresCoordinatorCloneAndRestorePublishAuthoritativeHeads(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "clone-restore")
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)

	quota := 100
	policy := graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 100}},
	}
	if _, err := store.CreateTenant(ctx, "tenant-source", TenantCreateOptions{
		Config:       &TenantConfig{Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota}},
		SourcePolicy: &policy,
	}); err != nil {
		t.Fatalf("create source: %v", err)
	}
	for _, id := range []string{"host:a", "host:b"} {
		if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: id, Kind: "host"}},
		}, CommitOptions{}); err != nil {
			t.Fatalf("commit source entity %q: %v", id, err)
		}
	}

	clone, err := store.CloneTenant(ctx, "tenant-source", TenantCloneOptions{
		TargetTenantID: "tenant-clone",
	})
	if err != nil {
		t.Fatalf("clone tenant: %v", err)
	}
	cloneHead, exists, err := coordinator.Head(ctx, "tenant-clone")
	if err != nil || !exists {
		t.Fatalf("clone head exists=%v err=%v", exists, err)
	}
	if clone.ManifestVersion != 2 ||
		cloneHead.GraphVersion != 2 ||
		cloneHead.Status != TenantStatusActive ||
		cloneHead.WriteContextRevision != 1 {
		t.Fatalf("clone=%#v head=%#v", clone, cloneHead)
	}
	cloneGraph, _, err := store.Load(ctx, "tenant-clone")
	if err != nil {
		t.Fatalf("load clone: %v", err)
	}
	if _, ok := cloneGraph.GetEntity("host:a"); !ok {
		t.Fatal("clone is missing source data")
	}
	if _, configured, err := store.GetTenantConfig(ctx, "tenant-clone"); err != nil || !configured {
		t.Fatalf("clone config configured=%v err=%v", configured, err)
	}

	backup, err := store.StartTask(ctx, "tenant-source", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-source", backup.ID)
	if backup.Status != TaskStatusSucceeded {
		t.Fatalf("backup task=%#v", backup)
	}
	backupKey, _ := backup.Result["backup_manifest_key"].(string)
	if backupKey == "" {
		t.Fatalf("backup manifest key missing: %#v", backup.Result)
	}

	restore, err := store.StartTask(ctx, "tenant-restored", TaskTypeTenantRestore, map[string]any{
		"backup_key": backupKey,
	})
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(t, ctx, store, "tenant-restored", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore task=%#v", restore)
	}
	restoredHead, exists, err := coordinator.Head(ctx, "tenant-restored")
	if err != nil || !exists {
		t.Fatalf("restored head exists=%v err=%v", exists, err)
	}
	if restoredHead.GraphVersion != 2 ||
		restoredHead.Status != TenantStatusActive ||
		restoredHead.WriteContextRevision != 1 {
		t.Fatalf("restored head=%#v", restoredHead)
	}
	restoredGraph, _, err := store.Load(ctx, "tenant-restored")
	if err != nil {
		t.Fatalf("load restored tenant: %v", err)
	}
	if _, ok := restoredGraph.GetEntity("host:b"); !ok {
		t.Fatal("restored tenant is missing source data")
	}

	if _, err := store.Commit(ctx, "tenant-restored", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:discard", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("mutate restore target: %v", err)
	}
	beforeOverwrite, _, err := coordinator.Head(ctx, "tenant-restored")
	if err != nil {
		t.Fatalf("head before overwrite: %v", err)
	}
	overwrite, err := store.StartTask(ctx, "tenant-restored", TaskTypeTenantRestore, map[string]any{
		"backup_key": backupKey,
		"overwrite":  true,
	})
	if err != nil {
		t.Fatalf("start overwrite restore: %v", err)
	}
	overwrite = waitForTaskAcrossPurge(t, ctx, store, "tenant-restored", overwrite.ID)
	if overwrite.Status != TaskStatusSucceeded {
		t.Fatalf("overwrite restore task=%#v", overwrite)
	}
	afterOverwrite, _, err := coordinator.Head(ctx, "tenant-restored")
	if err != nil {
		t.Fatalf("head after overwrite: %v", err)
	}
	if afterOverwrite.Generation <= beforeOverwrite.Generation ||
		afterOverwrite.GraphVersion != 2 ||
		afterOverwrite.Status != TenantStatusActive ||
		afterOverwrite.WriteContextRevision != 1 {
		t.Fatalf("head before=%#v after=%#v", beforeOverwrite, afterOverwrite)
	}
	restoredGraph, _, err = store.Load(ctx, "tenant-restored")
	if err != nil {
		t.Fatalf("load overwrite restore: %v", err)
	}
	if _, ok := restoredGraph.GetEntity("host:discard"); ok {
		t.Fatal("overwrite restore retained data from the prior generation")
	}

	if _, err := store.SyncLegacyManifests(ctx); err != nil {
		t.Fatalf("sync legacy manifests: %v", err)
	}
	legacy := NewTenantStore(objects, "test")
	_, legacyManifest, err := legacy.Load(ctx, "tenant-restored")
	if err != nil {
		t.Fatalf("load restored tenant through 1.0 path: %v", err)
	}
	if legacyManifest.Version != afterOverwrite.GraphVersion {
		t.Fatalf("legacy manifest version=%d head version=%d",
			legacyManifest.Version, afterOverwrite.GraphVersion)
	}
}

func TestPostgresCoordinatorCloneActivatesAfterSnapshotIsComplete(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "clone-atomic-activation",
	)
	objects := &blockingCloneSnapshotStore{
		ObjectStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	quota := 10
	if _, err := store.CreateTenant(
		ctx,
		"tenant-source",
		TenantCreateOptions{
			Config: &TenantConfig{
				Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota},
			},
		},
	); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-source", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	objects.arm("test/tenants/tenant-clone/snapshots/sharded/")

	result := make(chan error, 1)
	go func() {
		_, err := store.CloneTenant(ctx, "tenant-source", TenantCloneOptions{
			TargetTenantID: "tenant-clone",
		})
		result <- err
	}()
	select {
	case <-objects.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("clone did not reach snapshot publication")
	}
	if head, exists, err := coordinator.Head(
		ctx, "tenant-clone",
	); err != nil {
		t.Fatalf("load target head during clone: %v", err)
	} else if exists && head.Status == TenantStatusActive {
		t.Fatalf("clone exposed active target before snapshot completion: %#v", head)
	}
	close(objects.release)
	if err := <-result; err != nil {
		t.Fatalf("clone tenant: %v", err)
	}

	head, exists, err := coordinator.Head(ctx, "tenant-clone")
	if err != nil || !exists {
		t.Fatalf("clone head exists=%v err=%v", exists, err)
	}
	if head.Status != TenantStatusActive ||
		head.GraphVersion != 1 ||
		head.WriteContextRevision != 1 {
		t.Fatalf("clone head=%#v", head)
	}
	g, _, err := store.Load(ctx, "tenant-clone")
	if err != nil {
		t.Fatalf("load clone: %v", err)
	}
	if _, ok := g.GetEntity("host:source"); !ok {
		t.Fatal("clone is missing source data")
	}
	if config, configured, err := store.GetTenantConfig(
		ctx, "tenant-clone",
	); err != nil || !configured ||
		config.Quota.MaxEntitiesPerTenant == nil ||
		*config.Quota.MaxEntitiesPerTenant != quota {
		t.Fatalf("clone config=%#v configured=%v err=%v", config, configured, err)
	}
}

func TestPostgresCoordinatorCreateActivatesWithWriteContext(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "create-atomic-context",
	)
	objects := &blockingCloneSnapshotStore{
		ObjectStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	store := NewTenantStore(objects, "test")
	store.SetCoordinator(coordinator)
	quota := 10
	objects.arm("test/tenants/tenant-a/coordination/write-contexts/")

	result := make(chan error, 1)
	go func() {
		_, err := store.CreateTenant(
			ctx,
			"tenant-a",
			TenantCreateOptions{
				Config: &TenantConfig{
					Quota: TenantQuotaConfig{
						MaxEntitiesPerTenant: &quota,
					},
				},
			},
		)
		result <- err
	}()
	select {
	case <-objects.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("create did not reach write-context publication")
	}
	if head, exists, err := coordinator.Head(ctx, "tenant-a"); err != nil {
		t.Fatalf("load target head during create: %v", err)
	} else if exists {
		t.Fatalf("create exposed head before write-context completion: %#v", head)
	}
	close(objects.release)
	if err := <-result; err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("created head exists=%v err=%v", exists, err)
	}
	if head.Status != TenantStatusActive || head.WriteContextRevision != 1 {
		t.Fatalf("created head=%#v", head)
	}
	config, configured, err := store.GetTenantConfig(ctx, "tenant-a")
	if err != nil || !configured ||
		config.Quota.MaxEntitiesPerTenant == nil ||
		*config.Quota.MaxEntitiesPerTenant != quota {
		t.Fatalf("created config=%#v configured=%v err=%v", config, configured, err)
	}
}

type blockingCloneSnapshotStore struct {
	ObjectStore
	mu      sync.Mutex
	prefix  string
	armed   bool
	blocked bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingCloneSnapshotStore) arm(prefix string) {
	s.mu.Lock()
	s.prefix = prefix
	s.armed = true
	s.mu.Unlock()
}

func (s *blockingCloneSnapshotStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	s.mu.Lock()
	block := s.armed && !s.blocked && strings.HasPrefix(key, s.prefix)
	if block {
		s.blocked = true
	}
	s.mu.Unlock()
	if block {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ObjectMeta{Key: key}, ctx.Err()
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func TestPostgresCoordinatorTenantMigrationIsFencedAndPublishesHead(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "tenant-migration")
	source := NewTenantStore(NewMemoryStore(), "source")
	quota := 50
	policy := graph.SourcePolicy{
		Sources: []graph.SourcePolicyItem{{Name: "migration", Priority: 25}},
	}
	if _, err := source.CreateTenant(ctx, "tenant-a", TenantCreateOptions{
		Config:       &TenantConfig{Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota}},
		SourcePolicy: &policy,
	}); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:migrated", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit source: %v", err)
	}

	targetObjects := NewMemoryStore()
	target := NewTenantStore(targetObjects, "target")
	target.SetCoordinator(coordinator)
	blocker, acquired, err := coordinator.AcquireTaskLease(
		ctx, "tenant-a", coordinatorMigrationTaskType, "blocker", time.Minute,
	)
	if err != nil || !acquired {
		t.Fatalf("acquire blocker lease acquired=%v err=%v", acquired, err)
	}
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); !errors.Is(err, ErrTaskLeaseHeld) {
		t.Fatalf("migration with held lease err=%v", err)
	}
	if err := coordinator.ReleaseTaskLease(ctx, blocker); err != nil {
		t.Fatalf("release blocker lease: %v", err)
	}

	report, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	)
	if err != nil {
		t.Fatalf("migrate tenant: %v", err)
	}
	if report.Copied == 0 || report.Objects == 0 {
		t.Fatalf("migration report=%#v", report)
	}
	head, exists, err := coordinator.Head(ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("migration head exists=%v err=%v", exists, err)
	}
	if head.Status != TenantStatusActive ||
		head.GraphVersion != 1 ||
		head.Generation != 1 {
		t.Fatalf("migration head=%#v", head)
	}
	g, manifest, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated tenant: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("migrated version=%d", manifest.Version)
	}
	if _, ok := g.GetEntity("host:migrated"); !ok {
		t.Fatal("migrated entity is missing")
	}
	if config, configured, err := target.GetTenantConfig(ctx, "tenant-a"); err != nil ||
		!configured ||
		config.Quota.MaxEntitiesPerTenant == nil ||
		*config.Quota.MaxEntitiesPerTenant != quota {
		t.Fatalf("migrated config configured=%v config=%#v err=%v",
			configured, config, err)
	}
	if migratedPolicy, configured, err := target.GetSourcePolicy(ctx, "tenant-a"); err != nil ||
		!configured ||
		migratedPolicy.PriorityFor("migration", 0) != 25 {
		t.Fatalf("migrated policy configured=%v policy=%#v err=%v",
			configured, migratedPolicy, err)
	}

	if _, err := target.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:discard", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("mutate migration target: %v", err)
	}
	beforeOverwrite, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head before overwrite migration: %v", err)
	}
	if _, err := CopyTenantObjects(
		ctx,
		source,
		"tenant-a",
		target,
		"tenant-a",
		TenantMigrationOptions{Overwrite: true},
	); err != nil {
		t.Fatalf("overwrite migration: %v", err)
	}
	afterOverwrite, _, err := coordinator.Head(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("head after overwrite migration: %v", err)
	}
	if afterOverwrite.Generation <= beforeOverwrite.Generation ||
		afterOverwrite.GraphVersion != 1 {
		t.Fatalf("head before=%#v after=%#v", beforeOverwrite, afterOverwrite)
	}
	g, _, err = target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load overwritten migration: %v", err)
	}
	if _, ok := g.GetEntity("host:discard"); ok {
		t.Fatal("overwrite migration retained the previous generation")
	}
}

func newPostgresIntegrationCoordinator(
	t *testing.T,
	namespace string,
) (context.Context, *PostgresCoordinator) {
	t.Helper()
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GRAPHDB_TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	schema := fmt.Sprintf("graphdb_test_%d", time.Now().UnixNano())
	coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, namespace)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		coordinator.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		admin, err := NewPostgresCoordinator(context.Background(), dsn, schema, namespace+"-cleanup")
		if err == nil {
			_, _ = admin.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+schema+`" CASCADE`)
			admin.Close()
		}
		coordinator.Close()
	})
	return ctx, coordinator
}
