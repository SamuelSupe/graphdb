package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestRestoreDrillRejectsExistingTargetWithoutChangingIt(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")
	target := NewTenantStore(objects, "drill")
	seedRestoreDrillTenant(t, ctx, target, "tenant-existing", "host:sentinel")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-existing",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed || !strings.Contains(task.Error, "not empty") {
		t.Fatalf("restore drill task = %#v", task)
	}
	g, manifest, err := target.Load(ctx, "tenant-existing")
	if err != nil {
		t.Fatalf("load existing target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("existing target version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:sentinel"); !ok {
		t.Fatal("restore drill changed the existing target")
	}
}

func TestRestoreDrillRejectsLiveTenantNamespace(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, store, "tenant-a", "host:source")
	seedRestoreDrillTenant(t, ctx, store, "tenant-live", "host:sentinel")

	task, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "test",
		"target_tenant_id": "tenant-live",
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed || !strings.Contains(task.Error, "isolated") {
		t.Fatalf("restore drill task = %#v", task)
	}
	g, manifest, err := store.Load(ctx, "tenant-live")
	if err != nil {
		t.Fatalf("load live target: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("live target version = %d, want 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:sentinel"); !ok {
		t.Fatal("restore drill changed the live target")
	}
}

func TestRestoreDrillCleanupRejectsChangedOwnedTarget(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "drill")
	seedRestoreDrillTenant(t, ctx, store, "tenant-target", "host:a")
	now := time.Now().UTC()
	marker := Task{
		ID:         "drill-restore",
		TenantID:   "tenant-target",
		Type:       TaskTypeTenantRestore,
		Status:     TaskStatusSucceeded,
		Phase:      "done",
		OwnerID:    store.InstanceID,
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, marker); err != nil {
		t.Fatalf("save ownership marker: %v", err)
	}
	ownership, err := store.captureRestoreDrillOwnership(ctx, "tenant-target", marker.ID)
	if err != nil {
		t.Fatalf("capture ownership: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-target", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("change target after capture: %v", err)
	}
	lease, _, ok := store.getCachedWriterLeaseAny("tenant-target")
	if !ok {
		t.Fatal("target writer lease is not cached")
	}
	claim := restoreDrillClaim{fence: writerFenceRef{
		ownerID: lease.OwnerID,
		token:   lease.FenceToken,
		epoch:   lease.FenceEpoch,
	}}
	if _, _, err := store.cleanupOwnedRestoreDrillTarget(
		ctx, "tenant-target", ownership, claim, true,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("cleanup err = %v, want ErrConflict", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-target")
	if err != nil {
		t.Fatalf("load changed target: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("changed target version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("rejected cleanup deleted the changed target")
	}
}

func TestRestoreDrillOwnershipUsesBoundedObjectPages(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(paged, "drill")
	tenantID := "tenant-target"
	prefix := store.tenantObjectPrefix(tenantID)
	for i := 0; i < objectPrefixScanPageSize+1; i++ {
		if err := base.Put(
			ctx,
			fmt.Sprintf("%sdata/item-%04d", prefix, i),
			[]byte("value"),
		); err != nil {
			t.Fatalf("put target object: %v", err)
		}
	}
	now := time.Now().UTC()
	marker := Task{
		ID:         "drill-restore",
		TenantID:   tenantID,
		Type:       TaskTypeTenantRestore,
		Status:     TaskStatusSucceeded,
		Phase:      "done",
		OwnerID:    store.InstanceID,
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, marker); err != nil {
		t.Fatalf("save ownership marker: %v", err)
	}
	ownership, err := store.captureRestoreDrillOwnership(ctx, tenantID, marker.ID)
	if err != nil {
		t.Fatalf("capture ownership: %v", err)
	}
	if ownership.objectCount != objectPrefixScanPageSize+2 {
		t.Fatalf("ownership object count = %d, want %d", ownership.objectCount, objectPrefixScanPageSize+2)
	}
	if ownership.fingerprint == "" {
		t.Fatal("ownership fingerprint is empty")
	}
	if paged.listCalls != 0 || paged.pageCalls < 2 {
		t.Fatalf("list calls=%d page calls=%d, want 0 and at least 2", paged.listCalls, paged.pageCalls)
	}
}

func TestRestoreDrillOwnedCleanupRemovesOnlyTargetTenant(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-cleanup",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded || task.Result["status"] != "passed" {
		t.Fatalf("restore drill task = %#v", task)
	}
	target := NewTenantStore(objects, "drill")
	if _, err := target.GetTenantInfo(ctx, "tenant-cleanup"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cleaned target err = %v, want ErrNotFound", err)
	}
	objectsLeft, err := objects.List(ctx, target.tenantObjectPrefix("tenant-cleanup"))
	if err != nil {
		t.Fatalf("list cleaned target: %v", err)
	}
	if len(objectsLeft) != 0 {
		t.Fatalf("cleaned target objects = %#v", objectsLeft)
	}
	purged, err := target.tenantPurgeTombstoneExists(ctx, "tenant-cleanup")
	if err != nil || purged {
		t.Fatalf("cleanup tombstone exists=%v err=%v", purged, err)
	}
}

func TestRestoreDrillCleanupFailureFailsTaskAndRetriesCleanup(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &failOnceTargetDeleteStore{
		ObjectStore: base,
		prefix:      "drill/tenants/tenant-cleanup/",
	}
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-cleanup",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed ||
		!strings.Contains(task.Error, "injected cleanup delete failure") {
		t.Fatalf("restore drill task = %#v", task)
	}
	remaining, err := base.List(ctx, objects.prefix)
	if err != nil {
		t.Fatalf("list retried cleanup target: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("retried cleanup left target objects: %#v", remaining)
	}
}

func TestRestoreDrillCleansOwnedTargetAfterPostRestoreFailure(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &failNthTargetListStore{
		ObjectStore: base,
		prefix:      "drill/tenants/tenant-cleanup/",
		failAt:      5,
	}
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-cleanup",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed ||
		!strings.Contains(task.Error, "injected restored usage failure") {
		t.Fatalf("restore drill task = %#v", task)
	}

	target := NewTenantStore(base, "drill")
	remaining, err := base.List(ctx, target.tenantObjectPrefix("tenant-cleanup"))
	if err != nil {
		t.Fatalf("list cleaned target: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("failed restore drill left target objects: %#v", remaining)
	}
	purged, err := target.tenantPurgeTombstoneExists(ctx, "tenant-cleanup")
	if err != nil || purged {
		t.Fatalf("cleanup tombstone exists=%v err=%v", purged, err)
	}
}

func TestRestoreDrillCleansClaimedTargetAfterPartialRestoreFailure(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &failTargetManifestPutStore{
		ObjectStore: base,
		key:         "drill/tenants/tenant-cleanup/manifest.parquet",
	}
	source := NewTenantStore(objects, "test")
	seedRestoreDrillTenant(t, ctx, source, "tenant-a", "host:source")

	task, err := source.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_prefix":    "drill",
		"target_tenant_id": "tenant-cleanup",
		"cleanup":          true,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForRestoreDrillTerminalTask(t, ctx, source, "tenant-a", task.ID)
	if task.Status != TaskStatusFailed ||
		!strings.Contains(task.Error, "injected target manifest failure") {
		t.Fatalf("restore drill task = %#v", task)
	}
	remaining, err := base.List(ctx, "drill/tenants/tenant-cleanup/")
	if err != nil {
		t.Fatalf("list cleaned target: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("partial restore left target objects: %#v", remaining)
	}
}

func TestRestoreDrillClaimFailureReleasesWriterLease(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &failNthTargetListStore{
		ObjectStore: base,
		prefix:      "drill/tenants/tenant-cleanup/",
		failAt:      2,
	}
	target := NewTenantStore(objects, "drill")

	if _, err := target.claimRestoreDrillTarget(
		ctx, "tenant-cleanup",
	); err == nil || !strings.Contains(err.Error(), "injected restored usage failure") {
		t.Fatalf("claim restore drill target err = %v", err)
	}
	remaining, err := base.List(ctx, "drill/tenants/tenant-cleanup/")
	if err != nil {
		t.Fatalf("list failed claim target: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("failed claim left target objects: %#v", remaining)
	}
}

func seedRestoreDrillTenant(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, entityID string) {
	t.Helper()
	if _, err := store.CreateTenant(ctx, tenantID, TenantCreateOptions{}); err != nil {
		t.Fatalf("create %s: %v", tenantID, err)
	}
	if _, err := store.Commit(ctx, tenantID, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: entityID, Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit %s: %v", tenantID, err)
	}
}

func waitForRestoreDrillTerminalTask(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, taskID string) Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(ctx, tenantID, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if task.Status != TaskStatusQueued && task.Status != TaskStatusRunning {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish", taskID)
	return Task{}
}

type failNthTargetListStore struct {
	ObjectStore
	mu     sync.Mutex
	prefix string
	count  int
	failAt int
}

func (s *failNthTargetListStore) List(
	ctx context.Context,
	prefix string,
) ([]ObjectInfo, error) {
	if prefix == s.prefix {
		s.mu.Lock()
		s.count++
		fail := s.count == s.failAt
		s.mu.Unlock()
		if fail {
			return nil, errors.New("injected restored usage failure")
		}
	}
	return s.ObjectStore.List(ctx, prefix)
}

type failTargetManifestPutStore struct {
	ObjectStore
	mu     sync.Mutex
	key    string
	failed bool
}

type failOnceTargetDeleteStore struct {
	ObjectStore
	mu     sync.Mutex
	prefix string
	failed bool
}

func (s *failOnceTargetDeleteStore) DeleteConditional(
	ctx context.Context,
	key string,
	condition PutCondition,
) error {
	s.mu.Lock()
	fail := strings.HasPrefix(key, s.prefix) && !s.failed
	if fail {
		s.failed = true
	}
	s.mu.Unlock()
	if fail {
		return errors.New("injected cleanup delete failure")
	}
	return s.ObjectStore.DeleteConditional(ctx, key, condition)
}

func (s *failTargetManifestPutStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	if key == s.key {
		s.mu.Lock()
		fail := !s.failed
		s.failed = true
		s.mu.Unlock()
		if fail {
			return ObjectMeta{Key: key}, errors.New("injected target manifest failure")
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}
