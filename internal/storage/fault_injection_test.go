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

func TestFaultInjectionRegressionMatrix(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"object_store_timeout_keeps_failed_commit_invisible", testFaultObjectStoreTimeoutKeepsFailedCommitInvisible},
		{"partial_5xx_on_index_publish_keeps_commit_and_marks_index_stale", testFaultPartial5xxKeepsCommitAndMarksIndexStale},
		{"etag_cas_commit_collision_retries_without_duplicate_visibility", testFaultCASCommitCollisionRetries},
		{"inconsistent_list_skips_vanished_orphan_commit", testFaultInconsistentListSkipsVanishedCommit},
		{"reader_old_catalog_is_not_used_for_current_version", testFaultReaderOldCatalogRejected},
		{"task_interrupt_retry_reuses_checkpointed_result", testFaultTaskInterruptRetryReusesCheckpoint},
		{"process_crash_after_commit_object_recovers_on_restart", testFaultProcessCrashRecovery},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testFaultObjectStoreTimeoutKeepsFailedCommitInvisible(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	faults := newFaultMatrixStore(base, faultMatrixRule{
		name:      "manifest-timeout",
		op:        "put_conditional",
		contains:  "/manifest.parquet",
		err:       context.DeadlineExceeded,
		remaining: -1,
	})
	store.Objects = faults
	_, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commit err = %v, want deadline exceeded", err)
	}
	if faults.fired("manifest-timeout") == 0 {
		t.Fatal("timeout fault did not fire")
	}
	reader := NewTenantStore(base, "test")
	g, manifest, err := reader.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load after failed commit: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want old version 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); ok {
		t.Fatal("failed commit became visible")
	}
}

func testFaultPartial5xxKeepsCommitAndMarksIndexStale(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	faults := newFaultMatrixStore(base, faultMatrixRule{
		name:      "index-catalog-5xx",
		op:        "write",
		contains:  "/indexes/catalog.parquet",
		err:       ErrObjectStoreUnavailable,
		remaining: -1,
	})
	store.Objects = faults
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"}}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit with injected index 5xx: %v", err)
	}
	if result.Version != 2 || len(result.IndexWarnings) != 1 || !strings.Contains(result.IndexWarnings[0], "incremental index update failed") {
		t.Fatalf("commit result = %#v", result)
	}
	if faults.fired("index-catalog-5xx") == 0 {
		t.Fatal("index 5xx fault did not fire")
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("index health: %v", err)
	}
	if health.Status != "stale" {
		t.Fatalf("index health = %#v, want stale", health)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load committed graph: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want 2", manifest.Version)
	}
	if entity, ok := g.GetEntity("host:app-01"); !ok || entity.Fields["hostname"] != "app-02" {
		t.Fatalf("committed entity = %#v ok=%v", entity, ok)
	}
}

func testFaultCASCommitCollisionRetries(t *testing.T) {
	ctx := context.Background()
	faults := newFaultMatrixStore(NewMemoryStore(), faultMatrixRule{
		name:      "commit-if-none-match-conflict",
		op:        "put_conditional",
		contains:  "/commits/",
		err:       ErrConflict,
		remaining: 1,
	})
	store := NewTenantStore(faults, "test")
	store.MaxRetries = 2
	manifest, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit after CAS collision: %v", err)
	}
	if faults.fired("commit-if-none-match-conflict") != 1 {
		t.Fatalf("CAS fault fired %d times, want 1", faults.fired("commit-if-none-match-conflict"))
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(g.Entities) != 1 {
		t.Fatalf("entities = %#v, want exactly one visible entity", g.Entities)
	}
}

func testFaultInconsistentListSkipsVanishedCommit(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	orphan := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		ID:            "listed-then-vanished",
		TenantID:      "tenant-a",
		Version:       2,
		CreatedAt:     time.Now().UTC(),
		Mutations:     graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}}},
	}
	orphanKey := store.commitKey("tenant-a", orphan.Version, orphan.ID)
	if err := store.putCommitObjectIfAbsent(ctx, orphanKey, orphan); err != nil {
		t.Fatalf("put orphan commit: %v", err)
	}
	listFault := &deleteListedCommitStore{ObjectStore: base, key: orphanKey}
	store.Objects = listFault
	store.deleteWriteCache("tenant-a")
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !listFault.deleted {
		t.Fatal("list inconsistency fault did not delete the listed commit")
	}
	if report.Recovered != 0 || report.EndVersion != 1 {
		t.Fatalf("recovery report = %#v", report)
	}
}

func testFaultReaderOldCatalogRejected(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	oldCatalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	current, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current catalog: %v", err)
	}
	if current.Version != 2 {
		t.Fatalf("current catalog version = %d, want 2", current.Version)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: current.Version, Catalog: oldCatalog}
	if ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"}); err != nil || ok || len(ids) != 0 {
		t.Fatalf("old catalog lookup ids=%#v ok=%v err=%v, want unavailable", ids, ok, err)
	}
}

func testFaultTaskInterruptRetryReusesCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	exportTask, err := store.StartTask(ctx, "tenant-a", TaskTypeExportSnapshot, nil)
	if err != nil {
		t.Fatalf("start export task: %v", err)
	}
	exportTask = waitForTask(t, ctx, store, "tenant-a", exportTask.ID)
	if exportTask.ResultKey == "" {
		t.Fatalf("export task missing result key: %#v", exportTask)
	}
	now := time.Now().UTC()
	interrupted := Task{
		ID:         "fault-matrix-interrupted-export",
		TenantID:   "tenant-a",
		Type:       TaskTypeExportSnapshot,
		Status:     TaskStatusFailed,
		Phase:      TaskStatusFailed,
		Checkpoint: map[string]any{"result_key": exportTask.ResultKey},
		Error:      "simulated process stop after result write",
		StartedAt:  now,
		UpdatedAt:  now,
		FinishedAt: now,
	}
	if err := store.saveTask(ctx, interrupted); err != nil {
		t.Fatalf("save interrupted task: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", interrupted.ID)
	if err != nil {
		t.Fatalf("retry interrupted task: %v", err)
	}
	retry = waitForTask(t, ctx, store, "tenant-a", retry.ID)
	if retry.Status != TaskStatusSucceeded || retry.ResultKey != exportTask.ResultKey || retry.Result["resumed"] != true {
		t.Fatalf("retry task = %#v", retry)
	}
}

func testFaultProcessCrashRecovery(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	writer.LeaseTTL = time.Nanosecond
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	time.Sleep(time.Millisecond)
	crashed := graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		ID:            "crashed-after-commit-object",
		TenantID:      "tenant-a",
		Version:       2,
		CreatedAt:     time.Now().UTC(),
		Mutations:     graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:after-crash", Kind: "host"}}},
	}
	if err := writer.putCommitObjectIfAbsent(ctx, writer.commitKey("tenant-a", crashed.Version, crashed.ID), crashed); err != nil {
		t.Fatalf("write crashed commit object: %v", err)
	}
	restarted := NewTenantStore(base, "test")
	report, err := restarted.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("recover after restart: %v", err)
	}
	if report.Recovered != 1 || report.EndVersion != 2 {
		t.Fatalf("recovery report = %#v", report)
	}
	g, manifest, err := restarted.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recovered tenant: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:after-crash"); !ok {
		t.Fatal("crashed commit was not recovered")
	}
}

type faultMatrixRule struct {
	name      string
	op        string
	contains  string
	err       error
	remaining int
}

type faultMatrixStore struct {
	ObjectStore
	mu    sync.Mutex
	rules []faultMatrixRule
	hits  map[string]int
}

func newFaultMatrixStore(base ObjectStore, rules ...faultMatrixRule) *faultMatrixStore {
	return &faultMatrixStore{ObjectStore: base, rules: rules, hits: map[string]int{}}
}

func (s *faultMatrixStore) Put(ctx context.Context, key string, data []byte) error {
	if err := s.fail("put", key); err != nil {
		return err
	}
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *faultMatrixStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if err := s.fail("put_conditional", key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *faultMatrixStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.fail("get", key); err != nil {
		return nil, err
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *faultMatrixStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := s.fail("get_with_meta", key); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *faultMatrixStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := s.fail("list", prefix); err != nil {
		return nil, err
	}
	return s.ObjectStore.List(ctx, prefix)
}

func (s *faultMatrixStore) fail(op string, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.rules {
		rule := &s.rules[i]
		if !faultMatrixRuleMatches(*rule, op, key) {
			continue
		}
		if rule.remaining == 0 {
			continue
		}
		if rule.remaining > 0 {
			rule.remaining--
		}
		s.hits[rule.name]++
		return rule.err
	}
	return nil
}

func (s *faultMatrixStore) fired(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[name]
}

func faultMatrixRuleMatches(rule faultMatrixRule, op string, key string) bool {
	if rule.op != "" && rule.op != op {
		if rule.op != "write" || (op != "put" && op != "put_conditional") {
			return false
		}
	}
	return rule.contains == "" || strings.Contains(key, rule.contains)
}
