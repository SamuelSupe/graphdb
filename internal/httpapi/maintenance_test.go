package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestMaintenanceAutoCompactsLongCommitTail(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	autoCompact := true
	threshold := 2
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{AutoCompact: &autoCompact, CompactCommitTailThreshold: &threshold},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if report.Compacted != 1 || len(report.Errors) != 0 {
		t.Fatalf("maintenance report = %#v", report)
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(manifest.CommitKeys) != 0 || manifest.SnapshotKey == "" {
		t.Fatalf("manifest after compact = %#v", manifest)
	}
	if len(report.Tenants) != 1 || report.Tenants[0].CompactReason != "commit_tail" {
		t.Fatalf("tenant report = %#v", report.Tenants)
	}
}

func TestMaintenanceYieldsUntilTenantIngestIsIdle(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	autoCompact := true
	threshold := 1
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{
			AutoCompact:                &autoCompact,
			CompactCommitTailThreshold: &threshold,
		},
	}); err != nil {
		t.Fatal(err)
	}
	config := storage.DefaultIngestServiceConfig(t.TempDir())
	config.FlushInterval = time.Hour
	service, err := storage.OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(closeCtx); err != nil {
			t.Errorf("close ingest service: %v", err)
		}
	}()
	if _, err := service.Accept(ctx, "tenant-a", storage.IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []storage.IngestItem{{
			Entity: &graph.Entity{ID: "host:pending", Kind: "host"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if readiness := service.Readiness(); readiness.Pending != 0 {
		t.Fatalf("ingest readiness = %#v, want an idle queue", readiness)
	}

	report := (&Server{Store: store, Mode: "all", IngestService: service}).runMaintenanceOnce(ctx, time.Now().UTC())
	if report.Compacted != 0 || len(report.Errors) != 0 || len(report.Tenants) != 1 {
		t.Fatalf("maintenance report = %#v", report)
	}
	if report.Tenants[0].Skipped != "ingest_active" {
		t.Fatalf("tenant maintenance = %#v, want ingest_active", report.Tenants[0])
	}
}

func TestMaintenanceAutoCompactsWhenObjectCountThresholdExceeded(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	autoCompact := true
	tailThreshold := 1000
	objectThreshold := 1
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{
			AutoCompact:                 &autoCompact,
			CompactCommitTailThreshold:  &tailThreshold,
			CompactObjectCountThreshold: &objectThreshold,
		},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if report.Compacted != 1 || len(report.Errors) != 0 {
		t.Fatalf("maintenance report = %#v", report)
	}
	if len(report.Tenants) != 1 || report.Tenants[0].CompactReason != "object_count" {
		t.Fatalf("tenant report = %#v", report.Tenants)
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(manifest.CommitKeys) != 0 || manifest.SnapshotKey == "" {
		t.Fatalf("manifest after compact = %#v", manifest)
	}
}

func TestMaintenanceAutoCompactsSmallFiles(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	autoCompact := true
	tailThreshold := 1000
	smallObjects := 1
	smallBytes := int64(1 << 30)
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{
			AutoCompact:                &autoCompact,
			CompactCommitTailThreshold: &tailThreshold,
			SmallFileObjectThreshold:   &smallObjects,
			SmallFileBytesThreshold:    &smallBytes,
		},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if report.Compacted != 1 || len(report.Errors) != 0 {
		t.Fatalf("maintenance report = %#v", report)
	}
	if len(report.Tenants) != 1 || report.Tenants[0].CompactReason != "small_files" {
		t.Fatalf("tenant report = %#v", report.Tenants)
	}
}

func TestAutoCompactSkipsUsageForAlreadyCompactedManifest(t *testing.T) {
	server := &Server{}
	report := &MaintenanceReport{}
	manifest := storage.Manifest{
		TenantID:           "tenant-a",
		Version:            42,
		SnapshotVersion:    42,
		SnapshotKey:        "snapshot.parquet",
		SnapshotCatalogKey: "snapshot-catalog.parquet",
	}
	autoCompact := true
	objectThreshold := 1
	decision := server.autoCompactDecision(context.Background(), "tenant-a", manifest, storage.TenantMaintenanceConfig{
		AutoCompact:                 &autoCompact,
		CompactObjectCountThreshold: &objectThreshold,
	}, report)
	if decision.Compact || len(report.Errors) != 0 {
		t.Fatalf("auto compact decision = %#v report=%#v", decision, report)
	}
}

func TestMaintenanceReusesTenantUsageHTTPCache(t *testing.T) {
	ctx := context.Background()
	objects := &maintenanceCountingListStore{
		ObjectStore: storage.NewMemoryStore(),
		counts:      map[string]int{},
	}
	store := storage.NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	handler := server.Handler()
	prefix := "test/tenants/tenant-a/"
	objects.reset()

	usage := serveJSON(handler, http.MethodGet, "/v1/tenant-usage", "tenant-a", nil)
	if usage.Code != http.StatusOK {
		t.Fatalf("tenant usage status=%d body=%s", usage.Code, usage.Body.String())
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	objectThreshold := 1
	decision := server.autoCompactDecision(ctx, "tenant-a", manifest, storage.TenantMaintenanceConfig{
		CompactObjectCountThreshold: &objectThreshold,
	}, &MaintenanceReport{})
	if !decision.Compact || decision.Reason != "object_count" {
		t.Fatalf("decision=%#v, want object_count compaction", decision)
	}
	if calls := objects.count(prefix); calls != 1 {
		t.Fatalf("tenant object list calls=%d, want shared cached call", calls)
	}
}

func TestMaintenanceHistoricalObjectsDoNotRetriggerCompactForOneNewCommit(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	for i := 0; i < 32; i++ {
		key := fmt.Sprintf("test/tenants/tenant-a/history/object-%03d.parquet", i)
		if err := store.Objects.Put(ctx, key, []byte("historical")); err != nil {
			t.Fatalf("put historical object: %v", err)
		}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("post-compact commit: %v", err)
	}
	autoCompact := true
	autoRebuild := false
	tailThreshold := 1000
	objectThreshold := 1
	gcDisabled := int64(0)
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{
			AutoCompact:                 &autoCompact,
			CompactCommitTailThreshold:  &tailThreshold,
			CompactObjectCountThreshold: &objectThreshold,
			GCIntervalSeconds:           &gcDisabled,
		},
		Indexes: storage.TenantIndexConfig{AutoRebuild: &autoRebuild, RebuildOnStale: &autoRebuild},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}

	report := (&Server{Store: store, Mode: "all"}).runMaintenanceOnce(ctx, time.Now().UTC())
	if report.Compacted != 0 || len(report.Errors) != 0 {
		t.Fatalf("maintenance report = %#v, want no historical-object recompact", report)
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if storage.ManifestCommitTailLength(manifest) != 1 {
		t.Fatalf("commit tail = %d, want 1", storage.ManifestCommitTailLength(manifest))
	}
}

func TestMaintenanceRunsGCOnConfiguredInterval(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("first compact: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	gcEvery := int64(60)
	keepSnapshots := 1
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{GCIntervalSeconds: &gcEvery, KeepSnapshots: &keepSnapshots},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	first := server.runMaintenanceOnce(ctx, time.Unix(1000, 0).UTC())
	if first.GCRuns != 1 || len(first.Errors) != 0 {
		t.Fatalf("first maintenance report = %#v", first)
	}
	snapshots, err := store.Objects.List(ctx, "test/tenants/tenant-a/snapshots/")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	fullSnapshots := 0
	for _, object := range snapshots {
		if strings.Contains(object.Key, "/snapshots/sharded/") {
			continue
		}
		fullSnapshots++
	}
	if fullSnapshots != 1 {
		t.Fatalf("full snapshot count = %d, want 1; all objects=%#v", fullSnapshots, snapshots)
	}
	second := server.runMaintenanceOnce(ctx, time.Unix(1010, 0).UTC())
	if second.GCRuns != 0 {
		t.Fatalf("second maintenance report = %#v, want no gc before interval", second)
	}
}

func TestMaintenanceReportsStorageLayoutFindings(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name:     "runs_on",
			FromKind: "service",
			ToKind:   "host",
			Directed: true,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
			{ID: "service:a", Kind: "service"},
		},
		UpsertEdges: []graph.Edge{{
			ID:   "edge-a",
			Type: "runs_on",
			From: "service:a",
			To:   "host:a",
		}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	threshold := 1
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{
			EntityPageSplitThreshold: &threshold,
			EdgeShardSplitThreshold:  &threshold,
		},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if len(report.Errors) != 0 || len(report.Tenants) != 1 {
		t.Fatalf("maintenance report = %#v", report)
	}
	if !hasStorageFinding(report.Tenants[0].StorageFindings, "entity_page_split_needed") {
		t.Fatalf("storage findings = %#v, want entity page split", report.Tenants[0].StorageFindings)
	}
	if !hasStorageFinding(report.Tenants[0].StorageFindings, "edge_shard_split_needed") {
		t.Fatalf("storage findings = %#v, want edge shard split", report.Tenants[0].StorageFindings)
	}
}

func TestMaintenanceStartsIndexRebuildWhenConfigured(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	autoRebuild := true
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Indexes: storage.TenantIndexConfig{AutoRebuild: &autoRebuild},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if report.IndexRebuilds != 1 || len(report.Errors) != 0 {
		t.Fatalf("maintenance report = %#v", report)
	}
	if len(report.Tenants) != 1 || report.Tenants[0].IndexTaskID == "" {
		t.Fatalf("tenant report = %#v", report.Tenants)
	}
	waitForIndexTask(t, ctx, store, "tenant-a", report.Tenants[0].IndexTaskID)
}

func TestMaintenanceUsesDefaultsWhenTenantConfigMissing(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	server := &Server{Store: store, Mode: "all"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if report.IndexRebuilds != 1 || len(report.Errors) != 0 {
		t.Fatalf("maintenance report = %#v", report)
	}
	if len(report.Tenants) != 1 || report.Tenants[0].Skipped != "" || report.Tenants[0].IndexTaskID == "" {
		t.Fatalf("tenant report = %#v", report.Tenants)
	}
	waitForIndexTask(t, ctx, store, "tenant-a", report.Tenants[0].IndexTaskID)
}

func TestMaintenanceNoopsInReaderMode(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	autoCompact := true
	threshold := 1
	if _, err := store.PutTenantConfig(ctx, "tenant-a", storage.TenantConfig{
		Maintenance: storage.TenantMaintenanceConfig{AutoCompact: &autoCompact, CompactCommitTailThreshold: &threshold},
	}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	server := &Server{Store: store, Mode: "reader"}
	report := server.runMaintenanceOnce(ctx, time.Now().UTC())
	if report.Compacted != 0 || report.TenantsChecked != 0 {
		t.Fatalf("reader maintenance report = %#v", report)
	}
}

func waitForIndexTask(t *testing.T, ctx context.Context, store *storage.TenantStore, tenantID string, taskID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetIndexTask(ctx, tenantID, taskID)
		if err != nil {
			t.Fatalf("get index task: %v", err)
		}
		if task.Status != "running" {
			if task.Status != "succeeded" {
				t.Fatalf("index task = %#v, want succeeded", task)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("index task %s did not complete", taskID)
}

func hasStorageFinding(findings []StorageLayoutFinding, code string) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

type maintenanceCountingListStore struct {
	storage.ObjectStore
	mu     sync.Mutex
	counts map[string]int
}

func (s *maintenanceCountingListStore) List(
	ctx context.Context,
	prefix string,
) ([]storage.ObjectInfo, error) {
	s.mu.Lock()
	s.counts[prefix]++
	s.mu.Unlock()
	return s.ObjectStore.List(ctx, prefix)
}

func (s *maintenanceCountingListStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts = map[string]int{}
}

func (s *maintenanceCountingListStore) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[prefix]
}
