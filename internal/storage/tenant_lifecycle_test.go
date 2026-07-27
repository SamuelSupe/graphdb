package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestTenantLifecycleDisableDeletePurge(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	info, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"})
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if info.Status != TenantStatusActive || info.Name != "Tenant A" {
		t.Fatalf("created tenant = %#v", info)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit active: %v", err)
	}
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); !errors.Is(err, ErrTenantDisabled) {
		t.Fatalf("commit disabled err = %v, want ErrTenantDisabled", err)
	}
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host"}},
	}, CommitOptions{}); !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("commit deleted err = %v, want ErrTenantDeleted", err)
	}
	report, err := store.PurgeTenant(ctx, "tenant-a", false)
	if err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	if report.Deleted == 0 {
		t.Fatalf("purge report = %#v, want deleted objects", report)
	}
	if _, err := store.GetTenantInfo(ctx, "tenant-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tenant after purge err = %v, want ErrNotFound", err)
	}
}

func TestPurgeTenantUsesBoundedPagesAndCapsDeletedKeySamples(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	prefix := store.tenantObjectPrefix("tenant-a")
	for index := 0; index < objectPrefixScanPageSize+10; index++ {
		key := fmt.Sprintf("%sbulk/object-%04d.parquet", prefix, index)
		if err := base.Put(ctx, key, []byte("data")); err != nil {
			t.Fatalf("put object %d: %v", index, err)
		}
	}

	report, err := store.PurgeTenant(ctx, "tenant-a", true)
	if err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	if objects.listCalls != 0 || objects.pageCalls < 2 {
		t.Fatalf(
			"list calls=%d page calls=%d",
			objects.listCalls, objects.pageCalls,
		)
	}
	if report.Deleted <= tenantPurgeDeletedKeySampleLimit ||
		len(report.DeletedKeys) != tenantPurgeDeletedKeySampleLimit ||
		!report.DeletedKeysTruncated {
		t.Fatalf("purge report=%#v", report)
	}
}

func TestTenantLifecycleObjectsAreParquet(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	for name, key := range map[string]string{
		"metadata": store.tenantMetadataKey("tenant-a"),
		"registry": store.tenantRegistryKey(),
		"lease":    store.writerLeaseKey("tenant-a"),
	} {
		data, err := store.Objects.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s object %q: %v", name, key, err)
		}
		if !isParquetBytes(data) {
			t.Fatalf("%s object %q is not parquet", name, key)
		}
	}
}

func TestCreateTenantAppliesConfigAndSourcePolicyTemplates(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	quota := 10
	config := TenantConfig{Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota}}
	policy := graph.SourcePolicy{
		DefaultPriority: 1,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
		},
	}
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{
		Name:         "Tenant A",
		Config:       &config,
		SourcePolicy: &policy,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	gotConfig, configured, err := store.GetTenantConfig(ctx, "tenant-a")
	if err != nil || !configured {
		t.Fatalf("tenant config configured=%v err=%v", configured, err)
	}
	if gotConfig.Quota.MaxEntitiesPerTenant == nil || *gotConfig.Quota.MaxEntitiesPerTenant != quota {
		t.Fatalf("tenant config = %#v", gotConfig)
	}
	gotPolicy, configured, err := store.GetSourcePolicy(ctx, "tenant-a")
	if err != nil || !configured {
		t.Fatalf("source policy configured=%v err=%v", configured, err)
	}
	if gotPolicy.PriorityFor("manual", 0) != 1000 || gotPolicy.PriorityFor("agent", 0) != 100 {
		t.Fatalf("source policy = %#v", gotPolicy)
	}
}

func TestTenantCloneCopiesCurrentSnapshotAndConfig(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{
		Name: "Tenant A", Labels: map[string]string{"env": "prod"}, Metadata: map[string]any{"owner": "platform"},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.PutTenantConfig(ctx, "tenant-a", TenantConfig{}); err != nil {
		t.Fatalf("put tenant config: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit source: %v", err)
	}
	clone, err := store.CloneTenant(ctx, "tenant-a", TenantCloneOptions{TargetTenantID: "tenant-b"})
	if err != nil {
		t.Fatalf("clone tenant: %v", err)
	}
	if clone.TenantID != "tenant-b" || clone.ClonedFrom != "tenant-a" || clone.ManifestVersion != 1 {
		t.Fatalf("clone info = %#v", clone)
	}
	g, manifest, err := store.Load(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("load clone: %v", err)
	}
	if manifest.TenantID != "tenant-b" {
		t.Fatalf("clone manifest tenant = %q", manifest.TenantID)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatalf("cloned graph missing entity")
	}
	if _, ok, err := store.GetTenantConfig(ctx, "tenant-b"); err != nil || !ok {
		t.Fatalf("clone tenant config ok=%v err=%v", ok, err)
	}
}

func TestTenantBackupAndRestoreTasks(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	quota := 100
	policy := graph.SourcePolicy{Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}}}
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{
		Config:       &TenantConfig{Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota}},
		SourcePolicy: &policy,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit source: %v", err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	if backup.Status != TaskStatusSucceeded || backup.ResultKey == "" || backup.Result["backup_key"] != backup.ResultKey {
		t.Fatalf("backup task = %#v", backup)
	}
	assertTaskActionCompleted(t, backup, "load_snapshot_metadata")
	assertTaskActionCompleted(t, backup, "write_backup_record")
	assertTaskActionCompleted(t, backup, "write_backup_manifest")
	assertTaskActionCompleted(t, backup, "validate_backup_manifest")
	backupManifestKey, ok := backup.Result["backup_manifest_key"].(string)
	if !ok || backupManifestKey == "" {
		t.Fatalf("backup manifest key missing from result: %#v", backup.Result)
	}
	backupManifest, err := store.loadBackupManifest(ctx, backupManifestKey)
	if err != nil {
		t.Fatalf("load backup manifest: %v", err)
	}
	if backupManifest.Stats.Entities != 1 || backupManifest.Stats.ObjectCount == 0 {
		t.Fatalf("backup manifest stats = %#v", backupManifest.Stats)
	}
	integrity := store.validateBackupManifest(ctx, backupManifest)
	if integrity.Status != "ok" {
		t.Fatalf("backup integrity = %#v", integrity)
	}
	dryRun, err := store.StartTask(ctx, "tenant-b", TaskTypeTenantRestore, map[string]any{"backup_key": backupManifestKey, "dry_run": true})
	if err != nil {
		t.Fatalf("start restore dry-run: %v", err)
	}
	dryRun = waitForTask(t, ctx, store, "tenant-b", dryRun.ID)
	if dryRun.Status != TaskStatusSucceeded || dryRun.Result["dry_run"] != true {
		t.Fatalf("restore dry-run task = %#v", dryRun)
	}
	assertTaskActionCompleted(t, dryRun, "load_backup")
	assertTaskActionCompleted(t, dryRun, "dry_run")
	if exists, err := store.tenantRestoreDataExists(ctx, "tenant-b"); err != nil || exists {
		t.Fatalf("dry-run wrote target exists=%v err=%v", exists, err)
	}
	restore, err := store.StartTask(ctx, "tenant-b", TaskTypeTenantRestore, map[string]any{"backup_key": backupManifestKey})
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(t, ctx, store, "tenant-b", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore task = %#v", restore)
	}
	if restore.Result["backup_manifest_key"] != backupManifestKey {
		t.Fatalf("restore result manifest key = %#v", restore.Result)
	}
	assertTaskActionCompleted(t, restore, "load_backup")
	assertTaskActionCompleted(t, restore, "write_snapshot")
	assertTaskActionCompleted(t, restore, "write_metadata")
	assertTaskActionCompleted(t, restore, "rebuild_indexes")
	assertTaskActionCompleted(t, restore, "verify_restore")
	g, manifest, err := store.Load(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("load restored tenant: %v", err)
	}
	if manifest.TenantID != "tenant-b" || manifest.Version != 1 {
		t.Fatalf("restored manifest = %#v", manifest)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("restored graph missing entity")
	}
	if _, configured, err := store.GetTenantConfig(ctx, "tenant-b"); err != nil || !configured {
		t.Fatalf("restored config configured=%v err=%v", configured, err)
	}
	if _, configured, err := store.GetSourcePolicy(ctx, "tenant-b"); err != nil || !configured {
		t.Fatalf("restored source policy configured=%v err=%v", configured, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-b")
	if err != nil || health.Status != "ready" {
		t.Fatalf("restored index health=%#v err=%v", health, err)
	}
	overwrite, err := store.StartTask(ctx, "tenant-b", TaskTypeTenantRestore, map[string]any{"backup_key": backupManifestKey, "overwrite": true})
	if err != nil {
		t.Fatalf("start overwrite restore: %v", err)
	}
	overwrite = waitForTaskAcrossPurge(t, ctx, store, "tenant-b", overwrite.ID)
	if overwrite.Status != TaskStatusSucceeded || overwrite.Result["overwrote"] != true {
		t.Fatalf("overwrite restore task = %#v", overwrite)
	}
}

func waitForTaskAcrossPurge(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, taskID string) Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(ctx, tenantID, taskID)
		if errors.Is(err, ErrNotFound) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
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

func TestTenantBackupRetryContinuesAfterBackupRecord(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	if backup.Status != TaskStatusSucceeded || backup.ResultKey == "" {
		t.Fatalf("backup = %#v", backup)
	}
	now := time.Now().UTC()
	failed := Task{
		ID:        "backup-after-record",
		TenantID:  "tenant-a",
		Type:      TaskTypeTenantBackup,
		Status:    TaskStatusFailed,
		Phase:     TaskStatusFailed,
		StartedAt: now,
		UpdatedAt: now,
		Checkpoint: map[string]any{
			"backup_key": backup.ResultKey,
			"actions": []map[string]any{{
				"id":     "write_backup_record",
				"status": "completed",
				"output": map[string]any{"backup_key": backup.ResultKey},
			}},
		},
	}
	if err := store.saveTask(ctx, failed); err != nil {
		t.Fatalf("save failed task: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-a", failed.ID)
	if err != nil {
		t.Fatalf("retry backup: %v", err)
	}
	retry = waitForTask(t, ctx, store, "tenant-a", retry.ID)
	if retry.Status != TaskStatusSucceeded || retry.ResultKey != backup.ResultKey {
		t.Fatalf("retry backup = %#v", retry)
	}
	assertTaskActionCompleted(t, retry, "write_backup_record")
	assertTaskActionCompleted(t, retry, "write_backup_manifest")
	assertTaskActionCompleted(t, retry, "validate_backup_manifest")
}

func TestTenantRestoreRetrySkipsCheckpointedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	backupManifestKey, _ := backup.Result["backup_manifest_key"].(string)
	restore, err := store.StartTask(ctx, "tenant-c", TaskTypeTenantRestore, map[string]any{"backup_key": backupManifestKey})
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(t, ctx, store, "tenant-c", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore = %#v", restore)
	}
	now := time.Now().UTC()
	failed := Task{
		ID:        "restore-after-snapshot",
		TenantID:  "tenant-c",
		Type:      TaskTypeTenantRestore,
		Status:    TaskStatusFailed,
		Phase:     TaskStatusFailed,
		Params:    map[string]any{"backup_key": backupManifestKey},
		StartedAt: now,
		UpdatedAt: now,
		Checkpoint: map[string]any{
			"backup_key":       backupManifestKey,
			"snapshot_written": true,
			"actions": []map[string]any{{
				"id":     "write_snapshot",
				"status": "completed",
				"output": map[string]any{"version": float64(1)},
			}},
		},
	}
	if err := store.saveTask(ctx, failed); err != nil {
		t.Fatalf("save failed restore: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-c", failed.ID)
	if err != nil {
		t.Fatalf("retry restore: %v", err)
	}
	retry = waitForTask(t, ctx, store, "tenant-c", retry.ID)
	if retry.Status != TaskStatusSucceeded {
		t.Fatalf("retry restore = %#v", retry)
	}
	assertTaskActionCompleted(t, retry, "write_snapshot")
	assertTaskActionCompleted(t, retry, "write_metadata")
	assertTaskActionCompleted(t, retry, "rebuild_indexes")
	assertTaskActionCompleted(t, retry, "verify_restore")
}

func TestTenantRestoreRetryRecoversPublishedSnapshotBeforeCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatalf("start backup: %v", err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	backupManifestKey, _ := backup.Result["backup_manifest_key"].(string)
	restore, err := store.StartTask(ctx, "tenant-c", TaskTypeTenantRestore, map[string]any{"backup_key": backupManifestKey})
	if err != nil {
		t.Fatalf("start restore: %v", err)
	}
	restore = waitForTask(t, ctx, store, "tenant-c", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore = %#v", restore)
	}

	now := time.Now().UTC()
	failed := Task{
		ID:        "restore-after-publish",
		TenantID:  "tenant-c",
		Type:      TaskTypeTenantRestore,
		Status:    TaskStatusFailed,
		Phase:     TaskStatusFailed,
		Params:    map[string]any{"backup_key": backupManifestKey},
		StartedAt: now,
		UpdatedAt: now,
		Checkpoint: map[string]any{
			"phase":            "restore_write_snapshot",
			"backup_key":       backupManifestKey,
			"source_tenant_id": "tenant-a",
			"version":          int64(1),
			"actions": []map[string]any{{
				"id":     "write_snapshot",
				"status": "running",
			}},
		},
	}
	if err := store.saveTask(ctx, failed); err != nil {
		t.Fatalf("save failed restore: %v", err)
	}
	retry, err := store.RetryTask(ctx, "tenant-c", failed.ID)
	if err != nil {
		t.Fatalf("retry restore: %v", err)
	}
	retry = waitForTask(t, ctx, store, "tenant-c", retry.ID)
	if retry.Status != TaskStatusSucceeded {
		t.Fatalf("retry restore = %#v", retry)
	}
	assertTaskActionCompleted(t, retry, "write_snapshot")
	assertTaskActionCompleted(t, retry, "verify_restore")
}

func TestTenantRestoreDrillRestoresToTargetPrefixAndRunsQueries(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"env": "prod"}}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit source: %v", err)
	}
	if _, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:    "prod-hosts",
		Request: query.Request{Op: "match", Kind: "host", Filters: graph.Fields{"env": "prod"}, Limit: 10},
	}); err != nil {
		t.Fatalf("save query: %v", err)
	}
	task, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantRestoreDrill, map[string]any{
		"target_tenant_id": "tenant-a-drill",
		"target_prefix":    "drill",
		"cleanup":          false,
	})
	if err != nil {
		t.Fatalf("start restore drill: %v", err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded || task.Result["status"] != "passed" || task.Result["recoverable"] != true {
		t.Fatalf("restore drill task = %#v", task)
	}
	if task.Result["backup_manifest_key"] == "" {
		t.Fatalf("backup manifest key missing: %#v", task.Result)
	}
	target := NewTenantStore(objects, "drill")
	g, manifest, err := target.Load(ctx, "tenant-a-drill")
	if err != nil {
		t.Fatalf("load drill target: %v", err)
	}
	if manifest.TenantID != "tenant-a-drill" || manifest.Version != 1 {
		t.Fatalf("drill manifest = %#v", manifest)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatal("drill target missing restored entity")
	}
	results, ok := task.Result["query_results"].([]any)
	if !ok || len(results) == 0 {
		t.Fatalf("query results missing: %#v", task.Result["query_results"])
	}
}
