package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gitlab.jiagouyun.com/guance/graphdb/internal/backupstore"
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestLocalTenantInitializationFailureAndRetry(t *testing.T) {
	for _, clone := range []bool{false, true} {
		for _, phase := range []string{"config", "metadata", "registry"} {
			name := "create/" + phase
			if clone {
				name = "clone/" + phase
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				files, err := OpenFileStore(root)
				if err != nil {
					t.Fatal(err)
				}
				defer files.Close()
				store := NewTenantStore(files, "test")
				one := 1
				opts := TenantCreateOptions{Config: &TenantConfig{Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &one}}, SourcePolicy: &graph.SourcePolicy{DefaultPriority: 17}}
				sourceInfo, err := store.CreateTenant(ctx, "source", opts)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Commit(ctx, "source", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{}); err != nil {
					t.Fatal(err)
				}
				_, record, md5, err := store.captureTenantBackup(ctx, "source")
				if err != nil {
					t.Fatal(err)
				}
				cloneOpts := TenantCloneOptions{TargetTenantID: "target"}
				if phase == "registry" {
					store.Objects = &localLifecycleFailureStore{&failPutStore{ObjectStore: files, contains: store.tenantRegistryKey()}}
					if clone {
						_, err = store.CloneTenant(ctx, "source", cloneOpts)
					} else {
						_, err = store.CreateTenant(ctx, "target", opts)
					}
				} else {
					_, err = store.publishLocalTenantLifecycle(ctx, "target", clone, func(stage *TenantStore) (TenantInfo, error) {
						key := stage.tenantConfigKey("target")
						if phase == "metadata" {
							key = stage.tenantMetadataKey("target")
						}
						stage.Objects = &failPutStore{ObjectStore: stage.Objects, contains: key}
						if clone {
							return stage.cloneTenantRecord(ctx, "source", sourceInfo, record, md5, cloneOpts)
						}
						return stage.CreateTenant(ctx, "target", opts)
					})
				}
				if err == nil {
					t.Fatal("expected initialization failure")
				}
				if exists, err := store.tenantDataExists(ctx, "target"); err != nil || exists {
					t.Fatalf("partial tenant exposed: exists=%v err=%v", exists, err)
				}
				if err := files.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenFileStore(root)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				store = NewTenantStore(reopened, "test")
				store.Backpressure = NewWritePressure(BackpressureConfig{})
				if clone {
					_, err = store.CloneTenant(ctx, "source", cloneOpts)
				} else {
					_, err = store.CreateTenant(ctx, "target", opts)
				}
				if err != nil {
					t.Fatalf("retry initialization: %v", err)
				}
				config, configured, err := store.GetTenantConfig(ctx, "target")
				if err != nil || !configured || config.Quota.MaxEntitiesPerTenant == nil || *config.Quota.MaxEntitiesPerTenant != 1 {
					t.Fatalf("config=%+v configured=%v err=%v", config, configured, err)
				}
				policy, configured, err := store.GetSourcePolicy(ctx, "target")
				if err != nil || !configured || policy.DefaultPriority != 17 {
					t.Fatalf("policy=%+v configured=%v err=%v", policy, configured, err)
				}
				if _, err := store.Commit(ctx, "target", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}, {ID: "host:c", Kind: "host"}}}, CommitOptions{}); !errors.Is(err, ErrBackpressure) {
					t.Fatalf("quota bypass: %v", err)
				}
				g, _, err := store.Load(ctx, "target")
				want := 0
				if clone {
					want = 1
				}
				if err != nil || len(g.Entities) != want {
					t.Fatalf("initialized graph count=%d err=%v", len(g.Entities), err)
				}
			})
		}
	}
}

type localLifecycleFailureStore struct{ *failPutStore }

func (s *localLifecycleFailureStore) UnwrapObjectStore() ObjectStore { return s.ObjectStore }

func TestLocalCreateRetainsExistingDataAndLateControlWrites(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	if _, err := store.Commit(ctx, "tenant", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{IdempotencyKey: "existing"}); err != nil {
		t.Fatal(err)
	}
	_, err = store.publishLocalTenantLifecycle(ctx, "tenant", false, func(stage *TenantStore) (TenantInfo, error) {
		info, err := stage.CreateTenant(ctx, "tenant", TenantCreateOptions{Name: "managed"})
		if err != nil {
			return TenantInfo{}, err
		}
		// Task records can finish without the tenant lock while staging runs.
		task := Task{ID: "late", TenantID: "tenant", Type: TaskTypeExportSnapshot, Status: TaskStatusSucceeded}
		return info, store.saveTask(ctx, task)
	})
	if err != nil {
		t.Fatal(err)
	}
	if task, err := store.GetTask(ctx, "tenant", "late"); err != nil || task.Status != TaskStatusSucceeded {
		t.Fatalf("late task=%+v err=%v", task, err)
	}
	result, err := store.CommitWithReport(ctx, "tenant", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, CommitOptions{IdempotencyKey: "existing"})
	if err != nil || !result.IdempotentReplay || result.Version != 1 {
		t.Fatalf("preserved idempotency=%+v err=%v", result, err)
	}
	g, manifest, err := store.Load(ctx, "tenant")
	if err != nil || manifest.Version != 1 || len(g.Entities) != 1 {
		t.Fatalf("preserved graph=%+v err=%v", manifest, err)
	}
}

func TestObjectBackupRetryAfterReopenAndVerifiedRestore(t *testing.T) {
	endpoint := os.Getenv("GRAPHDB_TEST_BACKUP_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set GRAPHDB_TEST_BACKUP_S3_ENDPOINT for S3 integration")
	}
	ctx := context.Background()
	dir := t.TempDir()
	files, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { files.Close() }()
	store := NewTenantStore(files, "test")
	quota := 100
	policy := graph.SourcePolicy{Sources: []graph.SourcePolicyItem{{Name: "manual", Priority: 1000}}}
	if _, err := store.CreateTenant(ctx, "source", TenantCreateOptions{Name: "Backup Source", Config: &TenantConfig{Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota}}, SourcePolicy: &policy}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "source", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "captured"}}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	_, capture, _, err := store.captureTenantBackup(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce death after the durable capture rename but before the
	// snapshot_captured checkpoint; retry must not capture a newer version.
	backupID := "captured-before-crash"
	if err := store.putTaskResult(ctx, "source", backupID, taskResult(capture)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	failed := Task{ID: backupID, TenantID: "source", Type: TaskTypeTenantBackup, Status: TaskStatusFailed, StartedAt: now, UpdatedAt: now, Params: map[string]any{"destination": "object"}, Checkpoint: map[string]any{"remote_backup_id": backupID}}
	if err := store.saveTask(ctx, failed); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store = NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	backupCfg := backupstore.Config{
		Endpoint: endpoint, Bucket: os.Getenv("GRAPHDB_TEST_BACKUP_S3_BUCKET"), Prefix: fmt.Sprintf("restore-%d", time.Now().UnixNano()), PathStyle: true,
		AccessKeyID: os.Getenv("GRAPHDB_TEST_BACKUP_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("GRAPHDB_TEST_BACKUP_S3_SECRET_ACCESS_KEY"),
	}
	store.Backups, err = backupstore.New(ctx, backupCfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "source", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:later", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	retry, err := store.RetryTask(ctx, "source", backupID)
	if err != nil {
		t.Fatal(err)
	}
	retry = waitForTask(t, ctx, store, "source", retry.ID)
	if retry.Status != TaskStatusSucceeded {
		t.Fatalf("backup: %+v", retry)
	}
	uri := stringTaskParam(retry.Result, "backup_key")
	manifest, err := store.Backups.ReadManifest(ctx, uri)
	if err != nil || manifest.Version != capture.Version {
		t.Fatalf("snapshot changed on retry: %+v %v", manifest, err)
	}
	if _, err := store.PurgeTenant(ctx, "source", true); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListObjectBackups(ctx, "source", "", 20)
	if err != nil || len(page.Backups) != 1 {
		t.Fatalf("backup lost after source purge: %+v %v", page, err)
	}
	for _, dryRun := range []bool{true, false} {
		restore, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": uri, "dry_run": dryRun})
		if err != nil {
			t.Fatal(err)
		}
		restore = waitForTask(t, ctx, store, "target", restore.ID)
		if restore.Status != TaskStatusSucceeded {
			t.Fatalf("restore dry_run=%v: %+v", dryRun, restore)
		}
		if dryRun {
			if exists, err := store.tenantRestoreDataExists(ctx, "target"); err != nil || exists {
				t.Fatalf("dry run mutated target: %v %v", exists, err)
			}
		}
	}
	g, _, err := store.Load(ctx, "target")
	if err != nil || !reflect.DeepEqual(g.Snapshot(), capture.Snapshot) {
		t.Fatalf("restored graph differs: %v", err)
	}
	config, configured, err := store.GetTenantConfig(ctx, "target")
	if err != nil || !configured || !reflect.DeepEqual(config, *capture.Config) {
		t.Fatalf("config differs: %+v %v", config, err)
	}
	restoredPolicy, configured, err := store.GetSourcePolicy(ctx, "target")
	if err != nil || !configured || !reflect.DeepEqual(restoredPolicy, *capture.SourcePolicy) {
		t.Fatalf("source policy differs: %+v %v", restoredPolicy, err)
	}
	info, err := store.GetTenantInfo(ctx, "target")
	if err != nil || info.Name != "Backup Source" {
		t.Fatalf("metadata differs: %+v %v", info, err)
	}
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider(backupCfg.AccessKeyID, backupCfg.SecretAccessKey, "")}, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(backupCfg.Bucket), Key: aws.String(manifest.SnapshotKey), Body: bytes.NewReader([]byte("corrupted"))}); err != nil {
		t.Fatal(err)
	}
	restore, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": uri, "overwrite": true})
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		restore, err = store.GetTask(ctx, "target", restore.ID)
		if err != nil {
			t.Fatal(err)
		}
		if taskTerminal(restore.Status) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if restore.Status != TaskStatusFailed {
		t.Fatalf("corrupt backup accepted: %+v", restore)
	}
	after, _, err := store.Load(ctx, "target")
	if err != nil || !reflect.DeepEqual(after.Snapshot(), capture.Snapshot) {
		t.Fatalf("failed restore changed target: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".tmp-backup-") {
			t.Fatalf("leaked staging file: %s", entry.Name())
		}
	}
}

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

func TestLocalBackupSurvivesCommitsAndOverwriteRestore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	files, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { files.Close() }()
	store := NewTenantStore(files, "review")
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:before", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	if backup.Status != TaskStatusSucceeded {
		t.Fatalf("backup failed: %+v", backup)
	}
	key := stringTaskParam(backup.Result, "backup_manifest_key")
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:after", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	task, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantRestore, map[string]any{"backup_key": key, "overwrite": true})
	if err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, ctx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded {
		t.Fatalf("successful backup becomes unrestorable after normal commit: %s", task.Error)
	}

	if _, err := store.loadTenantBackupInput(ctx, key); err != nil {
		t.Fatalf("backup lost after overwrite: %v", err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store = NewTenantStore(files, "review")
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != 1 {
		t.Fatalf("restored manifest=%+v err=%v", manifest, err)
	}
	if _, ok := g.GetEntity("host:after"); ok {
		t.Fatal("restored graph retained later entity")
	}
	if _, ok := g.GetEntity("host:before"); !ok {
		t.Fatal("restored graph lost backup entity")
	}
	if _, err := store.GetTask(ctx, "tenant-a", task.ID); err != nil {
		t.Fatalf("restore task missing: %v", err)
	}
	if input, err := store.loadTenantBackupInput(ctx, key); err != nil || input.Integrity.Status != "ok" {
		t.Fatalf("backup after reopen: %+v %v", input.Integrity, err)
	}
}

func TestLocalRestoreAdmissionDistinguishesDryRun(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "review")
	if _, err = store.Commit(ctx, "source", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	backup, err := store.StartTask(ctx, "source", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForTask(t, ctx, store, "source", backup.ID)
	key := stringTaskParam(backup.Result, "backup_manifest_key")
	release, err := store.PinReadView(ctx, "target")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		release()
		stop, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := store.ShutdownTasks(stop); err != nil {
			t.Error(err)
		}
	}()
	dryRun, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": key, "dry_run": true})
	if err != nil {
		t.Fatal(err)
	}
	restore, err := store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": key, "overwrite": true})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("different restore params: task=%+v err=%v", restore, err)
	}
	release()
	release = func() {}
	dryRun = waitForTask(t, ctx, store, "target", dryRun.ID)
	restore, err = store.StartTask(ctx, "target", TaskTypeTenantRestore, map[string]any{"backup_key": key, "overwrite": true})
	if err != nil {
		t.Fatal(err)
	}
	restore = waitForTask(t, ctx, store, "target", restore.ID)
	if restore.ID == dryRun.ID {
		t.Fatalf("real restore silently returned dry-run task: same_id=%s returned_dry_run=%v requested_overwrite=true", restore.ID, restore.Params["dry_run"])
	}
}
