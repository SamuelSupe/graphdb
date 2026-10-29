package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestTenantMetadataCandidateDecodeBudget(t *testing.T) {
	ConfigureParquetDecodeMaxConcurrent(2)
	defer ConfigureParquetDecodeMaxConcurrent(0)
	for _, state := range []string{"healthy", "stale_backup", "pending"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			objects := NewMemoryStore()
			store := NewTenantStore(objects, "test")
			primary, err := marshalParquetTenantMetadata(ctx, TenantMetadata{TenantID: "tenant-a", Status: TenantStatusActive})
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{store.tenantMetadataKey("tenant-a"), store.tenantMetadataBackupKey("tenant-a")} {
				if err := objects.Put(ctx, key, primary); err != nil {
					t.Fatal(err)
				}
			}
			wantDecodes, wantStatus := 1, TenantStatusActive
			if state != "healthy" {
				key := store.tenantMetadataBackupKey("tenant-a")
				if state == "pending" {
					key = store.tenantMetadataPendingKey("tenant-a")
					wantDecodes, wantStatus = 2, TenantStatusDisabled
				}
				putTenantMetadataFixture(t, ctx, objects, key, TenantMetadata{TenantID: "tenant-a", Status: TenantStatusDisabled})
			}
			stats := &parquetDecodeTraceStats{}
			_, candidate, _, repair, err := store.localTenantMetadataCandidate(withParquetDecodeTraceStats(ctx, stats), "tenant-a")
			if err != nil || candidate.metadata.Status != wantStatus || repair != (state != "healthy") {
				t.Fatalf("candidate=%+v repair=%t err=%v", candidate.metadata, repair, err)
			}
			if count, _ := stats.snapshot(); count != wantDecodes {
				t.Fatalf("metadata decode admissions=%d, want %d; unused backup decoding consumes control capacity", count, wantDecodes)
			}
		})
	}
}

func TestTenantMetadataBackupRepairPreservesPrimary(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt", "stale", "invalid_pending"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			store := NewTenantStore(files, "test")
			defer store.ShutdownTasks(ctx)
			if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "kept"}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err != nil {
				t.Fatal(err)
			}
			key, backupKey := store.tenantMetadataKey("tenant-a"), store.tenantMetadataBackupKey("tenant-a")
			data, err := files.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(filepath.Join(files.root, filepath.FromSlash(key)))
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing":
				err = files.Delete(ctx, backupKey)
			case "corrupt":
				err = files.Put(ctx, backupKey, []byte("damaged"))
			case "stale":
				putTenantMetadataFixture(t, ctx, files, backupKey, TenantMetadata{TenantID: "tenant-a", Status: TenantStatusActive})
			case "invalid_pending":
				err = files.Put(ctx, store.tenantMetadataPendingKey("tenant-a"), []byte("damaged"))
			}
			if err != nil {
				t.Fatal(err)
			}
			// Repairing a secondary copy must not require replacing a healthy
			// primary or allocating a new write-intent inode.
			store.Objects = &localLifecycleFailureStore{&failPutStore{ObjectStore: files, contains: "/metadata.parquet"}}
			if _, err := store.repairLocalTenantMetadata(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(filepath.Join(files.root, filepath.FromSlash(key)))
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("healthy primary replaced: %v", err)
			}
			backup, err := files.Get(ctx, backupKey)
			if err != nil || !bytes.Equal(data, backup) {
				t.Fatalf("backup differs from authoritative primary: %v", err)
			}
			if _, err := files.Get(ctx, store.tenantMetadataPendingKey("tenant-a")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("pending intent remains: %v", err)
			}
		})
	}
}

func BenchmarkLocalTenantMetadataCandidate(b *testing.B) {
	ctx := context.Background()
	files, err := OpenFileStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "benchmark", Labels: map[string]string{"owner": "platform", "region": "cn"}}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, candidate, _, repair, err := store.localTenantMetadataCandidate(ctx, "tenant-a")
		if err != nil || !candidate.valid || repair {
			b.Fatalf("candidate: valid=%t repair=%t err=%v", candidate.valid, repair, err)
		}
	}
}

func TestTenantMetadataOnlineRecoveryPreservesState(t *testing.T) {
	for _, status := range []string{TenantStatusActive, TenantStatusDisabled, TenantStatusDeleted} {
		for _, damage := range []string{"missing", "corrupt"} {
			t.Run(status+"/"+damage, func(t *testing.T) {
				ctx := context.Background()
				files, err := OpenFileStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer files.Close()
				store := NewTenantStore(files, "test")
				defer store.ShutdownTasks(ctx)
				if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "original", Labels: map[string]string{"owner": "a"}}); err != nil {
					t.Fatal(err)
				}
				if _, err := store.SetTenantStatus(ctx, "tenant-a", status); err != nil {
					t.Fatal(err)
				}
				key := store.tenantMetadataKey("tenant-a")
				if damage == "missing" {
					err = files.Delete(ctx, key)
				} else {
					err = files.Put(ctx, key, []byte("corrupt parquet"))
				}
				if err != nil {
					t.Fatal(err)
				}
				info, err := store.GetTenantInfo(ctx, "tenant-a")
				if err != nil || info.Status != status || info.Name != "original" || info.Labels["owner"] != "a" {
					t.Fatalf("lost original metadata: %+v %v", info, err)
				}
				waitForRecoveredTenantMetadata(t, files, store, "tenant-a", status)
			})
		}
	}
}

func TestTenantMetadataWaitHonorsCancellation(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Directory publication holds this guard while retaining the existing tree.
	// Canceled readers must leave the queue without waiting for that IO to finish.
	unlock := files.lockObject("\x00metadata:" + store.tenantMetadataKey("tenant-a"))
	release := sync.OnceFunc(unlock)
	defer release()
	attempt, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := store.GetTenantInfo(attempt, "tenant-a")
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("canceled metadata read: %v", err)
		}
	case <-time.After(time.Second):
		release()
		<-result
		t.Fatal("canceled metadata read waited for directory publication")
	}
	release()
	if _, err := store.GetTenantInfo(ctx, "tenant-a"); err != nil {
		t.Fatalf("live request after canceled waiter: %v", err)
	}
}

func TestTenantMetadataOnlineRecoveryLegacyAndPurge(t *testing.T) {
	for _, phase := range []string{"absent", "cleared", "running", "complete", "unknown"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			store := NewTenantStore(files, "test")
			defer store.ShutdownTasks(ctx)
			if _, err := store.Commit(ctx, "legacy", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{IdempotencyKey: "original"}); err != nil {
				t.Fatal(err)
			}
			removeTenantMetadataFixture(t, store, "legacy")
			if phase != "absent" {
				putTenantMetadataFixture(t, ctx, files, store.tenantPurgeTombstoneKey("legacy"), TenantMetadata{TenantID: "legacy", Status: TenantStatusDeleted, Metadata: map[string]any{tenantPurgePhaseKey: phase}})
			}
			if err := files.Put(ctx, store.tenantMetadataKey("legacy"), []byte("damaged")); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(files.root, filepath.FromSlash(store.tenantObjectPrefix("legacy")))
			before, err := os.Stat(dir)
			if err != nil {
				t.Fatal(err)
			}
			var readers sync.WaitGroup
			for range 8 {
				readers.Go(func() { _, _ = store.GetTenantInfo(ctx, "legacy") })
			}
			readers.Wait()
			if phase != "absent" && phase != "cleared" {
				if _, err := store.repairLocalTenantMetadata(ctx, "legacy"); err != nil {
					t.Fatal(err)
				}
				data, err := files.Get(ctx, store.tenantMetadataKey("legacy"))
				if err != nil || string(data) != "damaged" {
					t.Fatalf("purge was bypassed: %q %v", data, err)
				}
				return
			}
			waitForRecoveredTenantMetadata(t, files, store, "legacy", TenantStatusActive)
			after, err := os.Stat(dir)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("replaced graph directory: %v", err)
			}
			result, err := store.CommitWithReport(ctx, "legacy", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{IdempotencyKey: "original"})
			if err != nil || !result.IdempotentReplay || result.Version != 1 {
				t.Fatalf("changed graph or receipt: %+v %v", result, err)
			}
		})
	}
}

func TestTenantMetadataPendingDisableSurvivesPublicationFailure(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "original"}); err != nil {
		t.Fatal(err)
	}
	store.Objects = &localLifecycleFailureStore{&failPutStore{ObjectStore: files, contains: "/metadata.parquet"}}
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDisabled); err == nil {
		t.Fatal("expected injected publication failure")
	}
	if err := store.ShutdownTasks(ctx); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store = NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	status, err := store.TenantStatus(ctx, "tenant-a")
	if err != nil || status != TenantStatusDisabled {
		t.Fatalf("old active backup revived: %q %v", status, err)
	}
	waitForRecoveredTenantMetadata(t, files, store, "tenant-a", TenantStatusDisabled)
}

func TestTenantMetadataOnlineRecoveryRequiresManifestAndDiskSpace(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.GetTenantInfo(ctx, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("created nonexistent tenant: %v", err)
	}
	if _, err := store.CreateTenant(ctx, "headless", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := files.Delete(ctx, store.manifestKey("headless")); err != nil {
		t.Fatal(err)
	}
	if err := files.Delete(ctx, store.tenantMetadataKey("headless")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetTenantInfo(ctx, "headless"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("backup revived a tenant without its manifest: %v", err)
	}
	if _, err := store.Commit(ctx, "legacy", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	removeTenantMetadataFixture(t, store, "legacy")
	if err := files.ConfigureDiskSpace(DiskSpacePolicy{MinFreeInodes: 128}); err != nil {
		t.Fatal(err)
	}
	files.diskProbe = func(string) (DiskSpaceStatus, error) {
		return DiskSpaceStatus{AvailableBytes: 1 << 30, TotalInodes: 1024, InodesKnown: true}, nil
	}
	_, err = store.repairLocalTenantMetadata(ctx, "legacy")
	assertBackpressureReason(t, err, "disk_inodes_low")
	if _, err := files.Get(ctx, store.tenantMetadataKey("legacy")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("published under inode pressure: %v", err)
	}
	if err := files.Put(ctx, store.manifestKey("legacy"), []byte("damaged")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.repairLocalTenantMetadata(ctx, "legacy"); err == nil {
		t.Fatal("recovered from corrupt manifest")
	}
}

// Old installations could publish a graph without any management records.
func removeTenantMetadataFixture(t *testing.T, store *TenantStore, tenant string) {
	t.Helper()
	for _, key := range []string{store.tenantMetadataKey(tenant), store.tenantMetadataBackupKey(tenant), store.tenantMetadataPendingKey(tenant)} {
		if err := store.Objects.Delete(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	store.deleteCachedTenantMetadata(tenant)
}

func TestTenantMetadataPendingDeleteDoesNotOverrideRecreation(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	store.Objects = &localLifecycleFailureStore{&failPutStore{ObjectStore: files, contains: "/metadata.parquet"}}
	if _, err := store.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err == nil {
		t.Fatal("expected injected failure")
	}
	if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "recreated", Config: &TenantConfig{}}); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetTenantInfo(ctx, "tenant-a")
	if err != nil || info.Status != TenantStatusActive || info.Name != "recreated" {
		t.Fatalf("old pending delete survived directory publication: %+v %v", info, err)
	}
	if _, err := files.Get(ctx, store.tenantMetadataPendingKey("tenant-a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old intent retained: %v", err)
	}
}

func waitForRecoveredTenantMetadata(t *testing.T, files *FileStore, store *TenantStore, tenant, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		primary, err := files.Get(context.Background(), store.tenantMetadataKey(tenant))
		if err == nil {
			metadata, decodeErr := decodeParquetTenantMetadata(context.Background(), primary)
			backup, backupErr := files.Get(context.Background(), store.tenantMetadataBackupKey(tenant))
			_, pendingErr := files.Get(context.Background(), store.tenantMetadataPendingKey(tenant))
			if decodeErr == nil && metadata.Status == status && backupErr == nil && string(primary) == string(backup) && errors.Is(pendingErr, ErrNotFound) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("online recovery did not durably repair metadata copies")
}
