package ha

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound6PrepareReplicatedTaskReleasesAppReadLockDuringBackupIO(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := storage.NewTenantStore(fs, "r6-ha-lock")
	defer store.ShutdownTasks(ctx)

	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	var putOnce sync.Once
	var mu sync.Mutex
	objects := map[string][]byte{}
	fakeS3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		switch r.Method {
		case http.MethodPut:
			putOnce.Do(func() {
				close(publishEntered)
				<-releasePublish
			})
			var buf []byte
			if r.Body != nil {
				b := make([]byte, 0)
				tmp := make([]byte, 4096)
				for {
					n, readErr := r.Body.Read(tmp)
					if n > 0 {
						b = append(b, tmp[:n]...)
					}
					if readErr != nil {
						break
					}
				}
				buf = b
			}
			mu.Lock()
			objects[key] = buf
			mu.Unlock()
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			data, ok := objects[key]
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		case http.MethodHead:
			mu.Lock()
			_, ok := objects[key]
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer fakeS3.Close()

	store.Backups, err = backupstore.New(ctx, backupstore.Config{
		Bucket:          "test-backups",
		Prefix:          "backups",
		Endpoint:        fakeS3.URL,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		PathStyle:       true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tenant := "t-lock"
	if _, err := store.CreateTenant(ctx, tenant, storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "h1", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}

	task := storage.Task{
		ID:        "b-lock-1",
		TenantID:  tenant,
		Type:      storage.TaskTypeTenantBackup,
		Status:    storage.TaskStatusQueued,
		Params:    map[string]any{"destination": "object"},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	app := &Application{Files: fs, Store: store}
	// Capture backup via replicated helper.
	if _, err := store.StartTask(ctx, tenant, storage.TaskTypeTenantBackup, map[string]any{"destination": "object"}); err != nil && !strings.Contains(err.Error(), "replication") {
		// ignore; create backup task directly below
	}
	_ = task

	// Use CaptureReplicatedTaskPreparation under app.mu.RLock(), release RLock, then Build concurrently with app.mu.Lock()!
	// First capture local backup file directly via ReplicatedContext.
	repCtx := storage.ReplicatedContext(ctx, "cap-1", time.Now().UTC())
	// Create task in store using a task key? Wait - CreateTask / CaptureReplicatedObjectBackup works on saved task!
	// Let's seed via StartTask with ReplicatedContext or store.QueueReplicatedTask? Let's just start a single-node testCluster or save via Put.
	// Actually, we can use store.Objects.Put on the task key via StartTask? No, StartTask works on non-ReplicationMode stores!
	// Even simpler: save a task via StartTask? No, StartTask runs asynchronously.
	// Let's check: store.ReplicationMode = true -> StartTask queues without running!
	store.ReplicationMode = true
	queued, err := store.StartTask(repCtx, tenant, storage.TaskTypeTenantBackup, map[string]any{"destination": "object"})
	if err != nil {
		t.Fatalf("StartTask in ReplicationMode failed: %v", err)
	}
	if err := store.CaptureReplicatedObjectBackup(repCtx, tenant, queued.ID); err != nil {
		t.Fatalf("CaptureReplicatedObjectBackup failed: %v", err)
	}

	app.mu.RLock()
	prep, err := app.Store.CaptureReplicatedTaskPreparation(ctx, queued)
	app.mu.RUnlock()
	if err != nil {
		t.Fatalf("CaptureReplicatedTaskPreparation failed: %v", err)
	}
	defer prep.Close()

	buildDone := make(chan error, 1)
	go func() {
		_, buildErr := prep.Build(ctx)
		buildDone <- buildErr
	}()

	<-publishEntered
	// While S3 Publish is blocked, acquiring app.mu.Lock() MUST succeed immediately (not deadlocked/blocked by RLock!).
	lockAcquired := make(chan struct{})
	go func() {
		app.mu.Lock()
		app.mu.Unlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		close(releasePublish)
		t.Fatalf("app.mu.Lock() blocked during Build network I/O")
	}
	close(releasePublish)
	if err := <-buildDone; err != nil {
		t.Fatalf("prep.Build failed: %v", err)
	}
}

func TestRound6RetiredReceiptsUntrackedAndUnshardedSkipsTracking(t *testing.T) {
	// 1. Unsharded Application must not allocate/grow tenantReceipts.
	unsharded := &Application{}
	unsharded.initEmptyTenantReceipts()
	unsharded.trackTenantReceipt("t1", "k1")
	unsharded.tenantReceiptsMu.Lock()
	if unsharded.tenantReceipts != nil || unsharded.tenantReceiptOwners != nil {
		unsharded.tenantReceiptsMu.Unlock()
		t.Fatalf("expected unsharded application not to track tenant receipts")
	}
	unsharded.tenantReceiptsMu.Unlock()

	// 2. Sharded Application tracks and untracks cleanly on flush retirement.
	sharded := &Application{ShardID: "shard-1"}
	sharded.initEmptyTenantReceipts()
	sharded.trackTenantReceipt("t1", "k1")
	sharded.untrackTenantReceipt("k1")
	sharded.tenantReceiptsMu.Lock()
	defer sharded.tenantReceiptsMu.Unlock()
	if len(sharded.tenantReceiptOwners) != 0 || len(sharded.tenantReceipts) != 0 {
		t.Fatalf("expected retired receipt to be untracked, got owners=%v receipts=%v", sharded.tenantReceiptOwners, sharded.tenantReceipts)
	}
}

func TestRound6ApplyBatchSingleUnbatchableRejectionReportsAttemptedOne(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := storage.NewTenantStore(fs, "r6-batch")
	defer store.ShutdownTasks(ctx)
	store.ReplicationMode = true

	app := &Application{Files: fs, Store: store}
	// Two unbatchable commands (non-http kind = "task", so batchableCommand == false).
	// applyBatch should only take commands[:1] and return attempted == 1.
	cmd1, _ := json.Marshal(command{ID: "c1", Kind: "task", At: time.Now().UTC(), Tenant: "missing-tenant", IDs: []string{"t1"}})
	cmd2, _ := json.Marshal(command{ID: "c2", Kind: "task", At: time.Now().UTC(), Tenant: "missing-tenant", IDs: []string{"t2"}})
	_, attempted, _ := app.applyBatch(ctx, []replication.ApplyEntry{
		{Index: 1, Data: cmd1},
		{Index: 2, Data: cmd2},
	})
	if attempted != 1 {
		t.Fatalf("expected attempted=1 for unbatchable first command, got %d", attempted)
	}
}
