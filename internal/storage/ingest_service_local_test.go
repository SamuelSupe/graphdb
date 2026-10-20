package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type localIngestPublicationFault struct {
	ObjectStore
	key      string
	err      error
	failing  atomic.Bool
	attempts atomic.Int32
}

func TestLocalWALPoisonTenantDoesNotRetainHealthyHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	seed := NewTenantStore(files, "test")
	manifest, err := seed.Commit(ctx, "poison", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}}}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	key := manifest.CommitKeys[0]
	original, err := files.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := files.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	config := testIngestServiceConfig(t)
	config.WAL.MaxBytes, config.WAL.SegmentBytes = 64<<10, 16<<10
	config.FlushMaxRequests, config.RetryInterval = 1, time.Millisecond
	service, err := OpenIngestService(NewTenantStore(files, "test"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if service != nil {
			crashIngestService(t, service)
		}
	}()
	if _, err := service.Accept(ctx, "poison", ingestEntityRequest("pending", "host:pending")); err != nil {
		t.Fatal(err)
	}
	for i := range 80 {
		request := ingestEntityRequest(fmt.Sprintf("healthy-%d", i), fmt.Sprintf("host:healthy-%d", i))
		request.Items[0].Entity.Fields = graph.Fields{"payload": strings.Repeat("h", 2048)}
		accepted, err := service.Accept(ctx, "healthy", request)
		if err != nil {
			t.Fatalf("healthy request %d blocked by poisoned tenant: %v", i, err)
		}
		if _, err := service.Wait(ctx, accepted); err != nil {
			t.Fatalf("healthy request %d: %v", i, err)
		}
	}
	crashIngestService(t, service)
	service = nil
	if err := files.Put(ctx, key, original); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(files, "test")
	reopened, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, reopened)
	if err := reopened.FlushTenant(ctx, "poison"); err != nil {
		t.Fatal(err)
	}
	for tenant, count := range map[string]int{"poison": 2, "healthy": 80} {
		g, _, err := store.Load(ctx, tenant)
		if err != nil || g.Entities.Len() != count {
			t.Fatalf("%s recovered entities=%d want=%d err=%v", tenant, g.Entities.Len(), count, err)
		}
	}
}

func (s *localIngestPublicationFault) UnwrapObjectStore() ObjectStore { return s.ObjectStore }

func (s *localIngestPublicationFault) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if key == s.key && s.failing.Load() {
		s.attempts.Add(1)
		if s.err != nil {
			return ObjectMeta{}, s.err
		}
		return ObjectMeta{}, errors.New("injected local ingest publication failure")
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func TestLocalWALPreparedRetryReusesDurablePlan(t *testing.T) {
	for _, failure := range []string{"manifest", "batch_metadata"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			objects := &localIngestPublicationFault{ObjectStore: files}
			store := NewTenantStore(objects, "test")
			if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			request := ingestEntityRequest("retry", "host:retry")
			request.Items[0].Entity.Fields = map[string]any{"payload": strings.Repeat("x", 4<<10)}
			objects.key = store.manifestKey("tenant-a")
			if failure == "batch_metadata" {
				objects.key = store.ingestBatchKey("tenant-a", request.Source, request.CollectorID, request.BatchID)
			}
			objects.failing.Store(true)
			config := testIngestServiceConfig(t)
			config.WAL.MaxBytes, config.WAL.SegmentBytes = 32<<10, 16<<10
			config.FlushMaxRequests, config.RetryInterval = 1, time.Millisecond
			service, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Accept(ctx, "tenant-a", request); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for objects.attempts.Load() < 8 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			attempts := objects.attempts.Load()
			if err := service.prune(ctx); err != nil {
				t.Fatal(err)
			}
			crashIngestService(t, service)
			if attempts < 8 {
				t.Fatalf("retry stopped before reaching the failing store: attempts=%d", attempts)
			}
			prepared := 0
			_, _, _, err = scanIngestWAL(config.WAL.Dir, func(record IngestWALRecord) error {
				if record.Type == IngestWALPrepared {
					prepared++
				} else if record.Type == IngestWALAccepted {
					var envelope walIngestEnvelope
					if err := json.Unmarshal(record.Payload, &envelope); err != nil {
						return err
					}
					if envelope.Prepared != nil {
						prepared++
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if prepared != 1 {
				t.Fatalf("durable plans=%d after %d retries, want 1", prepared, attempts)
			}
			objects.failing.Store(false)
			reopened, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIngestService(t, reopened)
			flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := reopened.FlushTenant(flushCtx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			g, manifest, err := store.Load(ctx, "tenant-a")
			if err != nil || manifest.Version != 1 || g.Entities.Len() != 1 {
				t.Fatalf("recovery version=%d err=%v", manifest.Version, err)
			}
			accepted, err := reopened.Accept(ctx, "tenant-a", request)
			if err != nil {
				t.Fatal(err)
			}
			result, err := reopened.Wait(flushCtx, accepted)
			if err != nil || result.Version != 1 || result.SkipReason != IngestSkipReasonIdempotentReplay {
				t.Fatalf("replay=%+v err=%v", result, err)
			}
			t.Logf("%d failed publications used %d PREPARED; reopened and replayed at version %d", attempts, prepared, result.Version)
		})
	}
}

func TestIngestCheckpointDamageFailsClosed(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			store := NewTenantStore(NewMemoryStore(), "test")
			config := testIngestServiceConfig(t)
			config.FlushInterval = time.Hour
			service, err := OpenIngestService(store, config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Accept(context.Background(), "tenant-a", ingestEntityRequest("retained", "host:retained")); err != nil {
				crashIngestService(t, service)
				t.Fatal(err)
			}
			if err := service.prune(context.Background()); err != nil {
				crashIngestService(t, service)
				t.Fatal(err)
			}
			crashIngestService(t, service)
			filename := filepath.Join(config.WAL.Dir, ingestWALCheckpointFile)
			if damage == "missing" {
				err = os.Remove(filename)
			} else {
				err = os.WriteFile(filename, []byte("corrupt checkpoint"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenIngestService(store, config); !errors.Is(err, ErrIngestWALCorrupt) {
				if reopened != nil {
					crashIngestService(t, reopened)
				}
				t.Fatalf("damaged checkpoint accepted: %v", err)
			}
		})
	}
}

func TestLocalImportWaitsForPreparedWAL(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := &localIngestPublicationFault{ObjectStore: files}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.key = store.manifestKey("tenant-a")
	objects.failing.Store(true)
	config := testIngestServiceConfig(t)
	config.FlushMaxRequests, config.RetryInterval = 1, time.Millisecond
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	defer objects.failing.Store(false)
	accepted, err := service.Accept(ctx, "tenant-a", ingestEntityRequest("pending", "host:wal"))
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); objects.attempts.Load() == 0 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if objects.attempts.Load() == 0 {
		t.Fatal("WAL did not reach failed manifest publication")
	}
	task, err := store.StartImport(ctx, "tenant-a", []byte(`{"entity":{"id":"host:import","kind":"host"}}`), ImportOptions{Format: "jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.ShutdownTasks(ctx)
	other, err := service.Accept(ctx, "tenant-b", ingestEntityRequest("other", "host:other"))
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if result, err := service.Wait(waitCtx, other); err != nil || result.Applied != 1 {
		t.Fatalf("other tenant result=%+v err=%v", result, err)
	}
	objects.failing.Store(false)
	if err := service.FlushTenant(waitCtx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	result, err := service.Wait(waitCtx, accepted)
	if err != nil || result.Version != 1 {
		t.Fatalf("WAL result=%+v err=%v", result, err)
	}
	task = waitForTask(t, waitCtx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded {
		t.Fatalf("import: %+v", task)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != 2 || g.Entities.Len() != 2 {
		t.Fatalf("final version=%d err=%v", manifest.Version, err)
	}
	if !service.Readiness().Ready {
		t.Fatalf("WAL readiness=%+v", service.Readiness())
	}
}

func TestLocalImportWALWaitAllowsCompaction(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	for i := 0; i < 2; i++ {
		if _, err := store.Ingest(ctx, "tenant-a", ingestEntityRequest(fmt.Sprintf("seed-%d", i), fmt.Sprintf("host:seed-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 2, RetryAfter: time.Millisecond})
	store.taskExecutionSlots = make(chan struct{}, 1)
	config := testIngestServiceConfig(t)
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	accepted, err := service.Accept(ctx, "tenant-a", ingestEntityRequest("pending", "host:wal"))
	if err != nil {
		t.Fatal(err)
	}
	task, err := store.StartImport(ctx, "tenant-a", []byte(`{"entity":{"id":"host:import","kind":"host"}}`), ImportOptions{Format: "jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.ShutdownTasks(ctx)
	waiting := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		files.runtime.mu.Lock()
		gate := files.runtime.ingestAdmissions[store.tenantObjectPrefix("tenant-a")]
		waiting = gate != nil && gate.writer
		files.runtime.mu.Unlock()
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		t.Fatal("import did not wait for accepted WAL")
	}
	compact, err := store.StartTask(ctx, "tenant-a", TaskTypeCompact, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	compact = waitForTask(t, waitCtx, store, "tenant-a", compact.ID)
	if compact.Status != TaskStatusSucceeded {
		t.Fatalf("compact could not relieve WAL backpressure: %+v", compact)
	}
	if _, err := service.Wait(waitCtx, accepted); err != nil {
		t.Fatal(err)
	}
	task = waitForTask(t, waitCtx, store, "tenant-a", task.ID)
	if task.Status != TaskStatusSucceeded {
		t.Fatalf("import: %+v", task)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != 4 || g.Entities.Len() != 4 {
		t.Fatalf("final version=%d err=%v", manifest.Version, err)
	}
}

func TestLocalWALFailedStatusesBoundMemoryAndReload(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	config := testIngestServiceConfig(t)
	config.QueueMemoryBytes, config.FlushMaxRequests = 1<<20, 1
	config.WAL.MaxBytes, config.WAL.SegmentBytes = 64<<20, 4<<20
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	var first IngestAcceptance
	for i := 0; i < 12; i++ {
		request := ingestEntityRequest(fmt.Sprintf("failed-%d", i), fmt.Sprintf("host:%d", i))
		request.Items[0].ExternalID = strings.Repeat(fmt.Sprintf("%08d", i), (128<<10)/8)
		request.Items[0].Entity.Fields = map[string]any{"payload": strings.Repeat("x", 256<<10)}
		expected := int64(999)
		request.ExpectedVersion = &expected
		accepted, err := service.Accept(ctx, "tenant-a", request)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = accepted
		}
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := service.Wait(waitCtx, accepted)
		cancel()
		if err != nil || result.Failed != 1 {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	service.mu.Lock()
	retained := 0
	for _, cached := range service.activeByStatus {
		retained += len(cached.envelope.Request.Items)
	}
	_, firstCached := service.activeByStatus[ingestStatusKey("tenant-a", first.Source, first.CollectorID, first.BatchID)]
	bytes := service.failedStatusBytes
	service.mu.Unlock()
	if retained != 0 || firstCached || bytes > config.QueueMemoryBytes {
		t.Fatalf("retained items=%d firstCached=%t bytes=%d", retained, firstCached, bytes)
	}
	ready := service.Readiness()
	if ready.Pending != 0 || ready.PendingBytes != 0 {
		t.Fatalf("queue=%+v", ready)
	}
	status, err := service.Status(ctx, "tenant-a", first.Source, first.CollectorID, first.BatchID)
	if err != nil || status.State != IngestStateFailed || status.Result == nil || status.Result.ErrorCode != IngestErrorVersionConflict {
		t.Fatalf("evicted status=%+v err=%v", status, err)
	}
	result, err := service.Wait(ctx, first)
	if err != nil || result.Failed != 1 {
		t.Fatalf("evicted Wait result=%+v err=%v", result, err)
	}
	t.Logf("12 completed failures: no retained request items; status cache bytes=%d; evicted status reloaded", bytes)
}

type localImportManifestPause struct {
	ObjectStore
	key     string
	armed   atomic.Bool
	writes  atomic.Int32
	entered [2]chan struct{}
	release [2]chan struct{}
}

func (s *localImportManifestPause) UnwrapObjectStore() ObjectStore { return s.ObjectStore }
func (s *localImportManifestPause) PutConditional(ctx context.Context, key string, data []byte, c PutCondition) (ObjectMeta, error) {
	if key == s.key && s.armed.Load() {
		n := s.writes.Add(1)
		if n <= 2 {
			close(s.entered[n-1])
			select {
			case <-s.release[n-1]:
			case <-ctx.Done():
				return ObjectMeta{}, ctx.Err()
			}
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, c)
}

func TestLocalRestoreFencesImportAndPersistedRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	files, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := &localImportManifestPause{ObjectStore: files, entered: [2]chan struct{}{make(chan struct{}), make(chan struct{})}, release: [2]chan struct{}{make(chan struct{}), make(chan struct{})}}
	store := NewTenantStore(objects, "test")
	objects.key = store.manifestKey("tenant-a")
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "seed", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	if backup.Status != TaskStatusSucceeded {
		t.Fatal(backup.Error)
	}
	service, err := OpenIngestService(store, testIngestServiceConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	defer store.ShutdownTasks(context.Background())
	objects.armed.Store(true)
	data := []byte("{\"entity\":{\"id\":\"row1\",\"kind\":\"host\"}}\n{\"entity\":{\"id\":\"row2\",\"kind\":\"host\"}}\n{\"entity\":{\"id\":\"row3\",\"kind\":\"host\"}}")
	imp, err := store.StartImport(ctx, "tenant-a", data, ImportOptions{Format: "jsonl", BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-objects.entered[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	type startedTask struct {
		task Task
		err  error
	}
	restoreCh := make(chan startedTask, 1)
	go func() {
		task, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantRestore, map[string]any{"backup_key": stringTaskParam(backup.Result, "backup_manifest_key"), "overwrite": true})
		restoreCh <- startedTask{task, err}
	}()
	waitForTenantLockRefs(t, store, "tenant-a", 2)
	close(objects.release[0])
	started := <-restoreCh
	if started.err != nil {
		t.Fatal(started.err)
	}
	restore := started.task
	select {
	case <-objects.entered[1]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waiting := false
	for ctx.Err() == nil {
		files.runtime.mu.Lock()
		gate := files.runtime.ingestAdmissions[store.tenantObjectPrefix("tenant-a")]
		waiting = gate != nil && gate.writer
		files.runtime.mu.Unlock()
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(objects.release[1])
	if !waiting {
		t.Fatal("restore did not acquire admission while second import batch was publishing")
	}
	restore = waitForTask(t, ctx, store, "tenant-a", restore.ID)
	waitForTaskStatus(t, store, "tenant-a", imp.ID, TaskStatusFailed)
	imp, err = store.GetTask(ctx, "tenant-a", imp.ID)
	if err != nil {
		t.Fatal(err)
	}
	g, m, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if restore.Status != TaskStatusSucceeded || imp.Status != TaskStatusFailed {
		t.Fatalf("restore=%+v import=%+v", restore, imp)
	}
	if m.Version != 1 || g.Entities.Len() != 1 || g.Entities.At("seed").ID != "seed" {
		t.Fatalf("old import crossed restore: version=%d graph=%+v", m.Version, g.Entities)
	}
	if _, err := store.RetryTask(ctx, "tenant-a", imp.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry of old generation: %v", err)
	}
	if err := store.ShutdownTasks(ctx); err != nil {
		t.Fatal(err)
	}
	closeIngestService(t, service)
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := NewTenantStore(reopened, "test")
	defer restarted.ShutdownTasks(context.Background())
	if _, err := restarted.RetryTask(ctx, "tenant-a", imp.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry of persisted old generation: %v", err)
	}
	// Legacy checkpoints cannot prove they belong to the restored generation.
	if _, err := restarted.mutateTask(ctx, "tenant-a", imp.ID, func(task *Task) error {
		delete(task.Checkpoint, taskIngestGenerationCheckpoint)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.RetryTask(ctx, "tenant-a", imp.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry of unbound legacy checkpoint after restore: %v", err)
	}
	fresh, err := restarted.StartImport(ctx, "tenant-a", data, ImportOptions{Format: "jsonl", BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	fresh = waitForTask(t, ctx, restarted, "tenant-a", fresh.ID)
	g, m, err = restarted.Load(ctx, "tenant-a")
	if err != nil || fresh.Status != TaskStatusSucceeded || m.Version != 4 || g.Entities.Len() != 4 {
		t.Fatalf("fresh import=%+v version=%d err=%v", fresh, m.Version, err)
	}
}

type localRecoveryOrphanFault struct{ *localIngestPublicationFault }

func (s *localRecoveryOrphanFault) DeleteConditional(ctx context.Context, key string, c PutCondition) error {
	if s.failing.Load() && strings.Contains(key, "/commits/") {
		return errors.New("injected rollback delete failure")
	}
	return s.ObjectStore.DeleteConditional(ctx, key, c)
}
func TestLocalRecoverDrainsPreparedWALBeforeOrphans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := &localRecoveryOrphanFault{&localIngestPublicationFault{ObjectStore: files}}
	store := NewTenantStore(objects, "test")
	if _, err = store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.key = store.manifestKey("tenant-a")
	objects.failing.Store(true)
	if _, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "orphan", Kind: "host"}}}, CommitOptions{}); err == nil {
		t.Fatal("commit fault missing")
	}
	config := testIngestServiceConfig(t)
	config.FlushMaxRequests = 1
	config.RetryInterval = time.Hour
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	defer objects.failing.Store(false)
	accepted, err := service.Accept(ctx, "tenant-a", ingestEntityRequest("pending", "wal"))
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); objects.attempts.Load() < 2 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if objects.attempts.Load() < 2 {
		t.Fatal("prepared WAL publication was not attempted")
	}
	objects.failing.Store(false)
	report, err := store.RecoverTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if report.StartVersion != 1 || report.EndVersion != 1 || report.Recovered != 0 || report.Skipped != 1 {
		t.Fatalf("recovery must leave the orphan behind the prepared WAL: %+v", report)
	}
	result, err := service.Wait(ctx, accepted)
	if err != nil || result.Version != 1 || result.Applied != 1 {
		t.Fatalf("WAL result=%+v err=%v", result, err)
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != 1 || g.Entities.Len() != 1 || g.Entities.At("wal").ID != "wal" {
		t.Fatalf("recovered graph=%+v version=%d err=%v", g, manifest.Version, err)
	}
	other, err := service.Accept(ctx, "tenant-b", ingestEntityRequest("other", "other"))
	if err != nil {
		t.Fatal(err)
	}
	result, err = service.Wait(ctx, other)
	if err != nil || result.Applied != 1 || !service.Readiness().Ready {
		t.Fatalf("other tenant result=%+v readiness=%+v err=%v", result, service.Readiness(), err)
	}
}

type localImportSourceReads struct {
	ObjectStore
	imports atomic.Int32
}

func (s *localImportSourceReads) UnwrapObjectStore() ObjectStore { return s.ObjectStore }

func (s *localImportSourceReads) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.ObjectStore.Get(ctx, key)
	if err == nil && strings.Contains(key, "/tasks/imports/") {
		s.imports.Add(1)
	}
	return data, err
}

func TestLocalWaitingImportsBoundLoadedSourcesAndAllowCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	objects := &localImportSourceReads{ObjectStore: files}
	store := NewTenantStore(objects, "test")
	const tenants = 12
	for i := 0; i < tenants; i++ {
		if _, err := store.Commit(ctx, fmt.Sprintf("t%d", i), graph.Mutations{UpsertEntities: []graph.Entity{{ID: "seed", Kind: "host"}}}, CommitOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 1, RetryAfter: time.Millisecond})
	config := testIngestServiceConfig(t)
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer crashIngestService(t, service)
	defer store.ShutdownTasks(context.Background())
	var first IngestAcceptance
	for i := 0; i < tenants; i++ {
		tenant := fmt.Sprintf("t%d", i)
		accepted, err := service.Accept(ctx, tenant, ingestEntityRequest("pending", "wal"))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = accepted
		}
		data := []byte(`{"entity":{"id":"import","kind":"host","fields":{"payload":"` + strings.Repeat("x", 64<<10) + `"}}}`)
		if _, err := store.StartImport(ctx, tenant, data, ImportOptions{Format: "jsonl"}); err != nil {
			t.Fatal(err)
		}
		if i < cap(store.taskResidentSlots) {
			waitForLocalIngestPause(t, ctx, files, store.tenantObjectPrefix(tenant))
		}
	}
	// All resident tasks are blocked on WAL; extra queued imports must stay on disk.
	time.Sleep(100 * time.Millisecond)
	if loaded := int(objects.imports.Load()); loaded != cap(store.taskResidentSlots) || len(store.taskExecutionSlots) != 0 {
		t.Fatalf("loaded=%d resident=%d/%d execution=%d", loaded, len(store.taskResidentSlots), cap(store.taskResidentSlots), len(store.taskExecutionSlots))
	}
	compact, err := store.StartTask(ctx, "t0", TaskTypeCompact, nil)
	if err != nil {
		t.Fatal(err)
	}
	compact = waitForTask(t, ctx, store, "t0", compact.ID)
	if compact.Status != TaskStatusSucceeded {
		t.Fatalf("compact with all resident slots held: %+v", compact)
	}
	if result, err := service.Wait(ctx, first); err != nil || result.Applied != 1 {
		t.Fatalf("compacted tenant WAL result=%+v err=%v", result, err)
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := store.ShutdownTasks(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if len(store.taskResidentSlots) != 0 || len(store.taskExecutionSlots) != 0 || len(store.taskQueueSlots) != 0 {
		t.Fatalf("shutdown leaked admissions: resident=%d execution=%d queued=%d", len(store.taskResidentSlots), len(store.taskExecutionSlots), len(store.taskQueueSlots))
	}
}

func TestLocalRecoverWALWaitAllowsCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := NewTenantStore(files, "test")
	for i := 0; i < 2; i++ {
		if _, err := store.Ingest(ctx, "tenant-a", ingestEntityRequest(fmt.Sprintf("seed-%d", i), fmt.Sprintf("seed-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 2, RetryAfter: time.Millisecond})
	store.taskExecutionSlots = make(chan struct{}, 1)
	config := testIngestServiceConfig(t)
	config.FlushMaxRequests = 1
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer crashIngestService(t, service)
	defer store.ShutdownTasks(context.Background())
	accepted, err := service.Accept(ctx, "tenant-a", ingestEntityRequest("pending", "wal"))
	if err != nil {
		t.Fatal(err)
	}
	recoverCtx, release, err := store.TryAcquireMaintenanceContext(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	recovered := make(chan struct{})
	var recoverErr error
	go func() {
		defer close(recovered)
		defer release()
		report, err := store.RecoverTenant(recoverCtx, "tenant-a")
		if err == nil && report.EndVersion != 3 {
			err = fmt.Errorf("recovered version %d, want 3", report.EndVersion)
		}
		recoverErr = err
	}()
	// Cancel and join before closing the store, including on assertion failure.
	defer func() {
		cancel()
		<-recovered
	}()
	waitForLocalIngestPause(t, ctx, files, store.tenantObjectPrefix("tenant-a"))
	compact, err := store.StartTask(ctx, "tenant-a", TaskTypeCompact, nil)
	if err != nil {
		t.Fatal(err)
	}
	compact = waitForTask(t, ctx, store, "tenant-a", compact.ID)
	if compact.Status != TaskStatusSucceeded {
		t.Fatalf("compact while recover waits: %+v", compact)
	}
	if _, err := service.Wait(ctx, accepted); err != nil {
		t.Fatal(err)
	}
	select {
	case <-recovered:
		if recoverErr != nil {
			t.Fatal(recoverErr)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !service.Readiness().Ready {
		t.Fatalf("readiness=%+v", service.Readiness())
	}
}

func waitForLocalIngestPause(t *testing.T, ctx context.Context, files *FileStore, prefix string) {
	t.Helper()
	for ctx.Err() == nil {
		files.runtime.mu.Lock()
		gate := files.runtime.ingestAdmissions[prefix]
		waiting := gate != nil && gate.writer
		files.runtime.mu.Unlock()
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("operation did not pause WAL admission: ", ctx.Err())
}

func TestWALCoalescedPreparedOversizeSplitsBeforePublication(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	config := testIngestServiceConfig(t)
	config.WAL.SegmentBytes = 12 << 10
	config.FlushInterval = time.Hour
	config.FlushMaxRequests = 100
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	var accepted []IngestAcceptance
	var requests []IngestRequest
	for i := 0; i < 3; i++ {
		request := ingestEntityRequest(fmt.Sprint("batch-", i), fmt.Sprint("host:", i))
		request.Items[0].Entity.Fields["payload"] = strings.Repeat("x", 4<<10)
		a, e := service.Accept(ctx, "tenant-a", request)
		if e != nil {
			t.Fatal(e)
		}
		accepted = append(accepted, a)
		requests = append(requests, request)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	flushErr := service.FlushTenant(waitCtx, "tenant-a")
	t.Logf("flush=%v", flushErr)
	for i, a := range accepted {
		result, e := service.Wait(waitCtx, a)
		replayed, re := store.Ingest(ctx, "tenant-a", requests[i])
		t.Logf("accepted result applied=%d failed=%d err=%v; same batch replay applied=%d failed=%d skipped=%v err=%v", result.Applied, result.Failed, e, replayed.Applied, replayed.Failed, replayed.Skipped, re)
		if e != nil || result.Failed > 0 || result.Applied != 1 || re != nil || replayed.Failed > 0 || !replayed.Skipped {
			t.Errorf("valid accepted request became permanently failed")
		}
	}
}

type deadlineGenerationStore struct{ *TenantStore }

func (s *deadlineGenerationStore) CaptureIngestWALGeneration(ctx context.Context, tenant string) (int64, error) {
	if _, ok := ctx.Deadline(); !ok {
		return 0, errors.New("capture has no deadline")
	}
	<-ctx.Done()
	return 0, ctx.Err()
}
func TestWALGenerationCaptureTimeoutReleasesIdentity(t *testing.T) {
	store := &deadlineGenerationStore{NewTenantStore(NewMemoryStore(), "test")}
	service, err := OpenIngestService(store, testIngestServiceConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, service)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := ingestEntityRequest("capture", "host:a")
	for i := 0; i < 2; i++ {
		_, err := service.Accept(ctx, "tenant-a", request)
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			t.Fatalf("capture %d: %v, caller=%v", i, err, ctx.Err())
		}
	}
}

func TestWALBacklogLargerThanCommitTailMakesProgress(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 4, RetryAfter: time.Millisecond})
	config := testIngestServiceConfig(t)
	config.FlushInterval, config.FlushMaxRequests = time.Hour, 20
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer crashIngestService(t, service)
	for i := 0; i < 10; i++ {
		if _, err := service.Accept(ctx, "tenant-a", ingestEntityRequest(fmt.Sprint(i), fmt.Sprintf("host:%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	flushCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_ = service.FlushTenant(flushCtx, "tenant-a")
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil || manifest.Version == 0 || manifest.Version > 4 {
		t.Fatalf("oversized backlog made no bounded progress: version=%d error=%v", manifest.Version, err)
	}
}

func TestDeferredIndexUsesTenantEstimateAndRetriesPublication(t *testing.T) {
	for _, failPublication := range []bool{false, true} {
		t.Run(fmt.Sprint("publication_failure_", failPublication), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			objects := &localIngestPublicationFault{ObjectStore: files, err: ErrObjectStoreUnavailable}
			store := NewTenantStore(objects, "test")
			store.MaxMaintenanceBytes = 32 << 20
			store.WriteEntityRecords = false
			defer store.ShutdownTasks(context.Background())
			if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			_, releaseHeld, err := store.admitMaintenance(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseHeld()
			objects.key = store.indexCatalogKey("tenant-a")
			objects.failing.Store(failPublication)
			endMaintenance := store.beginLocalIndexMaintenance(ctx, "tenant-a")
			result, err := store.Ingest(ctx, "tenant-a", ingestEntityRequest("catchup", "host:new"))
			endMaintenance()
			if err != nil {
				t.Fatal(err)
			}
			for {
				if objects.attempts.Load() > 0 {
					objects.failing.Store(false)
				}
				catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
				if err == nil && catalog.Version == result.Version {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("catch-up failed with one byte held: attempts=%d catalog=%d result=%d error=%v", objects.attempts.Load(), catalog.Version, result.Version, err)
				case <-time.After(5 * time.Millisecond):
				}
			}
			if failPublication && objects.attempts.Load() == 0 {
				t.Fatal("publication fault was not exercised")
			}
		})
	}
}

func TestWALUsesRemainingCommitTailBeforeCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 4, RetryAfter: time.Millisecond})
	config := testIngestServiceConfig(t)
	config.FlushInterval, config.FlushMaxRequests = time.Hour, 10
	service, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	var requests []IngestRequest
	var accepted []IngestAcceptance
	for i := 0; i < 4; i++ {
		request := ingestEntityRequest(fmt.Sprint(i), fmt.Sprintf("host:%d", i))
		a, err := service.Accept(ctx, "tenant-a", request)
		if err != nil {
			crashIngestService(t, service)
			t.Fatal(err)
		}
		requests = append(requests, request)
		accepted = append(accepted, a)
	}
	service.forceCh <- ingestForceRequest{tenantID: "tenant-a", throughLSN: accepted[3].acceptedLSN}
	_, err = service.Wait(ctx, accepted[2])
	manifest, manifestErr := store.CurrentManifest(ctx, "tenant-a")
	crashIngestService(t, service)
	if err != nil || manifestErr != nil || manifest.Version != 4 {
		t.Fatalf("remaining capacity not used exactly: version=%d flush=%v manifest=%v", manifest.Version, err, manifestErr)
	}
	// The full tail must retain the final request durably until compact frees
	// capacity. Recovery must preserve order and each batch's original result.
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenIngestService(store, config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIngestService(t, recovered)
	if err := recovered.FlushTenant(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	for i, request := range requests {
		a, err := recovered.Accept(ctx, "tenant-a", request)
		if err != nil {
			t.Fatal(err)
		}
		if err := recovered.FlushTenant(ctx, "tenant-a"); err != nil {
			t.Fatal(err)
		}
		result, err := recovered.Wait(ctx, a)
		if err != nil || result.Failed != 0 || result.Applied != 1 || result.Version != int64(i+2) {
			t.Fatalf("replayed batch %d lost order/result: %+v err=%v", i, result, err)
		}
	}
	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil || manifest.Version != 5 || g.Entities.Len() != 5 {
		t.Fatalf("recovery duplicated or lost writes: version=%d err=%v", manifest.Version, err)
	}
}
