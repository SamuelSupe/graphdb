package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type localIngestPublicationFault struct {
	ObjectStore
	key      string
	failing  atomic.Bool
	attempts atomic.Int32
}

func (s *localIngestPublicationFault) UnwrapObjectStore() ObjectStore { return s.ObjectStore }

func (s *localIngestPublicationFault) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if key == s.key && s.failing.Load() {
		s.attempts.Add(1)
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
			crashIngestService(t, service)
			if attempts < 8 {
				t.Fatalf("retry stopped before reaching the failing store: attempts=%d", attempts)
			}
			prepared := 0
			_, _, _, err = scanIngestWAL(config.WAL.Dir, func(record IngestWALRecord) error {
				if record.Type == IngestWALPrepared {
					prepared++
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
			if err != nil || manifest.Version != 1 || len(g.Entities) != 1 {
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
	if err != nil || manifest.Version != 2 || len(g.Entities) != 2 {
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
	if err != nil || manifest.Version != 4 || len(g.Entities) != 4 {
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
	if m.Version != 1 || len(g.Entities) != 1 || g.Entities["seed"].ID != "seed" {
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
	if err != nil || fresh.Status != TaskStatusSucceeded || m.Version != 4 || len(g.Entities) != 4 {
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
	if err != nil || manifest.Version != 1 || len(g.Entities) != 1 || g.Entities["wal"].ID != "wal" {
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
