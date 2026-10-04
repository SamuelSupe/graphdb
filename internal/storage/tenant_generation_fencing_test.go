package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestTaskProgressValidationReadFailurePreservesTask(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(fmt.Sprint("bound=", bound), func(t *testing.T) {
			ctx := context.Background()
			objects := &taskValidationReadFailureStore{ObjectStore: NewMemoryStore()}
			store := NewTenantStore(objects, "test")
			if _, err := store.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if bound {
				var err error
				ctx, err = store.acquireAndBindWriterFence(ctx, "tenant-a")
				if err != nil {
					t.Fatal(err)
				}
			}
			task := Task{ID: "published-task", TenantID: "tenant-a", Type: TaskTypeCompact,
				Status: TaskStatusRunning, StartedAt: time.Now().UTC()}
			if err := store.saveTask(ctx, task); err != nil {
				t.Fatal(err)
			}
			objects.writeKey = store.taskKey(task.TenantID, task.ID)
			objects.readKey = store.manifestKey(task.TenantID)
			if bound {
				objects.readKey = store.writerLeaseKey(task.TenantID)
			}
			err := store.updateTaskProgress(ctx, task, "compacting", 1, 2, nil)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("validation read error was masked: %v", err)
			}
			if _, err := store.GetTask(context.Background(), task.TenantID, task.ID); err != nil {
				t.Fatalf("published task became unreadable after a validation read failed: %v", err)
			}
		})
	}
}

func TestTaskProgressValidationReadFailureRollsBackRaftApplication(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(fmt.Sprint("bound=", bound), func(t *testing.T) {
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			if err := files.RequireReplicatedWrites(); err != nil {
				t.Fatal(err)
			}
			objects := &taskValidationReadFailureStore{ObjectStore: files}
			store := NewTenantStore(objects, "test")
			task := Task{ID: "published-task", TenantID: "tenant-a", Type: TaskTypeCompact,
				Status: TaskStatusRunning, StartedAt: time.Now().UTC()}
			_, err = files.ApplyReplicated(context.Background(), 1, "create-task", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
				if _, err := store.CreateTenant(ctx, task.TenantID, TenantCreateOptions{}); err != nil {
					return nil, err
				}
				return nil, store.saveTask(ctx, task)
			})
			if err != nil {
				t.Fatal(err)
			}
			objects.writeKey = store.taskKey(task.TenantID, task.ID)
			objects.readKey = store.manifestKey(task.TenantID)
			if bound {
				objects.readKey = store.writerLeaseKey(task.TenantID)
			}
			_, err = files.ApplyReplicated(context.Background(), 2, "task-progress", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
				if bound {
					var err error
					ctx, err = store.acquireAndBindWriterFence(ctx, task.TenantID)
					if err != nil {
						return nil, err
					}
				}
				// HTTP handlers can translate a storage error into a response.
				_ = store.updateTaskProgress(ctx, task, "compacting", 1, 2, nil)
				return []byte("translated-error"), nil
			})
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("validation I/O failure was checkpointed: %v", err)
			}
			checkpoint, err := files.ReplicationCheckpoint()
			if err != nil || checkpoint.Index != 1 {
				t.Fatalf("failed validation advanced checkpoint: %+v, %v", checkpoint, err)
			}
			current, err := store.GetTask(context.Background(), task.TenantID, task.ID)
			if err != nil || current.ProgressCompleted != 0 || current.Phase != "" {
				t.Fatalf("failed validation did not restore the acknowledged task: %+v, %v", current, err)
			}
		})
	}
}

type taskValidationReadFailureStore struct {
	ObjectStore
	writeKey string
	readKey  string
	armed    bool
}

func (s *taskValidationReadFailureStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err == nil && key == s.writeKey {
		s.armed = true
	}
	return meta, err
}

func (s *taskValidationReadFailureStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if s.armed && key == s.readKey {
		s.armed = false
		return nil, ObjectMeta{Key: key}, io.ErrUnexpectedEOF
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func TestLateTenantConfigWriteIsRolledBackAfterPurge(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	probe := NewTenantStore(base, "test")
	objects := &purgeDuringPutStore{
		ObjectStore: base,
		base:        base,
		tenantID:    "tenant-a",
		triggerKey:  probe.tenantConfigKey("tenant-a"),
	}
	stale := NewTenantStore(objects, "test")
	if _, err := stale.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	_, err := stale.PutTenantConfig(ctx, "tenant-a", TenantConfig{})
	if !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrTenantDeleted) && !errors.Is(err, ErrConflict) {
		t.Fatalf("late config write err = %v, want fencing error", err)
	}
	if !objects.Triggered() {
		t.Fatal("purge was not triggered during config write")
	}
	if _, err := base.Get(ctx, stale.tenantConfigKey("tenant-a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("late config survived purge: %v", err)
	}

	recreator := NewTenantStore(base, "test")
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}
	if _, configured, err := recreator.GetTenantConfig(ctx, "tenant-a"); err != nil || configured {
		t.Fatalf("recreated tenant inherited stale config: configured=%v err=%v", configured, err)
	}
}

func TestLateTaskProgressCannotRecreateTaskAfterPurge(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	objects := newBlockingTaskPutStore(base)
	runner := NewTenantStore(objects, "test")
	if _, err := runner.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	now := time.Now().UTC()
	task := Task{
		ID: "late-task", TenantID: "tenant-a", Type: TaskTypeCompact,
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		OwnerID: runner.InstanceID, StartedAt: now, UpdatedAt: now,
	}
	if err := runner.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}

	entered, release := objects.blockNextPut(runner.taskKey("tenant-a", task.ID))
	done := make(chan error, 1)
	go func() {
		done <- runner.updateTaskProgress(ctx, task, "compact", 1, 2, nil)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("task progress did not reach blocked write")
	}
	time.Sleep(20 * time.Millisecond)
	purger := NewTenantStore(base, "test")
	if _, err := purger.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, ErrTenantDeleted) && !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("late task progress err = %v, want fencing error", err)
	}
	if _, err := base.Get(ctx, runner.taskKey("tenant-a", task.ID)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("late task was recreated after purge: %v", err)
	}
}

func TestBoundTaskCannotAdoptRecreatedTenantGeneration(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	runner := NewTenantStore(base, "test")
	if _, err := runner.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	boundCtx, err := runner.acquireAndBindWriterFence(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("bind task generation: %v", err)
	}
	now := time.Now().UTC()
	task := Task{
		ID: "old-generation-task", TenantID: "tenant-a", Type: TaskTypeCompact,
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		OwnerID: runner.InstanceID, StartedAt: now, UpdatedAt: now,
	}
	if err := runner.saveTask(boundCtx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	if err := expireWriterLeaseForTakeover(ctx, base, "tenant-a"); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	purger := NewTenantStore(base, "test")
	if _, err := purger.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	recreator := NewTenantStore(base, "test")
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}

	err = runner.updateTaskProgress(boundCtx, task, "compact", 1, 2, nil)
	if !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("old task progress err = %v, want fencing error", err)
	}
	if _, err := base.Get(ctx, runner.taskKey("tenant-a", task.ID)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old task crossed into recreated tenant: %v", err)
	}
	if err := runner.saveBackupSchedule(boundCtx, "tenant-a", BackupAutomationStatus{TaskID: task.ID}); !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("old backup schedule crossed into recreated tenant: %v", err)
	}
	if _, err := base.Get(ctx, runner.backupScheduleKey("tenant-a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recreated tenant inherited old backup schedule: %v", err)
	}
}

func TestBoundIndexTaskCannotAdoptRecreatedTenantGeneration(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	runner := NewTenantStore(base, "test")
	if _, err := runner.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	boundCtx, err := runner.acquireAndBindWriterFence(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("bind task generation: %v", err)
	}
	task := IndexTask{
		ID: "old-index-task", TenantID: "tenant-a", Type: "rebuild",
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		OwnerID: runner.InstanceID, StartedAt: time.Now().UTC(),
	}
	if err := runner.saveIndexTask(boundCtx, task); err != nil {
		t.Fatalf("save index task: %v", err)
	}
	if err := expireWriterLeaseForTakeover(ctx, base, "tenant-a"); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	purger := NewTenantStore(base, "test")
	if _, err := purger.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	recreator := NewTenantStore(base, "test")
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}

	err = runner.saveIndexTask(boundCtx, task)
	if !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("old index task err = %v, want fencing error", err)
	}
	if _, err := base.Get(ctx, runner.indexTaskKey("tenant-a", task.ID)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old index task crossed into recreated tenant: %v", err)
	}
}

func TestLateReaderHeartbeatCannotRecreateObjectAfterPurge(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	probe := NewTenantStore(base, "test")
	objects := &purgeDuringPutStore{
		ObjectStore: base,
		base:        base,
		tenantID:    "tenant-a",
		triggerKey:  probe.readerHeartbeatKey("tenant-a", "reader-a"),
	}
	writer := NewTenantStore(base, "test")
	if _, err := writer.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	time.Sleep(time.Millisecond)
	reader := NewTenantStore(objects, "test")
	_, err := reader.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{ReaderID: "reader-a", Status: "fresh"})
	if !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("late heartbeat err = %v, want ErrTenantDeleted", err)
	}
	if !objects.Triggered() {
		t.Fatal("purge was not triggered during heartbeat write")
	}
	if _, err := base.Get(ctx, reader.readerHeartbeatKey("tenant-a", "reader-a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("late heartbeat survived purge: %v", err)
	}
}

func TestLateCommitObjectCannotCrossPurgeAndRecreateGeneration(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	probe := NewTenantStore(base, "test")
	objects := &purgeDuringPutStore{
		ObjectStore:   base,
		base:          base,
		tenantID:      "tenant-a",
		triggerPrefix: probe.commitPrefix("tenant-a"),
		recreate:      true,
	}
	stale := NewTenantStore(objects, "test")
	if _, err := stale.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	time.Sleep(time.Millisecond)
	_, err := stale.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:stale", Kind: "host"}}}, CommitOptions{})
	if !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("late commit err = %v, want fencing error", err)
	}
	if !objects.Triggered() {
		t.Fatal("purge and recreate were not triggered during commit object write")
	}
	commits, err := base.List(ctx, probe.commitPrefix("tenant-a"))
	if err != nil {
		t.Fatalf("list commits: %v", err)
	}
	if len(commits) != 0 {
		t.Fatalf("late commit objects survived generation change: %#v", commits)
	}
	fresh := NewTenantStore(base, "test")
	g, manifest, err := fresh.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load recreated tenant: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("recreated manifest version = %d, want 0", manifest.Version)
	}
	if _, ok := g.GetEntity("host:stale"); ok {
		t.Fatal("stale entity crossed into recreated tenant")
	}
}

type purgeDuringPutStore struct {
	ObjectStore
	base          *MemoryStore
	tenantID      string
	triggerKey    string
	triggerPrefix string
	recreate      bool

	mu        sync.Mutex
	triggered bool
}

func (s *purgeDuringPutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		if err := expireWriterLeaseForTakeover(ctx, s.base, s.tenantID); err != nil {
			return ObjectMeta{}, err
		}
		purger := NewTenantStore(s.base, "test")
		if _, err := purger.PurgeTenant(ctx, s.tenantID, true); err != nil {
			return ObjectMeta{}, err
		}
		if s.recreate {
			recreator := NewTenantStore(s.base, "test")
			if _, err := recreator.CreateTenant(ctx, s.tenantID, TenantCreateOptions{}); err != nil {
				return ObjectMeta{}, err
			}
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *purgeDuringPutStore) shouldTrigger(key string) bool {
	if key != s.triggerKey && (s.triggerPrefix == "" || !strings.HasPrefix(key, s.triggerPrefix)) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *purgeDuringPutStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

func TestLocalTaskCannotAdoptRecreatedTenantGeneration(t *testing.T) {
	ctx := context.Background()
	base, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	runner := NewTenantStore(base, "test")
	if _, err := runner.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	boundCtx, err := runner.acquireAndBindWriterFence(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("bind task generation: %v", err)
	}
	now := time.Now().UTC()
	task := Task{
		ID: "old-generation-task", TenantID: "tenant-a", Type: TaskTypeCompact,
		Status: TaskStatusRunning, Phase: TaskStatusRunning,
		OwnerID: runner.InstanceID, StartedAt: now, UpdatedAt: now,
	}
	if err := runner.saveTask(boundCtx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	purger := runner
	if _, err := purger.PurgeTenant(ctx, "tenant-a", true); err != nil {
		t.Fatalf("purge tenant: %v", err)
	}
	recreator := runner
	if _, err := recreator.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}

	err = runner.updateTaskProgress(boundCtx, task, "compact", 1, 2, nil)
	if !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrTenantDeleted) {
		t.Fatalf("old task progress err = %v, want fencing error", err)
	}
	if _, err := base.Get(ctx, runner.taskKey("tenant-a", task.ID)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old task crossed into recreated tenant: %v", err)
	}
}
