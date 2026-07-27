package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

type indexTaskLeaseCoordinator struct {
	WriteCoordinator
	lease         CoordinatorTaskLease
	active        bool
	calls         int
	acquiredOwner string
	released      bool
}

func (*indexTaskLeaseCoordinator) Backend() string {
	return CoordinationPostgres
}

func (*indexTaskLeaseCoordinator) Namespace() string {
	return "index-task-test"
}

func (c *indexTaskLeaseCoordinator) TaskLease(
	_ context.Context,
	tenantID string,
	taskType string,
) (CoordinatorTaskLease, bool, error) {
	c.calls++
	if !c.active ||
		c.lease.TenantID != tenantID ||
		c.lease.TaskType != taskType {
		return CoordinatorTaskLease{}, false, nil
	}
	return c.lease, true, nil
}

func (c *indexTaskLeaseCoordinator) AcquireTaskLease(
	_ context.Context,
	tenantID string,
	taskType string,
	owner string,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	c.acquiredOwner = owner
	c.active = true
	c.lease = CoordinatorTaskLease{
		TenantID:   tenantID,
		TaskType:   taskType,
		OwnerToken: owner,
		FenceEpoch: 1,
		ExpiresAt:  time.Now().UTC().Add(ttl),
	}
	return c.lease, true, nil
}

func (c *indexTaskLeaseCoordinator) ReleaseTaskLease(
	_ context.Context,
	lease CoordinatorTaskLease,
) error {
	if lease.OwnerToken == c.lease.OwnerToken &&
		lease.FenceEpoch == c.lease.FenceEpoch {
		c.active = false
		c.released = true
	}
	return nil
}

func TestIndexTaskActiveUsesPostgresTaskLeaseAfterStartupGrace(t *testing.T) {
	now := time.Now().UTC()
	coordinator := &indexTaskLeaseCoordinator{
		active: true,
		lease: CoordinatorTaskLease{
			TenantID:   "tenant-a",
			TaskType:   coordinatorLifecycleTaskType,
			OwnerToken: "writer-a/task-a",
			ExpiresAt:  now.Add(time.Minute),
		},
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Second
	store.SetCoordinator(coordinator)
	active, err := store.indexTaskActive(
		context.Background(),
		"tenant-a",
		IndexTask{
			ID:        "task-a",
			TenantID:  "tenant-a",
			Type:      "rebuild",
			Status:    TaskStatusRunning,
			OwnerID:   "writer-a",
			StartedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		},
		now,
	)
	if err != nil {
		t.Fatalf("index task active: %v", err)
	}
	if !active {
		t.Fatal("active PostgreSQL task lease was treated as stale")
	}
	if coordinator.calls != 2 {
		t.Fatalf("task lease reads = %d, want 2", coordinator.calls)
	}
}

func TestIndexTaskActiveUsesPostgresQueueLeaseBeforeExecution(t *testing.T) {
	now := time.Now().UTC()
	coordinator := &indexTaskLeaseCoordinator{
		active: true,
		lease: CoordinatorTaskLease{
			TenantID:   "tenant-a",
			TaskType:   coordinatorQueuedIndexTaskLeaseType(),
			OwnerToken: "writer-a/task-a",
			ExpiresAt:  now.Add(time.Minute),
		},
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Second
	store.SetCoordinator(coordinator)
	active, err := store.indexTaskActive(
		context.Background(),
		"tenant-a",
		IndexTask{
			ID:        "task-a",
			TenantID:  "tenant-a",
			Type:      "rebuild",
			Status:    TaskStatusRunning,
			OwnerID:   "writer-a",
			StartedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		},
		now,
	)
	if err != nil {
		t.Fatalf("index task active: %v", err)
	}
	if !active {
		t.Fatal("active PostgreSQL queue lease was treated as stale")
	}
	if coordinator.calls != 1 {
		t.Fatalf("task lease reads = %d, want 1", coordinator.calls)
	}
}

func TestIndexTaskActiveIgnoresDifferentPostgresLifecycleLease(t *testing.T) {
	now := time.Now().UTC()
	coordinator := &indexTaskLeaseCoordinator{
		active: true,
		lease: CoordinatorTaskLease{
			TenantID:   "tenant-a",
			TaskType:   coordinatorLifecycleTaskType,
			OwnerToken: "gc-writer/gc-task",
			ExpiresAt:  now.Add(time.Minute),
		},
	}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Second
	store.SetCoordinator(coordinator)
	active, err := store.indexTaskActive(
		context.Background(),
		"tenant-a",
		IndexTask{
			ID:        "task-a",
			TenantID:  "tenant-a",
			Type:      "rebuild",
			Status:    TaskStatusRunning,
			OwnerID:   "writer-a",
			StartedAt: now.Add(-time.Minute),
			UpdatedAt: now.Add(-time.Minute),
		},
		now,
	)
	if err != nil {
		t.Fatalf("index task active: %v", err)
	}
	if active {
		t.Fatal("unrelated PostgreSQL lifecycle lease kept a stale index task active")
	}
}

func TestIndexTaskActiveAllowsRecentPostgresTaskToAcquireLease(t *testing.T) {
	now := time.Now().UTC()
	coordinator := &indexTaskLeaseCoordinator{}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Minute
	store.SetCoordinator(coordinator)
	active, err := store.indexTaskActive(
		context.Background(),
		"tenant-a",
		IndexTask{
			ID:        "task-a",
			TenantID:  "tenant-a",
			Type:      "rebuild",
			Status:    TaskStatusRunning,
			StartedAt: now.Add(-time.Second),
		},
		now,
	)
	if err != nil {
		t.Fatalf("index task active: %v", err)
	}
	if !active {
		t.Fatal("recent queued PostgreSQL task lost its startup grace")
	}
}

func TestIndexRebuildWorkerHoldsCoordinatorLeaseUntilStopped(t *testing.T) {
	coordinator := &indexTaskLeaseCoordinator{}
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Minute
	store.SetCoordinator(coordinator)
	ctx, stop, err := store.startIndexRebuildTaskLease(
		context.Background(),
		IndexTask{
			ID:       "task-a",
			TenantID: "tenant-a",
			OwnerID:  store.InstanceID,
		},
	)
	if err != nil {
		t.Fatalf("start index task lease: %v", err)
	}
	if !coordinatorLeaseContextMatches(
		ctx,
		"tenant-a",
		coordinatorLifecycleTaskType,
	) {
		t.Fatal("index worker context is not bound to lifecycle lease")
	}
	if coordinator.acquiredOwner != store.InstanceID+"/task-a" {
		t.Fatalf(
			"lease owner = %q, want %q",
			coordinator.acquiredOwner,
			store.InstanceID+"/task-a",
		)
	}
	stop()
	if !coordinator.released {
		t.Fatal("index worker did not release task lease")
	}
}

func TestPostgresIndexTaskDeduplicatesDuringPublication(t *testing.T) {
	ctx := context.Background()
	objects := &blockOncePutStore{
		ObjectStore: NewMemoryStore(),
		substring:   "/indexes/running/rebuild.parquet",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	coordinator := newTaskLeaseTestCoordinator()
	coordinator.contended = make(chan struct{})
	first := NewTenantStore(objects, "test")
	second := NewTenantStore(objects, "test")
	first.SetCoordinator(coordinator)
	second.SetCoordinator(coordinator)
	for range defaultTaskExecutionLimit {
		first.taskExecutionSlots <- struct{}{}
		second.taskExecutionSlots <- struct{}{}
	}
	releaseSlots := func() {
		for range defaultTaskExecutionLimit {
			<-first.taskExecutionSlots
			<-second.taskExecutionSlots
		}
	}
	defer releaseSlots()

	type startResult struct {
		task IndexTask
		err  error
	}
	firstDone := make(chan startResult, 1)
	go func() {
		task, err := first.StartIndexRebuild(ctx, "tenant-a")
		firstDone <- startResult{task: task, err: err}
	}()
	<-objects.paused
	secondDone := make(chan startResult, 1)
	go func() {
		task, err := second.StartIndexRebuild(ctx, "tenant-a")
		secondDone <- startResult{task: task, err: err}
	}()
	<-coordinator.contended
	select {
	case early := <-secondDone:
		close(objects.resume)
		<-firstDone
		t.Fatalf(
			"duplicate returned before the running marker was published: %#v",
			early,
		)
	case <-time.After(25 * time.Millisecond):
	}
	close(objects.resume)
	started := <-firstDone
	if started.err != nil {
		t.Fatalf("start first index task: %v", started.err)
	}
	reused := <-secondDone
	if reused.err != nil {
		t.Fatalf("start duplicate index task: %v", reused.err)
	}
	if reused.task.ID != started.task.ID {
		t.Fatalf(
			"duplicate index task id = %q, want %q",
			reused.task.ID,
			started.task.ID,
		)
	}
}

func TestPostgresIndexTaskDedupIgnoresStaleNegativeMarkerCache(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	cacheConfig := WriterObjectCacheConfig{
		MaxBytes:    1 << 20,
		MaxKeys:     100,
		NegativeTTL: time.Hour,
	}
	first := NewTenantStore(
		NewWriterObjectCache(objects, cacheConfig),
		"test",
	)
	second := NewTenantStore(
		NewWriterObjectCache(objects, cacheConfig),
		"test",
	)
	coordinator := newTaskLeaseTestCoordinator()
	first.SetCoordinator(coordinator)
	second.SetCoordinator(coordinator)
	if _, err := second.getIndexRebuildRunningMarker(
		ctx,
		"tenant-a",
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prime missing running marker err = %v", err)
	}
	for range defaultTaskExecutionLimit {
		first.taskExecutionSlots <- struct{}{}
	}
	defer func() {
		for range defaultTaskExecutionLimit {
			<-first.taskExecutionSlots
		}
	}()

	started, err := first.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start first index task: %v", err)
	}
	reused, err := second.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start duplicate index task: %v", err)
	}
	if reused.ID != started.ID {
		t.Fatalf(
			"duplicate index task id = %q, want %q",
			reused.ID,
			started.ID,
		)
	}
}

func TestIndexTaskStartDoesNotReuseStaleProcessCache(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	old := time.Now().UTC().Add(-time.Hour)
	stale := IndexTask{
		ID:         "stale-cached-task",
		TenantID:   "tenant-a",
		Type:       "rebuild",
		Status:     TaskStatusSucceeded,
		Phase:      "done",
		OwnerID:    "remote-writer",
		StartedAt:  old,
		UpdatedAt:  old,
		FinishedAt: old,
	}
	if err := store.saveIndexTask(ctx, stale); err != nil {
		t.Fatalf("save completed index task: %v", err)
	}
	stale.Status = TaskStatusRunning
	stale.Phase = TaskStatusRunning
	stale.FinishedAt = time.Time{}
	store.taskMu.Lock()
	store.indexTasks[stale.TenantID] = stale
	store.taskMu.Unlock()
	for range defaultTaskExecutionLimit {
		store.taskExecutionSlots <- struct{}{}
	}
	defer func() {
		for range defaultTaskExecutionLimit {
			<-store.taskExecutionSlots
		}
	}()

	started, err := store.StartIndexRebuild(ctx, stale.TenantID)
	if err != nil {
		t.Fatalf("start index task: %v", err)
	}
	if started.ID == stale.ID {
		t.Fatalf("reused stale process-cached index task %q", stale.ID)
	}
}

func TestCanceledIndexTaskAdmissionPersistsTerminalStatus(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	now := time.Now().UTC()
	task := IndexTask{
		ID:            "canceled-before-execution",
		TenantID:      "tenant-a",
		Type:          "rebuild",
		Status:        TaskStatusRunning,
		Phase:         TaskStatusQueued,
		ProgressTotal: 1,
		OwnerID:       store.InstanceID,
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := store.publishQueuedIndexTask(ctx, task); err != nil {
		t.Fatalf("publish queued index task: %v", err)
	}
	store.taskMu.Lock()
	store.indexTasks[task.TenantID] = task
	store.taskMu.Unlock()
	if !store.reserveQueuedTask() {
		t.Fatal("reserve queued task")
	}
	tenantSlot := store.taskTenantSlot(task.TenantID)
	tenantSlot <- struct{}{}

	runCtx, cancel := context.WithCancel(ctx)
	cancel()
	store.runIndexTaskAdmitted(runCtx, task.TenantID, task)
	<-tenantSlot

	loaded, err := store.GetIndexTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get index task: %v", err)
	}
	if loaded.Status != TaskStatusFailed ||
		loaded.Phase != TaskStatusFailed ||
		loaded.FinishedAt.IsZero() {
		t.Fatalf("canceled queued index task = %#v", loaded)
	}
	if _, err := store.getIndexRebuildRunningMarker(
		ctx,
		task.TenantID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get running marker err = %v, want ErrNotFound", err)
	}
}

func TestGetIndexTaskFailsAfterOwnerLeaseExpires(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.LeaseTTL = time.Millisecond
	old := time.Now().UTC().Add(-time.Minute)
	task := IndexTask{
		ID:        "stale-index-task",
		TenantID:  "tenant-a",
		Type:      "rebuild",
		Status:    TaskStatusRunning,
		Phase:     TaskStatusRunning,
		OwnerID:   "stopped-writer",
		StartedAt: old,
		UpdatedAt: old,
	}
	if err := store.saveIndexTask(ctx, task); err != nil {
		t.Fatalf("save index task: %v", err)
	}

	loaded, err := store.GetIndexTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get index task: %v", err)
	}
	if loaded.Status != TaskStatusFailed ||
		loaded.Phase != TaskStatusFailed ||
		loaded.Error != inactiveTaskError ||
		loaded.FinishedAt.IsZero() {
		t.Fatalf("recovered index task = %#v", loaded)
	}
	if _, err := store.getIndexRebuildRunningMarker(
		ctx,
		task.TenantID,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get running marker err = %v, want ErrNotFound", err)
	}
}
