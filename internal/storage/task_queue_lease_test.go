package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type taskLeaseTestCoordinator struct {
	WriteCoordinator

	mu               sync.Mutex
	head             CoordinationHead
	leases           map[string]CoordinatorTaskLease
	blockAcquireType string
	acquireEntered   chan struct{}
	acquireRelease   chan struct{}
	enterOnce        sync.Once
	contended        chan struct{}
	contendOnce      sync.Once
}

func newTaskLeaseTestCoordinator() *taskLeaseTestCoordinator {
	return &taskLeaseTestCoordinator{
		head: CoordinationHead{
			TenantID:   "tenant-a",
			Generation: 1,
			Status:     TenantStatusActive,
			Revision:   1,
		},
		leases: map[string]CoordinatorTaskLease{},
	}
}

func (*taskLeaseTestCoordinator) Backend() string {
	return CoordinationPostgres
}

func (*taskLeaseTestCoordinator) Namespace() string {
	return "task-lease-test"
}

func (c *taskLeaseTestCoordinator) Head(
	context.Context,
	string,
) (CoordinationHead, bool, error) {
	return c.head, true, nil
}

func (c *taskLeaseTestCoordinator) TaskLease(
	_ context.Context,
	tenantID string,
	taskType string,
) (CoordinatorTaskLease, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.leases[tenantID+"\x00"+taskType]
	active := ok && lease.OwnerToken != "" &&
		lease.ExpiresAt.After(time.Now().UTC())
	return lease, active, nil
}

func (c *taskLeaseTestCoordinator) AcquireTaskLease(
	ctx context.Context,
	tenantID string,
	taskType string,
	owner string,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	if taskType == c.blockAcquireType {
		c.enterOnce.Do(func() { close(c.acquireEntered) })
		select {
		case <-ctx.Done():
			return CoordinatorTaskLease{}, false, ctx.Err()
		case <-c.acquireRelease:
			return CoordinatorTaskLease{}, false, errors.New("acquire released")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := tenantID + "\x00" + taskType
	current := c.leases[key]
	if current.OwnerToken != "" &&
		current.OwnerToken != owner &&
		current.ExpiresAt.After(time.Now().UTC()) {
		if c.contended != nil {
			c.contendOnce.Do(func() { close(c.contended) })
		}
		return CoordinatorTaskLease{}, false, nil
	}
	epoch := current.FenceEpoch
	if epoch == 0 {
		epoch = 1
	} else if current.OwnerToken != owner {
		epoch++
	}
	lease := CoordinatorTaskLease{
		TenantID:   tenantID,
		TaskType:   taskType,
		OwnerToken: owner,
		FenceEpoch: epoch,
		ExpiresAt:  time.Now().UTC().Add(ttl),
	}
	c.leases[key] = lease
	return lease, true, nil
}

func (c *taskLeaseTestCoordinator) RenewTaskLease(
	_ context.Context,
	lease CoordinatorTaskLease,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := lease.TenantID + "\x00" + lease.TaskType
	current := c.leases[key]
	if current.OwnerToken != lease.OwnerToken ||
		current.FenceEpoch != lease.FenceEpoch {
		return CoordinatorTaskLease{}, false, nil
	}
	current.ExpiresAt = time.Now().UTC().Add(ttl)
	c.leases[key] = current
	return current, true, nil
}

func (c *taskLeaseTestCoordinator) ReleaseTaskLease(
	_ context.Context,
	lease CoordinatorTaskLease,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := lease.TenantID + "\x00" + lease.TaskType
	current := c.leases[key]
	if current.OwnerToken != lease.OwnerToken ||
		current.FenceEpoch != lease.FenceEpoch {
		return ErrConflict
	}
	current.OwnerToken = ""
	current.ExpiresAt = time.Now().UTC().Add(-time.Second)
	c.leases[key] = current
	return nil
}

func TestPostgresTaskQueueLeaseDeduplicatesAcrossWriters(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	coordinator := newTaskLeaseTestCoordinator()
	first := NewTenantStore(objects, "test")
	second := NewTenantStore(objects, "test")
	first.SetCoordinator(coordinator)
	second.SetCoordinator(coordinator)

	for range defaultTaskExecutionLimit {
		first.taskExecutionSlots <- struct{}{}
	}
	task, err := first.StartTask(
		ctx,
		"tenant-a",
		TaskTypeExportSnapshot,
		nil,
	)
	if err != nil {
		t.Fatalf("start first task: %v", err)
	}
	reused, err := second.StartTask(
		ctx,
		"tenant-a",
		TaskTypeExportSnapshot,
		nil,
	)
	if err != nil {
		t.Fatalf("start duplicate task: %v", err)
	}
	if reused.ID != task.ID {
		t.Fatalf("duplicate task id = %q, want %q", reused.ID, task.ID)
	}
	if _, err := first.CancelTask(ctx, task.TenantID, task.ID); err != nil {
		t.Fatalf("cancel queued task: %v", err)
	}
	for range defaultTaskExecutionLimit {
		<-first.taskExecutionSlots
	}
	waitForTaskStatus(t, first, task.TenantID, task.ID, TaskStatusCanceled)
}

func TestPostgresTaskQueueLeaseWaitsForTaskPublication(t *testing.T) {
	ctx := context.Background()
	objects := &blockOncePutStore{
		ObjectStore: NewMemoryStore(),
		substring:   "/tasks/",
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
	}
	slotsBlocked := true
	defer func() {
		if !slotsBlocked {
			return
		}
		for range defaultTaskExecutionLimit {
			<-first.taskExecutionSlots
		}
	}()

	type startResult struct {
		task Task
		err  error
	}
	firstDone := make(chan startResult, 1)
	go func() {
		task, err := first.StartTask(
			ctx,
			"tenant-a",
			TaskTypeExportSnapshot,
			nil,
		)
		firstDone <- startResult{task: task, err: err}
	}()
	<-objects.paused

	secondDone := make(chan startResult, 1)
	go func() {
		task, err := second.StartTask(
			ctx,
			"tenant-a",
			TaskTypeExportSnapshot,
			nil,
		)
		secondDone <- startResult{task: task, err: err}
	}()
	<-coordinator.contended
	close(objects.resume)

	started := <-firstDone
	if started.err != nil {
		t.Fatalf("start first task: %v", started.err)
	}
	reused := <-secondDone
	if reused.err != nil {
		t.Fatalf("start duplicate during publication: %v", reused.err)
	}
	if reused.task.ID != started.task.ID {
		t.Fatalf(
			"duplicate task id = %q, want %q",
			reused.task.ID,
			started.task.ID,
		)
	}
	if _, err := first.CancelTask(
		ctx,
		started.task.TenantID,
		started.task.ID,
	); err != nil {
		t.Fatalf("cancel queued task: %v", err)
	}
	for range defaultTaskExecutionLimit {
		<-first.taskExecutionSlots
	}
	slotsBlocked = false
	waitForTaskStatus(
		t,
		first,
		started.task.TenantID,
		started.task.ID,
		TaskStatusCanceled,
	)
}

func TestPostgresQueuedTaskCancellationFromAnotherWriterReleasesLease(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	coordinator := newTaskLeaseTestCoordinator()
	owner := NewTenantStore(objects, "test")
	canceller := NewTenantStore(objects, "test")
	owner.InstanceID = "task-writer-a"
	canceller.InstanceID = "task-writer-b"
	owner.SetCoordinator(coordinator)
	canceller.SetCoordinator(coordinator)

	for range defaultTaskExecutionLimit {
		owner.taskExecutionSlots <- struct{}{}
	}
	defer func() {
		for range defaultTaskExecutionLimit {
			<-owner.taskExecutionSlots
		}
	}()
	task, err := owner.StartTask(
		ctx,
		"tenant-a",
		TaskTypeExportSnapshot,
		nil,
	)
	if err != nil {
		t.Fatalf("start queued task: %v", err)
	}
	if _, err := canceller.CancelTask(
		ctx,
		task.TenantID,
		task.ID,
	); err != nil {
		t.Fatalf("cancel queued task from another writer: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, active, err := coordinator.TaskLease(
			ctx,
			task.TenantID,
			coordinatorQueuedTaskLeaseType(task),
		)
		if err != nil {
			t.Fatalf("read queue lease: %v", err)
		}
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cross-writer cancellation left the queued task lease active")
}

func TestTaskQueueLeaseAcquisitionHonorsStartContext(t *testing.T) {
	coordinator := newTaskLeaseTestCoordinator()
	coordinator.blockAcquireType = coordinatorQueuedTaskLeasePrefix +
		TaskTypeExportSnapshot
	coordinator.acquireEntered = make(chan struct{})
	coordinator.acquireRelease = make(chan struct{})
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.StartTask(
			ctx,
			"tenant-a",
			TaskTypeExportSnapshot,
			nil,
		)
		done <- err
	}()
	<-coordinator.acquireEntered
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("start task err = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(coordinator.acquireRelease)
		<-done
		t.Fatal("task queue lease acquisition ignored the start context")
	}
}

func TestStartTaskLockAcquisitionHonorsContext(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	unlock := store.lockTenant("tenant-a")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.StartTask(
			ctx,
			"tenant-a",
			TaskTypeExportSnapshot,
			nil,
		)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		unlock()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("start task err = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		unlock()
		<-done
		t.Fatal("task start ignored cancellation while waiting for tenant lock")
	}
}

func TestTaskLeaseAcquisitionStopsWhenTaskIsCanceled(t *testing.T) {
	ctx := context.Background()
	coordinator := newTaskLeaseTestCoordinator()
	coordinator.blockAcquireType = coordinatorLifecycleTaskType
	coordinator.acquireEntered = make(chan struct{})
	coordinator.acquireRelease = make(chan struct{})
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	now := time.Now().UTC()
	task := Task{
		ID:        "cancel-acquire",
		TenantID:  "tenant-a",
		Type:      TaskTypeCompact,
		Status:    TaskStatusQueued,
		Phase:     TaskStatusQueued,
		OwnerID:   store.InstanceID,
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		store.runTask(runCtx, cancel, task)
		close(done)
	}()
	<-coordinator.acquireEntered
	cancel()

	returned := false
	select {
	case <-done:
		returned = true
	case <-time.After(250 * time.Millisecond):
	}
	close(coordinator.acquireRelease)
	if !returned {
		<-done
		t.Fatal("task lease acquisition ignored task cancellation")
	}
}

func waitForTaskStatus(
	t *testing.T,
	store *TenantStore,
	tenantID string,
	taskID string,
	status string,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := store.GetTask(context.Background(), tenantID, taskID)
		if err == nil && task.Status == status {
			return
		}
		time.Sleep(time.Millisecond)
	}
	task, err := store.GetTask(context.Background(), tenantID, taskID)
	t.Fatalf("task status = %q err=%v, want %q", task.Status, err, status)
}
