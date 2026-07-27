package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPostgresCoordinatorTaskQueueDeduplicatesAcrossWriters(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t,
		"task-queue-dedupe",
	)
	objects := &blockOncePutStore{
		ObjectStore: NewMemoryStore(),
		substring:   "/tasks/",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	observed := &observedTaskQueueCoordinator{
		PostgresCoordinator: coordinator,
		contended:           make(chan struct{}),
		taskType:            coordinatorQueuedTaskLeasePrefix + TaskTypeExportSnapshot,
	}
	first := NewTenantStore(objects, "test")
	second := NewTenantStore(objects, "test")
	first.InstanceID = "task-writer-a"
	second.InstanceID = "task-writer-b"
	first.SetCoordinator(coordinator)
	second.SetCoordinator(observed)
	if _, err := first.CreateTenant(
		ctx,
		"tenant-a",
		TenantCreateOptions{},
	); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	for range defaultTaskExecutionLimit {
		first.taskExecutionSlots <- struct{}{}
	}
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
	<-observed.contended
	close(objects.resume)
	started := <-firstDone
	if started.err != nil {
		t.Fatalf("start first task: %v", started.err)
	}
	reused := <-secondDone
	if reused.err != nil {
		t.Fatalf("start duplicate task: %v", reused.err)
	}
	task := started.task
	if reused.task.ID != task.ID {
		t.Fatalf(
			"duplicate task id = %q, want %q",
			reused.task.ID,
			task.ID,
		)
	}
	if _, err := second.CancelTask(
		context.Background(),
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
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, active, err := coordinator.TaskLease(
		ctx,
		task.TenantID,
		coordinatorQueuedTaskLeaseType(task),
	); err != nil {
		t.Fatalf("read released queue lease: %v", err)
	} else if active {
		t.Fatal("cross-writer cancellation left the PostgreSQL queue lease active")
	}
	for range defaultTaskExecutionLimit {
		<-first.taskExecutionSlots
	}
	waitForTaskStatus(
		t,
		first,
		task.TenantID,
		task.ID,
		TaskStatusCanceled,
	)
}

func TestPostgresCoordinatorIndexTaskQueueDeduplicatesAcrossWriters(
	t *testing.T,
) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t,
		"index-task-queue-dedupe",
	)
	objects := &blockOncePutStore{
		ObjectStore: NewMemoryStore(),
		substring:   "/indexes/running/rebuild.parquet",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	observed := &observedTaskQueueCoordinator{
		PostgresCoordinator: coordinator,
		contended:           make(chan struct{}),
		taskType:            coordinatorQueuedIndexTaskLeaseType(),
	}
	first := NewTenantStore(objects, "test")
	second := NewTenantStore(objects, "test")
	first.InstanceID = "index-writer-a"
	second.InstanceID = "index-writer-b"
	first.SetCoordinator(coordinator)
	second.SetCoordinator(observed)
	if _, err := first.CreateTenant(
		ctx,
		"tenant-a",
		TenantCreateOptions{},
	); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	for range defaultTaskExecutionLimit {
		first.taskExecutionSlots <- struct{}{}
		second.taskExecutionSlots <- struct{}{}
	}

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
	<-observed.contended
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
	for range defaultTaskExecutionLimit {
		<-first.taskExecutionSlots
		<-second.taskExecutionSlots
	}
	waitForIndexTaskStatus(
		t,
		ctx,
		first,
		started.task.TenantID,
		started.task.ID,
	)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, active, err := coordinator.TaskLease(
			ctx,
			started.task.TenantID,
			coordinatorQueuedIndexTaskLeaseType(),
		)
		if err != nil {
			t.Fatalf("read index queue lease: %v", err)
		}
		if !active {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("completed index task left the PostgreSQL queue lease active")
}

type observedTaskQueueCoordinator struct {
	*PostgresCoordinator

	contended chan struct{}
	taskType  string
	once      sync.Once
}

func (c *observedTaskQueueCoordinator) AcquireTaskLease(
	ctx context.Context,
	tenantID string,
	taskType string,
	ownerToken string,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	lease, acquired, err := c.PostgresCoordinator.AcquireTaskLease(
		ctx,
		tenantID,
		taskType,
		ownerToken,
		ttl,
	)
	if err == nil &&
		!acquired &&
		taskType == c.taskType {
		c.once.Do(func() { close(c.contended) })
	}
	return lease, acquired, err
}
