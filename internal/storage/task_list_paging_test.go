package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestListTasksUsesPagedBoundedNewestSelection(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	paged := &pagingOnlyStore{ObjectStore: base}
	store := NewTenantStore(paged, "test")
	startedAt := time.Date(2026, 7, 24, 2, 0, 0, 0, time.UTC)

	for i := 0; i < 10; i++ {
		status := TaskStatusSucceeded
		if i >= 8 {
			status = TaskStatusFailed
		}
		at := startedAt.Add(time.Duration(i) * time.Minute)
		if i%2 == 0 {
			id := fmt.Sprintf("stored-%02d", i)
			task := Task{
				ID:        id,
				TenantID:  "tenant-a",
				Type:      TaskTypeCompact,
				Status:    status,
				StartedAt: at,
				UpdatedAt: at,
			}
			data, err := marshalParquetTask(ctx, task)
			if err != nil {
				t.Fatalf("marshal task %q: %v", id, err)
			}
			if err := base.Put(
				ctx, store.taskKey("tenant-a", id), data,
			); err != nil {
				t.Fatalf("put task %q: %v", id, err)
			}
			continue
		}
		id := fmt.Sprintf("index-%02d", i)
		task := IndexTask{
			ID:        id,
			TenantID:  "tenant-a",
			Type:      "rebuild",
			Status:    status,
			StartedAt: at,
			UpdatedAt: at,
		}
		data, err := marshalParquetIndexTask(ctx, task)
		if err != nil {
			t.Fatalf("marshal index task %q: %v", id, err)
		}
		if err := base.Put(
			ctx, store.indexTaskKey("tenant-a", id), data,
		); err != nil {
			t.Fatalf("put index task %q: %v", id, err)
		}
	}

	tasks, err := store.ListTasks(ctx, "tenant-a", TaskListOptions{
		Status: TaskStatusSucceeded,
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	got := make([]string, len(tasks))
	for i := range tasks {
		got[i] = tasks[i].ID
	}
	want := []string{"index-07", "stored-06", "index-05"}
	if len(got) != len(want) {
		t.Fatalf("task ids = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("task ids = %v, want %v", got, want)
		}
	}
	if paged.listCalls != 0 || paged.pageCalls != 2 {
		t.Fatalf(
			"list calls=%d page calls=%d, want 0 and 2",
			paged.listCalls, paged.pageCalls,
		)
	}
}

type countedTaskFiles struct{ *countingObjectStore }

func (s countedTaskFiles) UnwrapObjectStore() ObjectStore { return s.ObjectStore }

func TestLocalTaskListReadsOnlySelectedTerminalPayloads(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	counter := newCountingObjectStore(files)
	store := NewTenantStore(countedTaskFiles{counter}, "test")
	ctx := t.Context()
	const tenant = "tenant-a"
	start := time.Date(2026, 7, 24, 2, 0, 0, 0, time.UTC)
	put := func(id string, at time.Time, status string) {
		t.Helper()
		task := Task{ID: id, TenantID: tenant, Type: TaskTypeGC, Status: status, StartedAt: at, Result: map[string]any{"kind": id}}
		data, err := marshalParquetTask(ctx, task)
		if err != nil {
			t.Fatal(err)
		}
		if err := files.Put(ctx, store.taskKey(tenant, id), data); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 1000 {
		put(fmt.Sprintf("finished-%04d", i), start.Add(time.Duration(i)*time.Second), TaskStatusSucceeded)
	}
	limit := TaskListOptions{Limit: 10}
	read := func(options TaskListOptions, want string) []Task {
		t.Helper()
		tasks, err := store.ListTasks(ctx, tenant, options)
		if err != nil || len(tasks) == 0 || tasks[0].ID != want {
			t.Fatalf("first task %q expected, got %v (%v)", want, tasks, err)
		}
		return tasks
	}
	read(limit, "finished-0999")
	cold := counter.getWithMetaCalls
	counter.reset()
	warm := read(limit, "finished-0999")
	if len(warm) != 10 || warm[0].Result["kind"] != warm[0].ID || cold != 1000 || counter.getWithMetaCalls != 10 {
		t.Fatalf("full result or read budget changed: cold=%d warm=%d tasks=%v", cold, counter.getWithMetaCalls, warm)
	}
	t.Logf("task objects fetched for limit=10: cold=%d warm=%d", cold, counter.getWithMetaCalls)

	put("finished-0000", start.Add(time.Hour), TaskStatusFailed)
	read(TaskListOptions{Status: TaskStatusFailed, Limit: 1}, "finished-0000")
	if err := files.Delete(ctx, store.taskKey(tenant, "finished-0000")); err != nil {
		t.Fatal(err)
	}
	read(limit, "finished-0999")

	legacy := IndexTask{ID: "legacy", TenantID: tenant, Type: "rebuild", Status: TaskStatusSucceeded, StartedAt: start.Add(time.Hour)}
	data, err := marshalParquetIndexTask(ctx, legacy)
	if err != nil {
		t.Fatal(err)
	}
	key := store.indexTaskKey(tenant, legacy.ID)
	if err := files.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	filter := TaskListOptions{Type: TaskTypeIndexRebuild, Limit: 1}
	read(filter, "legacy")
	read(filter, "legacy")
	if err := files.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if tasks, err := store.ListTasks(ctx, tenant, filter); err != nil || len(tasks) != 0 {
		t.Fatalf("deleted legacy task is still listed: %v (%v)", tasks, err)
	}
}

func TestQueuedMaintenancePollTracksChangesWithoutReadingHistory(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	counter := newCountingObjectStore(files)
	store := NewTenantStore(countedTaskFiles{counter}, "test")
	store.ReplicationMode = true
	const tenant = "tenant-a"
	for i := range 256 {
		task := Task{ID: fmt.Sprintf("finished-%03d", i), TenantID: tenant, Type: TaskTypeGC, Status: TaskStatusSucceeded}
		if err := store.saveTask(t.Context(), task); err != nil {
			t.Fatal(err)
		}
	}
	poll := func(want int) {
		t.Helper()
		tasks, err := store.QueuedMaintenanceTasks(t.Context(), tenant)
		if err != nil || len(tasks) != want {
			t.Fatalf("queued tasks %v, err %v", tasks, err)
		}
	}
	poll(0)
	counter.reset()
	poll(0)
	if counter.getWithMetaCalls != 0 || counter.listCalls != 0 {
		t.Fatalf("idle poll: %d reads and %d lists", counter.getWithMetaCalls, counter.listCalls)
	}
	task := Task{ID: "new", TenantID: tenant, Type: TaskTypeGC, Status: TaskStatusQueued}
	if err := store.saveTask(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	poll(1)
	task.Status = TaskStatusSucceeded
	if err := store.saveTask(t.Context(), task); err != nil {
		t.Fatal(err)
	}
	poll(0)
	legacy := IndexTask{ID: "legacy", TenantID: tenant, Type: "rebuild", Status: TaskStatusQueued}
	data, err := marshalParquetIndexTask(t.Context(), legacy)
	if err != nil {
		t.Fatal(err)
	}
	key := store.indexTaskKey(tenant, legacy.ID)
	if err := files.Put(t.Context(), key, data); err != nil {
		t.Fatal(err)
	}
	poll(1)
	if err := files.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	poll(0)
}
