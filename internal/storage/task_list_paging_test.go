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
