package storage

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func (s *TenantStore) listTasks(
	ctx context.Context,
	tenantID string,
	options TaskListOptions,
) ([]Task, error) {
	collector := taskListCollector{options: options}
	if err := s.scanStoredTasks(ctx, tenantID, collector.add); err != nil {
		return nil, err
	}
	if err := s.scanIndexTasksAsTasks(ctx, tenantID, collector.add); err != nil {
		return nil, err
	}
	return collector.result(), nil
}

func (s *TenantStore) scanStoredTasks(
	ctx context.Context,
	tenantID string,
	visit func(Task),
) error {
	prefix := s.taskPrefix(tenantID)
	return scanObjectPrefix(
		ctx,
		s.Objects,
		prefix,
		func(objects []ObjectInfo) error {
			for _, object := range objects {
				rest := strings.TrimPrefix(object.Key, prefix)
				if strings.Contains(rest, "/") {
					continue
				}
				taskID, ok := taskIDFromKey(object.Key)
				if !ok {
					continue
				}
				task, err := s.getTaskObject(ctx, tenantID, taskID)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				if task.TenantID != tenantID {
					return fmt.Errorf(
						"task tenant mismatch: path tenant %q contains tenant %q",
						tenantID, task.TenantID,
					)
				}
				if task.ID != taskID {
					return fmt.Errorf(
						"task id mismatch: path task %q contains task %q",
						taskID, task.ID,
					)
				}
				visit(s.reconcileInactiveTask(ctx, task))
			}
			return nil
		},
	)
}

func (s *TenantStore) scanIndexTasksAsTasks(
	ctx context.Context,
	tenantID string,
	visit func(Task),
) error {
	return scanObjectPrefix(
		ctx,
		s.Objects,
		s.indexTaskPrefix(tenantID),
		func(objects []ObjectInfo) error {
			for _, object := range objects {
				taskID, ok := indexTaskIDFromKey(object.Key)
				if !ok {
					continue
				}
				task, err := s.GetIndexTask(ctx, tenantID, taskID)
				if errors.Is(err, ErrNotFound) ||
					errors.Is(err, errInvalidIndexTask) {
					continue
				}
				if err != nil {
					return err
				}
				visit(taskFromIndexTask(task))
			}
			return nil
		},
	)
}

type taskListCollector struct {
	options TaskListOptions
	tasks   taskNewestHeap
}

func (c *taskListCollector) add(task Task) {
	if c.options.Type != "" && task.Type != c.options.Type {
		return
	}
	if c.options.Status != "" && task.Status != c.options.Status {
		return
	}
	if c.options.Limit <= 0 {
		c.tasks = append(c.tasks, task)
		return
	}
	if c.tasks.Len() < c.options.Limit {
		heap.Push(&c.tasks, task)
		return
	}
	if taskBefore(task, c.tasks[0]) {
		heap.Pop(&c.tasks)
		heap.Push(&c.tasks, task)
	}
}

func (c *taskListCollector) result() []Task {
	tasks := append([]Task(nil), c.tasks...)
	sort.Slice(tasks, func(i int, j int) bool {
		return taskBefore(tasks[i], tasks[j])
	})
	return tasks
}

func taskBefore(left Task, right Task) bool {
	if !left.StartedAt.Equal(right.StartedAt) {
		return left.StartedAt.After(right.StartedAt)
	}
	return left.ID > right.ID
}

// taskNewestHeap keeps the oldest selected task at the root.
type taskNewestHeap []Task

func (h taskNewestHeap) Len() int {
	return len(h)
}

func (h taskNewestHeap) Less(i int, j int) bool {
	return taskBefore(h[j], h[i])
}

func (h taskNewestHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *taskNewestHeap) Push(value any) {
	*h = append(*h, value.(Task))
}

func (h *taskNewestHeap) Pop() any {
	tasks := *h
	last := len(tasks) - 1
	task := tasks[last]
	*h = tasks[:last]
	return task
}
