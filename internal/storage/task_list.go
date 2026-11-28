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
	if options.Limit > 0 && s.localFileStore() != nil && s.unsubscribe != nil {
		return s.listLocalTasks(ctx, tenantID, options)
	}
	return s.listTasksFull(ctx, tenantID, options)
}

func (s *TenantStore) listTasksFull(ctx context.Context, tenantID string, options TaskListOptions) ([]Task, error) {
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
	return s.scanTasks(ctx, tenantID, false, visit)
}

func (s *TenantStore) scanIndexTasksAsTasks(
	ctx context.Context,
	tenantID string,
	visit func(Task),
) error {
	return s.scanTasks(ctx, tenantID, true, visit)
}

func (s *TenantStore) scanTasks(ctx context.Context, tenant string, legacy bool, visit func(Task)) error {
	return s.scanTaskIDs(ctx, tenant, legacy, func(key taskListKey) error {
		task, err := s.readListTask(ctx, key)
		if err == nil {
			visit(task)
		}
		return err
	})
}

type taskListKey struct {
	tenant, id string
	legacy     bool
}

func (s *TenantStore) scanTaskIDs(ctx context.Context, tenant string, legacy bool, visit func(taskListKey) error) error {
	prefix, parse := s.taskPrefix(tenant), taskIDFromKey
	if legacy {
		prefix, parse = s.indexTaskPrefix(tenant), indexTaskIDFromKey
	}
	return scanObjectPrefix(ctx, s.Objects, prefix, func(objects []ObjectInfo) error {
		for _, object := range objects {
			if !legacy && strings.Contains(strings.TrimPrefix(object.Key, prefix), "/") {
				continue
			}
			id, ok := parse(object.Key)
			if !ok {
				continue
			}
			err := visit(taskListKey{tenant: tenant, id: id, legacy: legacy})
			if err != nil && !errors.Is(err, ErrNotFound) && !(legacy && errors.Is(err, errInvalidIndexTask)) {
				return err
			}
		}
		return nil
	})
}

func (s *TenantStore) readListTask(ctx context.Context, key taskListKey) (Task, error) {
	if key.legacy {
		task, err := s.GetIndexTask(ctx, key.tenant, key.id)
		return taskFromIndexTask(task), err
	}
	task, err := s.getTaskObject(ctx, key.tenant, key.id)
	if err != nil {
		return Task{}, err
	}
	if task.TenantID != key.tenant {
		return Task{}, fmt.Errorf("task tenant mismatch: path tenant %q contains tenant %q", key.tenant, task.TenantID)
	}
	if task.ID != key.id {
		return Task{}, fmt.Errorf("task id mismatch: path task %q contains task %q", key.id, task.ID)
	}
	return s.reconcileInactiveTask(ctx, task), nil
}

type taskListCollector struct {
	options TaskListOptions
	tasks   taskNewestHeap
}

func (c *taskListCollector) add(task Task) {
	c.addCandidate(taskListCandidate{task: task})
}

type taskListCandidate struct {
	task       Task
	loadSource taskListKey
}

func (c *taskListCollector) addCandidate(candidate taskListCandidate) {
	task := candidate.task
	if c.options.Type != "" && task.Type != c.options.Type {
		return
	}
	if c.options.Status != "" && task.Status != c.options.Status {
		return
	}
	if c.options.Limit <= 0 {
		c.tasks = append(c.tasks, candidate)
		return
	}
	if c.tasks.Len() < c.options.Limit {
		heap.Push(&c.tasks, candidate)
		return
	}
	if taskBefore(task, c.tasks[0].task) {
		heap.Pop(&c.tasks)
		heap.Push(&c.tasks, candidate)
	}
}

func (c *taskListCollector) result() []Task {
	var tasks []Task
	for _, candidate := range c.selections() {
		tasks = append(tasks, candidate.task)
	}
	return tasks
}

func (c *taskListCollector) selections() []taskListCandidate {
	tasks := append([]taskListCandidate(nil), c.tasks...)
	sort.Slice(tasks, func(i int, j int) bool {
		return taskBefore(tasks[i].task, tasks[j].task)
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
type taskNewestHeap []taskListCandidate

func (h taskNewestHeap) Len() int {
	return len(h)
}

func (h taskNewestHeap) Less(i int, j int) bool {
	return taskBefore(h[j].task, h[i].task)
}

func (h taskNewestHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *taskNewestHeap) Push(value any) {
	*h = append(*h, value.(taskListCandidate))
}

func (h *taskNewestHeap) Pop() any {
	tasks := *h
	last := len(tasks) - 1
	task := tasks[last]
	tasks[last] = taskListCandidate{}
	*h = tasks[:last]
	return task
}
