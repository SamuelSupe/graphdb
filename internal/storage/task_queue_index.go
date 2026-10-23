package storage

import (
	"context"
	"errors"
)

type queuedTaskCatalog struct {
	loaded bool
	tasks  map[string]Task
	dirty  map[string]struct{}
}

// An exclusively owned directory supplies mutation notifications, including
// rollback and restore. Only nonterminal tasks and changed IDs are retained;
// historical task payloads are decoded once, not at every proposal.
func (s *TenantStore) queuedTasksForStart(ctx context.Context, tenant, kind string) ([]Task, error) {
	if s.localFileStore() == nil || kind == TaskTypeIndexRebuild {
		return s.listTasks(ctx, tenant, TaskListOptions{Type: kind, Status: TaskStatusQueued, Limit: 1})
	}
	s.taskMu.Lock()
	catalog := s.queuedTaskCatalogs[tenant]
	if catalog == nil {
		if len(s.queuedTaskCatalogs) >= 64 {
			s.queuedTaskCatalogs = nil
		}
		if s.queuedTaskCatalogs == nil {
			s.queuedTaskCatalogs = map[string]*queuedTaskCatalog{}
		}
		catalog = &queuedTaskCatalog{tasks: map[string]Task{}, dirty: map[string]struct{}{}}
		s.queuedTaskCatalogs[tenant] = catalog
	}
	loaded := catalog.loaded
	s.taskMu.Unlock()
	if !loaded {
		tasks := map[string]Task{}
		collector := taskListCollector{options: TaskListOptions{Type: kind, Status: TaskStatusQueued, Limit: 1}}
		overflow := false
		if err := s.scanStoredTasks(ctx, tenant, func(task Task) {
			if task.Status == TaskStatusQueued {
				collector.add(queuedTaskHeader(task))
				if len(tasks) < 4096 {
					tasks[task.ID] = queuedTaskHeader(task)
				} else {
					overflow = true
				}
			}
		}); err != nil {
			return nil, err
		}
		if overflow {
			s.taskMu.Lock()
			delete(s.queuedTaskCatalogs, tenant)
			s.taskMu.Unlock()
			return s.loadQueuedTaskCandidate(ctx, tenant, collector.result())
		}
		s.taskMu.Lock()
		catalog.tasks = tasks
		catalog.loaded = true
		s.taskMu.Unlock()
	}
	s.taskMu.Lock()
	dirty := catalog.dirty
	catalog.dirty = map[string]struct{}{}
	s.taskMu.Unlock()
	for id := range dirty {
		task, err := s.getTaskObject(ctx, tenant, id)
		s.taskMu.Lock()
		if err != nil && !errors.Is(err, ErrNotFound) {
			delete(s.queuedTaskCatalogs, tenant)
			s.taskMu.Unlock()
			return nil, err
		}
		if err == nil && task.Status == TaskStatusQueued {
			catalog.tasks[id] = queuedTaskHeader(task)
		} else {
			delete(catalog.tasks, id)
		}
		s.taskMu.Unlock()
	}
	s.taskMu.Lock()
	collector := taskListCollector{options: TaskListOptions{Type: kind, Status: TaskStatusQueued, Limit: 1}}
	for _, task := range catalog.tasks {
		collector.add(task)
	}
	result := collector.result()
	// A pathological backlog must not become an unbounded resident index.
	if len(catalog.tasks)+len(catalog.dirty) > 4096 {
		delete(s.queuedTaskCatalogs, tenant)
	}
	s.taskMu.Unlock()
	return s.loadQueuedTaskCandidate(ctx, tenant, result)
}

// Do not retain request, checkpoint, result or error payloads in the index.
func queuedTaskHeader(task Task) Task {
	return Task{ID: task.ID, TenantID: task.TenantID, Type: task.Type, Status: task.Status, StartedAt: task.StartedAt}
}

func (s *TenantStore) loadQueuedTaskCandidate(ctx context.Context, tenant string, candidates []Task) ([]Task, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	task, err := s.GetTask(ctx, tenant, candidates[0].ID)
	if errors.Is(err, ErrNotFound) || (err == nil && (task.Status != TaskStatusQueued || task.Type != candidates[0].Type)) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []Task{task}, nil
}
func (s *TenantStore) taskObjectChanged(tenant, name string) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	catalog := s.queuedTaskCatalogs[tenant]
	if catalog == nil {
		return
	}
	id, ok := taskIDFromKey(name)
	if !ok {
		return
	}
	catalog.dirty[id] = struct{}{}
	if len(catalog.dirty) > 4096 {
		delete(s.queuedTaskCatalogs, tenant)
	}
}
