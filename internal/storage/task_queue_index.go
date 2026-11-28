package storage

import (
	"context"
	"errors"
	"sync"
)

type queuedTaskCatalog struct {
	refresh sync.Mutex
	loaded  bool
	tasks   map[string]Task
	dirty   map[string]struct{}
}

// An exclusively owned directory supplies mutation notifications, including
// rollback and restore. Only nonterminal tasks and changed IDs are retained;
// historical task payloads are decoded once, not at every proposal.
func (s *TenantStore) queuedTasks(ctx context.Context, tenant string, options TaskListOptions) ([]Task, error) {
	if s.localFileStore() == nil || s.unsubscribe == nil {
		return s.listTasks(ctx, tenant, options)
	}
	s.taskMu.Lock()
	catalog := s.queuedTaskCatalogs[tenant]
	if catalog == nil {
		if s.queuedTaskCatalogs == nil {
			s.queuedTaskCatalogs = make(map[string]*queuedTaskCatalog)
		}
		catalog = &queuedTaskCatalog{}
		s.queuedTaskCatalogs[tenant] = catalog
	}
	s.taskMu.Unlock()
	catalog.refresh.Lock()
	defer catalog.refresh.Unlock()
	// Subscribe before scanning, so mutations that race initial discovery are replayed.
	if !catalog.loaded {
		catalog.tasks = make(map[string]Task)
		overflow := false
		visit := func(task Task) {
			if task.Status == TaskStatusQueued {
				if len(catalog.tasks) >= 4096 {
					overflow = true
					return
				}
				catalog.tasks[task.ID] = taskListHeader(task)
			}
		}
		if err := s.scanStoredTasks(ctx, tenant, visit); err != nil {
			return nil, err
		}
		if err := s.scanIndexTasksAsTasks(ctx, tenant, visit); err != nil {
			return nil, err
		}
		if overflow {
			s.discardQueuedCatalog(tenant, catalog)
			return s.listTasks(ctx, tenant, options)
		}
		catalog.loaded = true
	}
	s.taskMu.Lock()
	dirty := catalog.dirty
	catalog.dirty = nil
	s.taskMu.Unlock()
	for id := range dirty {
		task, err := s.GetTask(ctx, tenant, id)
		if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, errInvalidIndexTask) {
			s.discardQueuedCatalog(tenant, catalog)
			return nil, err
		}
		if err == nil && task.Status == TaskStatusQueued {
			catalog.tasks[id] = taskListHeader(task)
		} else {
			delete(catalog.tasks, id)
		}
	}
	if len(catalog.tasks) > 4096 {
		s.discardQueuedCatalog(tenant, catalog)
	}
	candidateOptions := options
	if options.Limit == 1 {
		candidateOptions.Limit = 4
	}
	collector := taskListCollector{options: candidateOptions}
	for _, header := range catalog.tasks {
		collector.add(header)
	}
	var tasks []Task
	for _, header := range collector.result() {
		task, err := s.GetTask(ctx, tenant, header.ID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if task.Status == TaskStatusQueued && task.Type == header.Type {
			tasks = append(tasks, task)
			if options.Limit > 0 && len(tasks) == options.Limit {
				break
			}
		}
	}
	return tasks, nil
}

func (s *TenantStore) discardQueuedCatalog(tenant string, catalog *queuedTaskCatalog) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if s.queuedTaskCatalogs[tenant] == catalog {
		delete(s.queuedTaskCatalogs, tenant)
	}
}

// Do not retain request, checkpoint, result or error payloads in the index.
func taskListHeader(task Task) Task {
	return Task{ID: task.ID, TenantID: task.TenantID, Type: task.Type, Status: task.Status, StartedAt: task.StartedAt}
}

func (s *TenantStore) taskObjectChanged(tenant, name string) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	id, ok := taskIDFromKey(name)
	if !ok {
		return
	}
	s.taskHeaderEpoch++
	// Legacy index lookup can resolve to the unified task with the same ID.
	delete(s.terminalTaskHeaders, taskListKey{tenant: tenant, id: id})
	delete(s.terminalTaskHeaders, taskListKey{tenant: tenant, id: id, legacy: true})
	catalog := s.queuedTaskCatalogs[tenant]
	if catalog == nil {
		return
	}
	if catalog.dirty == nil {
		catalog.dirty = make(map[string]struct{})
	}
	catalog.dirty[id] = struct{}{}
	if len(catalog.dirty) > 4096 {
		delete(s.queuedTaskCatalogs, tenant)
	}
}
