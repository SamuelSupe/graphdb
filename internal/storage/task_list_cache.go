package storage

import (
	"context"
	"errors"
)

const maxTerminalTaskHeaders = 16384

// Terminal headers only carry the fields needed for filtering and ordering.
// Active records still pass through owner reconciliation on every list.
func (s *TenantStore) listLocalTasks(ctx context.Context, tenant string, options TaskListOptions) ([]Task, error) {
	collector := taskListCollector{options: options}
	visit := func(key taskListKey) error {
		s.taskMu.Lock()
		header, cached := s.terminalTaskHeaders[key]
		epoch := s.taskHeaderEpoch
		s.taskMu.Unlock()
		if cached {
			collector.addCandidate(taskListCandidate{task: header, loadSource: key})
			return nil
		}
		task, err := s.readListTask(ctx, key)
		if err != nil {
			return err
		}
		if taskTerminal(task.Status) {
			s.cacheTerminalTaskHeader(key, task, epoch)
		}
		collector.add(task)
		return nil
	}
	if err := s.scanTaskIDs(ctx, tenant, false, visit); err != nil {
		return nil, err
	}
	if err := s.scanTaskIDs(ctx, tenant, true, visit); err != nil {
		return nil, err
	}
	var tasks []Task
	for _, candidate := range collector.selections() {
		task := candidate.task
		if key := candidate.loadSource; key.id != "" {
			current, err := s.readListTask(ctx, key)
			if errors.Is(err, ErrNotFound) || (key.legacy && errors.Is(err, errInvalidIndexTask)) {
				return s.listTasksFull(ctx, tenant, options)
			}
			if err != nil {
				return nil, err
			}
			if current.Type != task.Type || current.Status != task.Status || !current.StartedAt.Equal(task.StartedAt) {
				return s.listTasksFull(ctx, tenant, options)
			}
			task = current
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (s *TenantStore) cacheTerminalTaskHeader(key taskListKey, task Task, epoch uint64) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if epoch != s.taskHeaderEpoch {
		return
	}
	if s.terminalTaskHeaders == nil {
		s.terminalTaskHeaders = make(map[taskListKey]Task)
	}
	if _, exists := s.terminalTaskHeaders[key]; !exists && len(s.terminalTaskHeaders) >= maxTerminalTaskHeaders {
		for old := range s.terminalTaskHeaders {
			delete(s.terminalTaskHeaders, old)
			break
		}
	}
	s.terminalTaskHeaders[key] = taskListHeader(task)
}

func (s *TenantStore) invalidateTenantTaskHeaders(tenant string) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	s.taskHeaderEpoch++
	for key := range s.terminalTaskHeaders {
		if key.tenant == tenant {
			delete(s.terminalTaskHeaders, key)
		}
	}
}
