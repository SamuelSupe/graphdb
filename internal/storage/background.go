package storage

import "context"

// Staged restore and restore drills execute synchronously, but their persisted
// tasks still need a live owner while progress and recovery inspect them.
func (s *TenantStore) inlineTask(ctx context.Context, task Task) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	s.registerTaskCancel(task.TenantID, task.ID, cancel)
	return ctx, func() { s.unregisterTaskCancel(task.TenantID, task.ID); cancel() }
}

// StartBackground registers a store-owned loop before it can race with shutdown.
// The caller's cancellation and store shutdown both stop the loop.
func (s *TenantStore) StartBackground(ctx context.Context, run func(context.Context)) bool {
	s.taskMu.Lock()
	if s.taskClosing {
		s.taskMu.Unlock()
		return false
	}
	s.taskWorkers.Add(1)
	s.taskMu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.backgroundCtx, cancel)
	go func() {
		defer s.taskWorkers.Done()
		defer cancel()
		defer stop()
		run(ctx)
	}()
	return true
}
