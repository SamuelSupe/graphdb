package storage

import (
	"context"
	"sync/atomic"
)

// ProgressReporter defines explicit task progress reporting behavior so
// background or parent tasks can mute progress on one store instance without
// leaking implicit context.Value flags into isolated child stores (such as
// restore drills).
type ProgressReporter interface {
	UpdateProgress(ctx context.Context, task Task, phase string, completed int, total int, checkpoint map[string]any) error
	UpdateActionProgress(ctx context.Context, task Task, phase string, completed int, total int, action taskActionUpdate, extra map[string]any) error
	Muted() bool
}

// LiveTaskReporter persists task progress updates to its bound TenantStore.
type LiveTaskReporter struct {
	store *TenantStore
}

// NewLiveTaskReporter creates a live progress reporter explicitly bound to store.
func NewLiveTaskReporter(store *TenantStore) LiveTaskReporter {
	return LiveTaskReporter{store: store}
}

func (r LiveTaskReporter) Muted() bool { return false }

func (r LiveTaskReporter) UpdateProgress(ctx context.Context, task Task, phase string, completed int, total int, checkpoint map[string]any) error {
	if r.store == nil {
		return nil
	}
	return r.store.persistTaskProgress(ctx, task, phase, completed, total, checkpoint)
}

func (r LiveTaskReporter) UpdateActionProgress(ctx context.Context, task Task, phase string, completed int, total int, action taskActionUpdate, extra map[string]any) error {
	if r.store == nil {
		return nil
	}
	return r.store.persistTaskActionProgress(ctx, task, phase, completed, total, action, extra)
}

// MutedReporter explicitly suppresses task progress persistence for the bound
// ExecutionEnv without affecting any other TenantStore instance.
type MutedReporter struct{}

func (MutedReporter) Muted() bool { return true }

func (MutedReporter) UpdateProgress(context.Context, Task, string, int, int, map[string]any) error {
	return nil
}

func (MutedReporter) UpdateActionProgress(context.Context, Task, string, int, int, taskActionUpdate, map[string]any) error {
	return nil
}

// IndexBuildOpts carries explicit index build controls instead of scattered
// boolean context.Value flags.
type IndexBuildOpts struct {
	AdjacencyEnabled      bool
	OrderedCatchup        bool
	ExplicitOnlineRebuild bool
	ForceRangePages       bool
}

// ExecutionEnv is an explicit, store-scoped execution handle binding progress
// reporting, index build controls, and optional I/O byte metering to a single
// TenantStore instance. Environments bound to one store are NEVER inherited by
// a different TenantStore instance (e.g. an isolated restore-drill targetStore).
type ExecutionEnv struct {
	Store     *TenantStore
	Progress  ProgressReporter
	BuildOpts IndexBuildOpts
	IOMeter   *atomic.Int64
}

type executionEnvKey struct{}

// NewExecutionEnv creates an explicit ExecutionEnv bound to s.
func (s *TenantStore) NewExecutionEnv(progress ProgressReporter, buildOpts IndexBuildOpts) ExecutionEnv {
	if progress == nil {
		progress = NewLiveTaskReporter(s)
	}
	return ExecutionEnv{
		Store:     s,
		Progress:  progress,
		BuildOpts: buildOpts,
	}
}

// WithExecutionEnv attaches an ExecutionEnv explicitly bound to s.
func (s *TenantStore) WithExecutionEnv(ctx context.Context, env ExecutionEnv) context.Context {
	env.Store = s
	if env.Progress == nil {
		env.Progress = NewLiveTaskReporter(s)
	}
	return context.WithValue(ctx, executionEnvKey{}, env)
}

// executionEnv resolves the ExecutionEnv bound to s from ctx. If ctx carries an
// ExecutionEnv bound to a DIFFERENT *TenantStore instance, it is intentionally
// ignored so parent-store flags (like MutedReporter) cannot cross-contaminate
// child stores.
func (s *TenantStore) executionEnv(ctx context.Context) ExecutionEnv {
	if env, ok := ctx.Value(executionEnvKey{}).(ExecutionEnv); ok {
		if env.Store == s {
			if env.Progress == nil {
				env.Progress = NewLiveTaskReporter(s)
			}
			return env
		}
		// Explicitly bound to a different TenantStore instance: never inherit
		// parent store flags or legacy bare keys across store boundaries.
		return ExecutionEnv{
			Store:    s,
			Progress: NewLiveTaskReporter(s),
		}
	}
	env := ExecutionEnv{
		Store:    s,
		Progress: NewLiveTaskReporter(s),
	}
	// Compatibility bridge for legacy white-box callers setting bare keys
	// when no store-scoped ExecutionEnv is present.
	if muted, _ := ctx.Value(taskProgressMutedKey{}).(bool); muted {
		env.Progress = MutedReporter{}
	}
	if adj, _ := ctx.Value(adjacencyBuildKey{}).(bool); adj {
		env.BuildOpts.AdjacencyEnabled = true
	}
	if ord, _ := ctx.Value(orderedIndexUpdateKey{}).(bool); ord {
		env.BuildOpts.OrderedCatchup = true
	}
	if online, _ := ctx.Value(explicitOnlineRebuildKey{}).(bool); online {
		env.BuildOpts.ExplicitOnlineRebuild = true
	}
	return env
}

// withBuildOpts returns a context whose ExecutionEnv for s updates BuildOpts
// while preserving the existing reporter and meter for s.
func (s *TenantStore) withBuildOpts(ctx context.Context, update func(*IndexBuildOpts)) context.Context {
	env := s.executionEnv(ctx)
	update(&env.BuildOpts)
	return s.WithExecutionEnv(ctx, env)
}

// withProgressReporter returns a context whose ExecutionEnv for s uses the
// given ProgressReporter without mutating any other store's environment.
func (s *TenantStore) withProgressReporter(ctx context.Context, reporter ProgressReporter) context.Context {
	env := s.executionEnv(ctx)
	env.Progress = reporter
	return s.WithExecutionEnv(ctx, env)
}

// withIOMeter attaches a lightweight atomic byte counter to s's ExecutionEnv
// so background compaction can throttle I/O directly on s without allocating a
// throwaway pseudo-TenantStore.
func (s *TenantStore) withIOMeter(ctx context.Context) (context.Context, *atomic.Int64) {
	env := s.executionEnv(ctx)
	meter := &atomic.Int64{}
	env.IOMeter = meter
	return s.WithExecutionEnv(ctx, env), meter
}

func (s *TenantStore) recordMeteredIO(ctx context.Context, n int64) {
	if n <= 0 {
		return
	}
	if env, ok := ctx.Value(executionEnvKey{}).(ExecutionEnv); ok && env.Store == s && env.IOMeter != nil {
		env.IOMeter.Add(n)
	}
}
