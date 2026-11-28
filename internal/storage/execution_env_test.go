package storage

import (
	"context"
	"testing"
)

func TestExecutionEnvIsolatesProgressAndBuildOptsAcrossStores(t *testing.T) {
	parent := NewTenantStore(NewMemoryStore(), "parent")
	child := NewTenantStore(NewMemoryStore(), "child")

	// Mute progress and enable adjacency/ordered options on parent.
	parentCtx := parent.WithExecutionEnv(context.Background(), ExecutionEnv{
		Progress: MutedReporter{},
		BuildOpts: IndexBuildOpts{
			AdjacencyEnabled: true,
			OrderedCatchup:   true,
			ForceRangePages:  true,
		},
	})

	parentEnv := parent.executionEnv(parentCtx)
	if !parentEnv.Progress.Muted() {
		t.Fatalf("expected parent ExecutionEnv progress to be muted")
	}
	if !parentEnv.BuildOpts.AdjacencyEnabled || !parentEnv.BuildOpts.OrderedCatchup || !parentEnv.BuildOpts.ForceRangePages {
		t.Fatalf("expected parent ExecutionEnv BuildOpts to be preserved: %+v", parentEnv.BuildOpts)
	}

	// Passing parentCtx into an isolated child store MUST NOT inherit parent's
	// MutedReporter or BuildOpts.
	childEnv := child.executionEnv(parentCtx)
	if childEnv.Progress.Muted() {
		t.Fatalf("child store inherited MutedReporter from parent store!")
	}
	if childEnv.BuildOpts.AdjacencyEnabled || childEnv.BuildOpts.OrderedCatchup || childEnv.BuildOpts.ForceRangePages {
		t.Fatalf("child store inherited BuildOpts from parent store: %+v", childEnv.BuildOpts)
	}

	// Verify IO meter scoping as well.
	meteredCtx, meter := parent.withIOMeter(parentCtx)
	parent.recordMeteredIO(meteredCtx, 1024)
	child.recordMeteredIO(meteredCtx, 4096) // different store -> ignored
	if got := meter.Load(); got != 1024 {
		t.Fatalf("meter = %d, want 1024 (child store I/O must not bleed into parent meter)", got)
	}
}
