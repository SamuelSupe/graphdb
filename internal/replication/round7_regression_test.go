package replication

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRound7ConcurrentScheduleSnapshotCleanupMonotonicAndNoDeadlock(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		ctx:                 ctx,
		cfg:                 Config{Dir: dir},
		snapshotCleanupWake: make(chan struct{}, 1),
	}
	node.workers.Add(1)
	go node.snapshotCleanupLoop(nil)

	// Hammer scheduleSnapshotCleanup concurrently from multiple producers, then cancel and exit cleanly.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 1; i <= 200; i++ {
				node.scheduleSnapshotCleanup(uint64(i))
				node.scheduleSnapshotCleanup(uint64(201 - i))
			}
		}(g)
	}
	wg.Wait()

	if got := node.snapshotCleanupTarget.Load(); got != 200 {
		t.Fatalf("monotonic snapshotCleanupTarget = %d, want 200", got)
	}

	// Cancel context and call scheduleSnapshotCleanup after loop exits: must never block!
	cancel()
	done := make(chan struct{})
	go func() {
		node.workers.Wait()
		node.scheduleSnapshotCleanup(300)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("snapshotCleanupLoop / scheduleSnapshotCleanup deadlocked on shutdown")
	}
}

func TestRound7AvailableStateSplitsBacklogAndProgressDoesNotWakeChanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &Node{
		ctx:            ctx,
		cfg:            Config{ID: 1},
		stateChanged:   make(chan struct{}),
		appliedChanged: make(chan struct{}),
	}
	n.leader.Store(1)
	n.committed.Store(2000)
	n.applied.Store(100)

	// Admission check (checkBacklog=true) rejects backlog > 1024.
	if err := n.availableState(true, true); err == nil {
		t.Fatalf("expected backlog > 1024 to fail admission check")
	}
	// Post-submission wait loop (checkBacklog=false) must NOT fail committed proposals with ErrOutcomeUnknown.
	if err := n.availableState(true, false); err != nil {
		t.Fatalf("expected post-submission availableState(true, false) to succeed, got %v", err)
	}

	// progress() signals appliedChanged without closing n.stateChanged.
	changedBefore := n.stateChanged
	appliedBefore := n.appliedChanged
	n.progress(101)
	select {
	case <-appliedBefore:
	default:
		t.Fatalf("expected progress() to close appliedChanged")
	}
	select {
	case <-changedBefore:
		t.Fatalf("progress() must not broadcast on n.changed")
	default:
	}

	// endProposal() without handoff must not broadcast on n.changed.
	n.activeProposals = 1
	n.endProposal()
	select {
	case <-changedBefore:
		t.Fatalf("endProposal() without handoff must not broadcast on n.changed")
	default:
	}

	// endProposal() with handoff and activeProposals reaching 0 MUST broadcast on n.changed.
	n.mu.Lock()
	n.handoff = true
	n.activeProposals = 1
	n.mu.Unlock()
	n.endProposal()
	select {
	case <-changedBefore:
	default:
		t.Fatalf("expected endProposal() with handoff & 0 active proposals to close n.changed")
	}
}
