package replication

import (
	"context"
	"slices"
	"time"
)

const snapshotHandoffDelay = 30 * time.Second

func (n *Node) recordSnapshotFailure(err error, index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotFailure = err
	if err == nil {
		n.snapshotFailureIndex = 0
		n.snapshotFailedSince = time.Time{}
		n.snapshotHandoffAttempt = time.Time{}
	} else if n.snapshotFailedSince.IsZero() {
		n.snapshotFailedSince = time.Now()
		n.snapshotFailureIndex = index
	}
}

// Keep snapshot recovery and peer HTTP outside both Raft's tick loop and the
// application loop. A shared capacity failure must not rotate the leadership.
func (n *Node) snapshotHandoffLoop() {
	defer n.workers.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case now := <-ticker.C:
			n.handoffSnapshotLeadership(now)
		}
	}
}

func (n *Node) handoffSnapshotLeadership(now time.Time) {
	if n.LeaderID() != n.cfg.ID || n.draining.Load() {
		return
	}
	n.mu.Lock()
	since := n.snapshotFailedSince
	failedAt := n.snapshotFailureIndex
	eligible := n.snapshotFailure != nil && !since.IsZero() && now.Sub(since) >= snapshotHandoffDelay && now.Sub(n.snapshotHandoffAttempt) >= snapshotHandoffDelay
	n.mu.Unlock()
	if !eligible {
		return
	}
	if !n.maintenanceMu.TryLock() {
		return
	}
	defer n.maintenanceMu.Unlock()
	if failedAt == 0 {
		return
	}
	status := n.raft.Status()
	if status.Lead != n.cfg.ID || len(status.Config.Voters[1]) != 0 || status.Config.AutoLeave {
		return
	}
	barrier := status.Commit
	var targets []uint64
	for id := range status.Config.Voters[0] {
		progress, known := status.Progress[id]
		if id != n.cfg.ID && known && progress.RecentActive && !progress.IsLearner && progress.Match >= barrier {
			targets = append(targets, id)
		}
	}
	slices.Sort(targets)
	if len(targets) == 0 {
		return
	}
	n.mu.Lock()
	if n.snapshotFailure == nil || n.snapshotFailedSince != since {
		n.mu.Unlock()
		return
	}
	n.snapshotHandoffAttempt = now
	n.mu.Unlock()
	timeout := min(5*time.Second, max(time.Second, n.cfg.Tick*time.Duration(n.cfg.ElectionTicks)))
	ctx, cancel := context.WithTimeout(n.ctx, timeout)
	defer cancel()
	for _, id := range targets {
		peer, err := n.peerStatus(ctx, id)
		// Require a completed snapshot covering the failed capture, not merely
		// an absent error (including older peers without snapshot status).
		if err != nil || peer.LeaderID != n.cfg.ID || peer.Error != "" || peer.SnapshotError != "" || peer.Draining || peer.Applied < barrier || peer.SnapshotIndex < failedAt {
			continue
		}
		current := n.raft.Status()
		n.mu.Lock()
		stillFailed := n.snapshotFailure != nil && n.snapshotFailedSince == since
		n.mu.Unlock()
		if !stillFailed || current.Lead != n.cfg.ID || current.Term != status.Term || n.draining.Load() {
			return
		}
		if _, voter := current.Config.Voters[0][id]; !voter || len(current.Config.Voters[1]) != 0 {
			return
		}
		_ = n.transferLeadership(ctx, id)
		return
	}
}
