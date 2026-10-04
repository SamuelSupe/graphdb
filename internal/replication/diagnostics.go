package replication

import (
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
	"go.etcd.io/raft/v3/tracker"
)

type diagnosticObservation struct {
	status map[string]any
	at     time.Time
}

func (n *Node) observeDiagnostics() {
	started := time.Now()
	finish := n.metrics.Start("status_sample")
	status := n.Status()
	finish(n.ctx.Err())
	n.diagnostics.Store(&diagnosticObservation{status: status, at: started})
}

func (n *Node) diagnosticsLoop() {
	defer n.workers.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.observeDiagnostics()
		}
	}
}

// DiagnosticStatus serves the last local status observation without waiting for
// Raft storage. Sample age accompanies values that can be stale during a stall.
// Strong reads, membership changes and upgrade preflight continue to use live state.
func (n *Node) DiagnosticStatus() map[string]any {
	observation := n.diagnostics.Load()
	status := map[string]any{"voters": []uint64(nil), "learners": []uint64(nil), "replicas": []PeerProgress(nil), "status_known": observation != nil, "status_age_seconds": float64(0)}
	if observation != nil {
		for key, value := range observation.status {
			status[key] = value
		}
		status["status_known"] = true
		status["status_age_seconds"] = max(0, time.Since(observation.at).Seconds())
		status["status_observed_at"] = observation.at.UTC().Format(time.RFC3339Nano)
		for _, key := range []string{"voters", "learners"} {
			if ids, ok := status[key].([]uint64); ok {
				status[key] = append([]uint64(nil), ids...)
			}
		}
		if peers, ok := status["replicas"].([]PeerProgress); ok {
			status["replicas"] = append([]PeerProgress(nil), peers...)
		}
	}
	n.mu.Lock()
	failure, snapshotFailure, cleanupFailure := n.failure, n.snapshotFailure, n.snapshotCleanupFailure
	n.mu.Unlock()
	for _, key := range []string{"error", "snapshot_error", "snapshot_cleanup_error"} {
		delete(status, key)
	}
	if failure != nil {
		status["error"] = failure.Error()
	}
	if snapshotFailure != nil {
		status["snapshot_error"] = snapshotFailure.Error()
	}
	if cleanupFailure != nil {
		status["snapshot_cleanup_error"] = cleanupFailure.Error()
	}
	status["leader_id"] = n.leader.Load()
	committed := n.committed.Load()
	status["durable_commit_index"] = committed
	status["durable_application_lag"] = committed - min(committed, n.applied.Load())
	status["draining"] = n.draining.Load()
	status["ready"] = observation != nil && failure == nil && n.ctx != nil && n.ctx.Err() == nil && n.leader.Load() == n.cfg.ID && !n.draining.Load()
	return status
}

func (n *Node) Status() map[string]any {
	n.mu.Lock()
	failure := n.failure
	snapshotFailure := n.snapshotFailure
	cleanupFailure := n.snapshotCleanupFailure
	proposals, reads := len(n.proposals), len(n.reads)
	n.mu.Unlock()
	raftStatus := n.raft.Status()
	applied := n.applied.Load()
	configuration := tracker.ProgressTracker{Config: raftStatus.Config}
	conf := configuration.ConfState()
	status := map[string]any{"node_id": n.cfg.ID, "cluster_id": n.cfg.ClusterID, "leader_id": n.leader.Load(), "term": raftStatus.Term, "commit_index": raftStatus.Commit, "applied_index": applied, "application_lag": raftStatus.Commit - min(raftStatus.Commit, applied), "application_bytes": n.applicationBytes.Load(), "proposal_bytes": n.proposalBytes.Load(), "ready": failure == nil && n.ctx.Err() == nil && n.leader.Load() == n.cfg.ID && !n.draining.Load(), "protocol_version": n.protocolVersion(), "allow_legacy_protocol": n.cfg.AllowLegacyProtocol, "draining": n.draining.Load(), "voters": conf.Voters, "learners": conf.Learners, "build": buildinfo.Current()}
	status["application_commits"] = n.applicationCommits.Load()
	status["application_entries"] = n.applicationEntries.Load()
	status["protocol_max"] = MaxProtocolVersion
	status["snapshot_format_max"] = 2
	status["stream_snapshots"] = n.cfg.StreamSnapshots
	status["tick_seconds"] = n.cfg.Tick.Seconds()
	status["election_timeout_seconds"] = n.cfg.Tick.Seconds() * float64(n.cfg.ElectionTicks)
	status["state"] = raftStatus.RaftState.String()
	status["pending_proposals"], status["pending_reads"] = proposals, reads
	status["snapshot_index"], status["snapshot_bytes"] = n.snapshotIndex.Load(), n.snapshotBytes.Load()
	members := conf.Voters
	members = append(append([]uint64(nil), members...), conf.Learners...)
	var peers []PeerProgress
	n.peerMu.RLock()
	for _, id := range members {
		progress, known := raftStatus.Progress[id]
		peer := PeerProgress{ID: id, Known: known, Match: progress.Match, Next: progress.Next, RecentActive: progress.RecentActive, Learner: progress.IsLearner, SendQueue: len(n.senders[id]) + len(n.controlSenders[id]), ControlSendQueue: len(n.controlSenders[id])}
		if !known {
			for _, learner := range conf.Learners {
				peer.Learner = peer.Learner || learner == id
			}
		} else {
			peer.Lag = raftStatus.Commit - min(raftStatus.Commit, progress.Match)
			peer.Paused = progress.IsPaused()
			if progress.Inflights != nil {
				peer.InflightMessages = progress.Inflights.Count()
			}
		}
		peers = append(peers, peer)
	}
	n.peerMu.RUnlock()
	status["replicas"] = peers
	if failure != nil {
		status["error"] = failure.Error()
	}
	if snapshotFailure != nil {
		status["snapshot_error"] = snapshotFailure.Error()
	}
	if cleanupFailure != nil {
		status["snapshot_cleanup_error"] = cleanupFailure.Error()
	}
	return status
}
