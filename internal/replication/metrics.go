package replication

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"go.etcd.io/raft/v3/raftpb"
)

type PeerProgress struct {
	ID               uint64 `json:"node_id"`
	Known            bool   `json:"progress_known"`
	Match            uint64 `json:"match_index"`
	Next             uint64 `json:"next_index"`
	Lag              uint64 `json:"replication_lag"`
	RecentActive     bool   `json:"recent_active"`
	Learner          bool   `json:"learner"`
	Paused           bool   `json:"paused"`
	SendQueue        int    `json:"send_queue"`
	ControlSendQueue int    `json:"control_send_queue"`
	InflightMessages int    `json:"inflight_messages"`
}

func snapshotPayloadBytes(data []byte, envelope *snapshotEnvelope) int64 {
	size := int64(len(data))
	if envelope != nil && envelope.File != nil {
		size += envelope.File.Bytes
	}
	return size
}

func (n *Node) observeSnapshot(snapshot raftpb.Snapshot) {
	n.snapshotIndex.Store(snapshot.Metadata.Index)
	size := int64(len(snapshot.Data))
	if len(snapshot.Data) > 0 && len(snapshot.Data) <= snapshotMetadataLimit {
		if envelope, err := decodeSnapshot(snapshot.Data); err == nil {
			size = snapshotPayloadBytes(snapshot.Data, &envelope)
		}
	}
	n.snapshotBytes.Store(size)
}

// WriteMetrics reads sampled local Raft status and bounded transport queues. Match
// indexes describe replicated logs, not remote application acknowledgement.
func (n *Node) WriteMetrics(w io.Writer) {
	status := n.DiagnosticStatus()
	n.metrics.WritePrometheus(w, "graphdb_raft")
	observability.WriteScalar(w, "graphdb_raft_status_observation_known", "Whether a local Raft status sample is available; not a quorum guarantee.", "gauge", boolFloat(status["status_known"] == true))
	observability.WriteScalar(w, "graphdb_raft_status_age_seconds", "Seconds since the last completed status sample began; a stalled Raft loop prevents refresh.", "gauge", status["status_age_seconds"].(float64))
	observability.WriteScalar(w, "graphdb_raft_snapshot_cleanup_failed", "Old snapshot file cleanup needs retry; durable replication remains active.", "gauge", boolFloat(status["snapshot_cleanup_error"] != nil))
	observability.WriteInfo(w, "graphdb_raft_node_info", "Local Raft group identity.", []string{"cluster_id", "node_id"}, []string{n.cfg.ClusterID, strconv.FormatUint(n.cfg.ID, 10)})
	for _, gauge := range []struct{ key, name, help string }{
		{"term", "term", "Current local Raft term."},
		{"commit_index", "commit_index", "Local Raft committed log index."},
		{"durable_commit_index", "durable_commit_index", "Latest committed log index durably recorded locally."},
		{"durable_application_lag", "durable_application_lag", "Locally durable committed entries not yet applied; sampled without waiting for Raft."},
		{"applied_index", "applied_index", "Local durably applied log index."},
		{"application_bytes", "application_bytes", "Bytes loaded for application from the durable log."},
		{"proposal_bytes", "proposal_bytes", "Bytes charged to locally waiting proposals."},
		{"pending_proposals", "pending_proposals", "Locally waiting proposal responses."},
		{"pending_reads", "pending_reads", "Locally waiting ReadIndex responses."},
		{"snapshot_index", "snapshot_index", "Latest locally persisted Raft snapshot index."},
		{"snapshot_bytes", "snapshot_bytes", "Latest persisted snapshot payload bytes including streaming data."},
		{"protocol_version", "protocol_version", "Active local Raft application protocol."},
		{"tick_seconds", "tick_seconds", "Configured heartbeat interval in seconds."},
		{"election_timeout_seconds", "election_timeout_seconds", "Configured minimum election timeout; randomized follower election waits up to twice this value."},
	} {
		var value float64
		switch v := status[gauge.key].(type) {
		case uint64:
			value = float64(v)
		case int64:
			value = float64(v)
		case int:
			value = float64(v)
		case float64:
			value = v
		}
		observability.WriteScalar(w, "graphdb_raft_"+gauge.name, gauge.help, "gauge", value)
	}
	observability.WriteScalar(w, "graphdb_raft_leader_changes_total", "Observed leader ID transitions including loss and initial election in this process.", "counter", float64(n.leaderChanges.Load()))
	observability.WriteScalar(w, "graphdb_raft_application_commits_total", "Durably applied state machine batches in this process.", "counter", float64(n.applicationCommits.Load()))
	observability.WriteScalar(w, "graphdb_raft_application_entries_total", "Durably applied log entries in this process.", "counter", float64(n.applicationEntries.Load()))
	observability.WriteScalar(w, "graphdb_raft_draining", "Local node has entered drain mode.", "gauge", boolFloat(n.draining.Load()))
	observability.WriteScalar(w, "graphdb_raft_voters", "Voters in the locally observed configuration.", "gauge", float64(len(status["voters"].([]uint64))))
	observability.WriteScalar(w, "graphdb_raft_learners", "Learners in the locally observed configuration.", "gauge", float64(len(status["learners"].([]uint64))))
	fmt.Fprintln(w, "# HELP graphdb_raft_state Local Raft state, not proof of current quorum.\n# TYPE graphdb_raft_state gauge")
	for _, state := range []string{"StateFollower", "StateCandidate", "StatePreCandidate", "StateLeader"} {
		fmt.Fprintf(w, "graphdb_raft_state{state=%q} %g\n", state, boolFloat(status["state"] == state))
	}
	info, err := os.Stat(filepath.Join(n.cfg.Dir, "raft.db"))
	observability.WriteScalar(w, "graphdb_raft_storage_inspection_success", "Whether local Raft database size could be inspected.", "gauge", boolFloat(err == nil))
	if err == nil {
		observability.WriteScalar(w, "graphdb_raft_storage_bytes", "Local Raft database file size, including reusable pages.", "gauge", float64(info.Size()))
	}
	peers := status["replicas"].([]PeerProgress)
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	for _, metric := range []struct {
		name, help string
		value      func(PeerProgress) float64
	}{
		{"progress_known", "Leader has progress for this member; zero means other progress values are unknown.", func(p PeerProgress) float64 { return boolFloat(p.Known) }},
		{"match_index", "Member's matched durable log index as observed by the leader.", func(p PeerProgress) float64 { return float64(p.Match) }},
		{"next_index", "Next log index the leader plans to send to the member.", func(p PeerProgress) float64 { return float64(p.Next) }},
		{"replication_lag", "Leader commit index minus member match, clamped at zero.", func(p PeerProgress) float64 { return float64(p.Lag) }},
		{"recent_active", "Recent member activity observed by the leader; not a quorum guarantee.", func(p PeerProgress) float64 { return boolFloat(p.RecentActive) }},
		{"learner", "Member is a learner in the local configuration.", func(p PeerProgress) float64 { return boolFloat(p.Learner) }},
		{"paused", "Leader replication to this member is paused.", func(p PeerProgress) float64 { return boolFloat(p.Paused) }},
		{"send_queue", "Packets currently waiting in both local bounded sender queues.", func(p PeerProgress) float64 { return float64(p.SendQueue) }},
		{"control_send_queue", "Quorum and response packets waiting independently of append and snapshot traffic.", func(p PeerProgress) float64 { return float64(p.ControlSendQueue) }},
		{"inflight_messages", "Unacknowledged append messages tracked by the leader.", func(p PeerProgress) float64 { return float64(p.InflightMessages) }},
	} {
		name := "graphdb_raft_peer_" + metric.name
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, metric.help, name)
		for _, peer := range peers {
			fmt.Fprintf(w, "%s{peer_id=%q} %g\n", name, strconv.FormatUint(peer.ID, 10), metric.value(peer))
		}
	}
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
