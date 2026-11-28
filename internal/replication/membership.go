package replication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

var errMembershipConflict = errors.New("membership changed; retry against the current configuration")

type memberChange struct {
	ID     uint64 `json:"id"`
	URL    string `json:"url,omitempty"`
	Action string `json:"action"`
}

type memberProposal struct {
	memberChange
	RequestID string            `json:"request_id,omitempty"`
	Expected  *raftpb.ConfState `json:"expected,omitempty"`
}

type membershipResult struct {
	id  string
	err error
}

type pendingPeerChange struct {
	change memberChange
	index  uint64
}

func (n *Node) changeMember(w http.ResponseWriter, r *http.Request) {
	if !n.membershipMu.TryLock() {
		http.Error(w, "another membership change is in progress", http.StatusConflict)
		return
	}
	defer n.membershipMu.Unlock()
	// Raft can turn a second pending configuration entry into a no-op.
	// Bound the wait so a discarded change does not hold up a drain.
	timeout := min(5*time.Second, max(2*time.Second, 2*time.Duration(n.cfg.ElectionTicks)*n.cfg.Tick))
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	r = r.WithContext(ctx)
	if err := n.beginProposal(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer n.endProposal()
	if err := n.ReadBarrier(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var change memberChange
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&change); err != nil || change.ID == 0 {
		http.Error(w, "invalid membership change", http.StatusBadRequest)
		return
	}
	status := n.raft.Status()
	if status.LeadTransferee != 0 {
		http.Error(w, "Raft leadership transfer is in progress", http.StatusServiceUnavailable)
		return
	}
	kind := raftpb.ConfChangeAddLearnerNode
	switch change.Action {
	case "add_learner":
		parsed, err := url.Parse(change.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			http.Error(w, "member URL must be an HTTP or HTTPS origin on the private Raft network", http.StatusBadRequest)
			return
		}
		retired, err := n.memberRetired(change.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if retired {
			http.Error(w, "removed member IDs cannot be reused", http.StatusConflict)
			return
		}
		if _, exists := status.Progress[change.ID]; exists {
			http.Error(w, "member ID is already in use", http.StatusConflict)
			return
		}
		if err := n.probeLearnerCompatibility(r.Context(), change.ID, change.URL); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
	case "promote":
		progress, ok := status.Progress[change.ID]
		if !ok || !progress.IsLearner || progress.Match < status.Commit {
			http.Error(w, "learner has not caught up", http.StatusConflict)
			return
		}
		probe, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		peer, err := n.peerStatus(probe, change.ID)
		cancel()
		if err != nil || peer.Error != "" || peer.SnapshotError != "" || peer.Applied < status.Commit ||
			(n.protocolVersion() >= 6 && peer.Protocol < n.protocolVersion()) {
			http.Error(w, "learner application or protocol has not caught up", http.StatusConflict)
			return
		}
		kind = raftpb.ConfChangeAddNode
	case "remove":
		if _, exists := status.Progress[change.ID]; !exists {
			http.Error(w, "member does not exist", http.StatusNotFound)
			return
		}
		if len(status.Config.Voters.IDs()) <= 3 && !status.Progress[change.ID].IsLearner {
			http.Error(w, "add and promote a replacement before removing a voter", http.StatusConflict)
			return
		}
		kind = raftpb.ConfChangeRemoveNode
	default:
		http.Error(w, "action must be add_learner, promote or remove", http.StatusBadRequest)
		return
	}
	id, err := randomID()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	progress := tracker.ProgressTracker{Config: status.Config}
	expected := progress.ConfState()
	data, err := json.Marshal(memberProposal{memberChange: change, RequestID: id, Expected: &expected})
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	ch := make(chan result, 1)
	n.mu.Lock()
	n.proposals[id] = ch
	n.mu.Unlock()
	defer func() { n.mu.Lock(); delete(n.proposals, id); n.mu.Unlock() }()
	if err := n.raft.ProposeConfChange(r.Context(), raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: kind, NodeID: change.ID}}, Context: data}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	for {
		select {
		case outcome := <-ch:
			if outcome.err != nil {
				http.Error(w, outcome.err.Error(), http.StatusConflict)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return
		default:
		}
		n.mu.Lock()
		changed := n.stateChanged
		n.mu.Unlock()
		select {
		case outcome := <-ch:
			if outcome.err != nil {
				http.Error(w, outcome.err.Error(), http.StatusConflict)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return
		case <-changed:
			select {
			case outcome := <-ch:
				if outcome.err != nil {
					http.Error(w, outcome.err.Error(), http.StatusConflict)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
				return
			default:
			}
			if err := n.availableState(true, false); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		case <-r.Context().Done():
			http.Error(w, r.Context().Err().Error(), http.StatusGatewayTimeout)
			return
		case <-n.ctx.Done():
			http.Error(w, "node stopped", http.StatusServiceUnavailable)
			return
		}
	}
}

func (n *Node) memberRetired(id uint64) (bool, error) {
	_, retired, err := n.memberRetiredAt(id)
	return retired, err
}

// probeLearnerCompatibility best-effort probes a reachable learner candidate
// before admitting it to the Raft configuration so incompatible protocols or
// mismatched cluster IDs are rejected before receiving snapshots.
func (n *Node) probeLearnerCompatibility(ctx context.Context, id uint64, address string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(address, "/")+"/raft/status", nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+n.cfg.Token)
	req.Header.Set("X-Raft-Cluster", n.cfg.ClusterID)
	resp, err := n.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	if !n.protocolCompatible(resp.Header.Get(protocolHeader)) {
		return fmt.Errorf("learner %d has an incompatible Raft protocol", id)
	}
	var peer PeerStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&peer); err != nil {
		return nil
	}
	if (peer.ID != 0 && peer.ID != id) || (peer.ClusterID != "" && peer.ClusterID != n.cfg.ClusterID) {
		return fmt.Errorf("learner %d returned a mismatched identity or cluster", id)
	}
	if max(peer.Protocol, peer.MaxProtocol) < n.protocolVersion() && !(peer.Protocol == 0 && n.cfg.AllowLegacyProtocol && n.protocolVersion() == ProtocolVersion) {
		return fmt.Errorf("learner %d max protocol %d is below active protocol %d", id, max(peer.Protocol, peer.MaxProtocol), n.protocolVersion())
	}
	return nil
}

func (n *Node) memberRetiredAt(id uint64) (uint64, bool, error) {
	members, err := n.retiredMembers()
	removedAt, retired := members[id]
	return removedAt, retired, err
}

func (n *Node) retiredMembers() (map[uint64]uint64, error) {
	if members := n.retired.Load(); members != nil {
		return *members, nil
	}
	members := map[uint64]uint64{}
	err := n.disk.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("retired"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			var index uint64
			if len(value) == 8 {
				index = binary.BigEndian.Uint64(value)
			}
			members[binary.BigEndian.Uint64(key)] = index
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	n.retired.CompareAndSwap(nil, &members)
	return *n.retired.Load(), nil
}

func (n *Node) cacheRetired(ids []uint64, index uint64) error {
	if _, err := n.retiredMembers(); err != nil {
		return err
	}
	for {
		previous := n.retired.Load()
		members := maps.Clone(*previous)
		for _, id := range ids {
			if _, found := members[id]; !found {
				members[id] = index
			}
		}
		if n.retired.CompareAndSwap(previous, &members) {
			return nil
		}
	}
}

func (n *Node) applyMembership(change raftpb.ConfChangeV2, current raftpb.ConfState, index uint64, pendingRetired map[uint64]uint64) (raftpb.ConfState, membershipResult, *memberChange, error) {
	if len(change.Context) == 0 {
		return *n.raft.ApplyConfChange(change), membershipResult{}, nil, nil
	}
	var proposal memberProposal
	if err := json.Unmarshal(change.Context, &proposal); err != nil {
		return current, membershipResult{}, nil, err
	}
	outcome := membershipResult{id: proposal.RequestID}
	reject := func() (raftpb.ConfState, membershipResult, *memberChange, error) {
		outcome.err = errMembershipConflict
		return current, outcome, nil, nil
	}
	// Admission can become stale across concurrent requests or a leader change.
	// Compare membership, rather than local snapshot positions, on every replica.
	if proposal.Expected != nil && proposal.Expected.Equivalent(current) != nil {
		return reject()
	}
	voter := slices.Contains(current.Voters, proposal.ID) || slices.Contains(current.VotersOutgoing, proposal.ID)
	learner := slices.Contains(current.Learners, proposal.ID) || slices.Contains(current.LearnersNext, proposal.ID)
	switch proposal.Action {
	case "add_learner":
		removedAt, retired, err := n.memberRetiredAt(proposal.ID)
		if err != nil {
			return current, outcome, nil, err
		}
		retiredBeforeIndex := (retired && (removedAt == 0 || removedAt < index)) || (pendingRetired != nil && pendingRetired[proposal.ID] > 0)
		if voter || learner || retiredBeforeIndex {
			return reject()
		}
	case "promote":
		if !learner || voter {
			return reject()
		}
	case "remove":
		if (!voter && !learner) || (voter && len(current.Voters) <= 3) {
			return reject()
		}
	default:
		return reject()
	}
	// etcd/raft requires rejected changes to be skipped, without ApplyConfChange.
	current = *n.raft.ApplyConfChange(change)
	if proposal.Action == "remove" && pendingRetired != nil {
		pendingRetired[proposal.ID] = index
	}
	applied := proposal.memberChange
	return current, outcome, &applied, nil
}

func (n *Node) applyPeerChange(data []byte, index uint64) error {
	var change memberChange
	if err := json.Unmarshal(data, &change); err != nil {
		return err
	}
	n.peerMu.RLock()
	fallbackPeers := maps.Clone(n.peers)
	n.peerMu.RUnlock()
	conf, confIndex := n.disk.confState()
	peerChanges := []pendingPeerChange{{change: change, index: index}}
	if err := n.disk.saveConfWithMembership(conf, confIndex, fallbackPeers, peerChanges); err != nil {
		return err
	}
	return n.applyPeerChangesInMemory(peerChanges)
}

func (n *Node) applyPeerChangesInMemory(peerChanges []pendingPeerChange) error {
	for _, pc := range peerChanges {
		if pc.change.Action == "remove" {
			if err := n.cacheRetired([]uint64{pc.change.ID}, pc.index); err != nil {
				return err
			}
		}
	}
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	for _, pc := range peerChanges {
		if pc.change.Action == "add_learner" {
			n.peers[pc.change.ID] = pc.change.URL
		}
		if pc.change.Action == "remove" {
			n.stopPeerSenders(pc.change.ID)
		}
	}
	return nil
}
