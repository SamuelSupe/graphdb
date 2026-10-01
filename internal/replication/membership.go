package replication

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"

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

func (n *Node) changeMember(w http.ResponseWriter, r *http.Request) {
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
	kind := raftpb.ConfChangeAddLearnerNode
	switch change.Action {
	case "add_learner":
		parsed, err := url.Parse(change.URL)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			http.Error(w, "member URL must be an HTTP origin on the private Raft network", http.StatusBadRequest)
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
	case "promote":
		progress, ok := status.Progress[change.ID]
		if !ok || !progress.IsLearner || progress.Match < status.Commit {
			http.Error(w, "learner has not caught up", http.StatusConflict)
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
		n.mu.Lock()
		changed := n.changed
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
			if err := n.available(true); err != nil {
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
	var retired bool
	err := n.disk.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("retired"))
		retired = bucket != nil && bucket.Get(indexKey(id)) != nil
		return nil
	})
	return retired, err
}

func (n *Node) applyMembership(change raftpb.ConfChangeV2, current raftpb.ConfState, index uint64) (raftpb.ConfState, membershipResult, error) {
	if len(change.Context) == 0 {
		return *n.raft.ApplyConfChange(change), membershipResult{}, nil
	}
	var proposal memberProposal
	if err := json.Unmarshal(change.Context, &proposal); err != nil {
		return current, membershipResult{}, err
	}
	outcome := membershipResult{id: proposal.RequestID}
	reject := func() (raftpb.ConfState, membershipResult, error) {
		outcome.err = errMembershipConflict
		return current, outcome, nil
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
		retired, err := n.memberRetired(proposal.ID)
		if err != nil {
			return current, outcome, err
		}
		if voter || learner || retired {
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
	return current, outcome, n.applyPeerChange(change.Context, index)
}

func (n *Node) applyPeerChange(data []byte, index uint64) error {
	var change memberChange
	if err := json.Unmarshal(data, &change); err != nil {
		return err
	}
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	if change.Action == "add_learner" {
		n.peers[change.ID] = change.URL
	}
	// Retain removed IDs as tombstones, so they can never be reused.
	encoded, err := json.Marshal(n.peers)
	if err != nil {
		return err
	}
	return n.disk.db.Update(func(tx *bolt.Tx) error {
		if change.Action == "remove" {
			if err := persistRetired(tx, []uint64{change.ID}, index); err != nil {
				return err
			}
		}
		return tx.Bucket([]byte("meta")).Put([]byte("peers"), encoded)
	})
}
