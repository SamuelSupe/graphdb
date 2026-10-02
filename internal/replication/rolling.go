package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
)

// ProtocolVersion identifies command semantics as well as wire and disk formats.
// A new command or an incompatible application change requires a new protocol.
const ProtocolVersion = 1
const protocolHeader = "X-GraphDB-Raft-Protocol"

type PeerStatus struct {
	ID            uint64         `json:"node_id"`
	ClusterID     string         `json:"cluster_id"`
	LeaderID      uint64         `json:"leader_id"`
	Commit        uint64         `json:"commit_index"`
	Applied       uint64         `json:"applied_index"`
	Protocol      int            `json:"protocol_version"`
	Draining      bool           `json:"draining"`
	Ready         bool           `json:"ready"`
	Error         string         `json:"error,omitempty"`
	SnapshotError string         `json:"snapshot_error,omitempty"`
	Build         buildinfo.Info `json:"build"`
}

type UpgradeStatus struct {
	NodeID     uint64       `json:"node_id"`
	LeaderID   uint64       `json:"leader_id"`
	Protocol   int          `json:"protocol_version"`
	Safe       bool         `json:"safe_to_restart"`
	CatchingUp bool         `json:"catching_up"`
	Reason     string       `json:"reason,omitempty"`
	Members    []PeerStatus `json:"members"`
}

func (n *Node) protocolCompatible(raw string) bool {
	if raw == "" {
		return n.cfg.AllowLegacyProtocol
	}
	return raw == strconv.Itoa(ProtocolVersion)
}

func (n *Node) beginProposal(ctx context.Context) error {
	for {
		n.mu.Lock()
		if !n.handoff {
			n.activeProposals++
			n.mu.Unlock()
			return nil
		}
		changed := n.changed
		n.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrUnavailable
		}
	}
}

func (n *Node) endProposal() {
	n.mu.Lock()
	n.activeProposals--
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
}

func (n *Node) peerStatus(ctx context.Context, id uint64) (PeerStatus, error) {
	n.peerMu.RLock()
	address := n.peers[id]
	n.peerMu.RUnlock()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+"/raft/status", nil)
	if err != nil {
		return PeerStatus{}, err
	}
	request.Header.Set("Authorization", "Bearer "+n.cfg.Token)
	request.Header.Set("X-Raft-Cluster", n.cfg.ClusterID)
	response, err := n.client.Do(request)
	if err != nil {
		return PeerStatus{}, err
	}
	defer response.Body.Close()
	var status PeerStatus
	if response.StatusCode != http.StatusOK || !n.protocolCompatible(response.Header.Get(protocolHeader)) {
		return status, fmt.Errorf("peer %d is unavailable or has an incompatible protocol", id)
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&status)
	if err == nil && (status.ID != id || (status.ClusterID != "" && status.ClusterID != n.cfg.ClusterID) ||
		(status.Protocol != ProtocolVersion && !(status.Protocol == 0 && n.cfg.AllowLegacyProtocol))) {
		err = fmt.Errorf("peer %d returned an incompatible identity or protocol", id)
	}
	return status, err
}

func (n *Node) UpgradeStatus(ctx context.Context) UpgradeStatus {
	report := UpgradeStatus{NodeID: n.cfg.ID, LeaderID: n.LeaderID(), Protocol: ProtocolVersion}
	status := n.raft.Status()
	if len(status.Config.Voters[1]) != 0 || len(status.Config.Learners) != 0 || len(status.Config.LearnersNext) != 0 || status.Config.AutoLeave {
		report.Reason = "finish membership changes before upgrading"
		return report
	}
	ids := status.Config.Voters.IDs()
	if len(ids) < 3 {
		report.Reason = "rolling upgrades require at least three voting replicas"
		return report
	}
	if _, voter := ids[n.cfg.ID]; !voter {
		report.Reason = "this node is not a voting replica"
		return report
	}
	var ordered []uint64
	for id := range ids {
		ordered = append(ordered, id)
	}
	slices.Sort(ordered)
	for _, id := range ordered {
		var peer PeerStatus
		var err error
		if id == n.cfg.ID {
			data, _ := json.Marshal(n.Status())
			err = json.Unmarshal(data, &peer)
		} else {
			probe, cancel := context.WithTimeout(ctx, 2*time.Second)
			peer, err = n.peerStatus(probe, id)
			cancel()
		}
		if err != nil {
			report.Reason = fmt.Sprintf("peer %d: %v", id, err)
			return report
		}
		if peer.Error != "" || peer.SnapshotError != "" || (peer.Draining && id != n.cfg.ID) {
			report.Reason = fmt.Sprintf("peer %d is failed or already draining", id)
			return report
		}
		report.Members = append(report.Members, peer)
	}
	var commit uint64
	for _, peer := range report.Members {
		if peer.ID == report.LeaderID && peer.Ready {
			commit = peer.Commit
		}
	}
	if commit == 0 {
		report.Reason = "no ready leader"
		return report
	}
	for _, peer := range report.Members {
		if peer.LeaderID != report.LeaderID || peer.Applied < commit {
			report.CatchingUp = true
			report.Reason = fmt.Sprintf("peer %d has not caught up to leader commit %d", peer.ID, commit)
			return report
		}
	}
	report.Safe = true
	return report
}

func (n *Node) replicationProbe(ctx context.Context) error {
	leader := n.LeaderID()
	if leader == n.cfg.ID {
		// Drain owns proposal admission here. A fresh no-op distinguishes live
		// replication from matching checkpoints left by a broken transport.
		_, err := n.propose(ctx, nil)
		return err
	}
	n.peerMu.RLock()
	address := n.peers[leader]
	n.peerMu.RUnlock()
	if leader == 0 || address == "" {
		return ErrNotLeader
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(address, "/")+"/raft/upgrade/barrier", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+n.cfg.Token)
	request.Header.Set("X-Raft-Cluster", n.cfg.ClusterID)
	response, err := n.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusNoContent || !n.protocolCompatible(response.Header.Get(protocolHeader)) {
		return fmt.Errorf("leader cannot confirm fresh upgrade replication (HTTP %d); transfer from an unversioned leader first", response.StatusCode)
	}
	return nil
}

// Drain keeps replication running. The caller removes this process only after
// success and must serialize upgrades across the group (including all hosts).
func (n *Node) Drain(ctx context.Context) (err error) {
	if !n.maintenanceMu.TryLock() {
		return fmt.Errorf("another local maintenance operation is running")
	}
	defer n.maintenanceMu.Unlock()
	wasDraining := n.draining.Load()
	n.mu.Lock()
	n.handoff = true
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.handoff = false
		close(n.changed)
		n.changed = make(chan struct{})
		n.mu.Unlock()
		if err != nil {
			n.draining.Store(wasDraining)
		}
	}()
	for {
		n.mu.Lock()
		active, changed := n.activeProposals, n.changed
		n.mu.Unlock()
		if active == 0 {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrUnavailable
		}
	}
	if err := n.replicationProbe(ctx); err != nil {
		return err
	}
	report := n.UpgradeStatus(ctx)
	for report.CatchingUp {
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrUnavailable
		}
		report = n.UpgradeStatus(ctx)
	}
	if !report.Safe {
		return fmt.Errorf("unsafe to drain: %s", report.Reason)
	}
	if report.LeaderID == n.cfg.ID {
		if err = n.ReadBarrier(ctx); err != nil {
			return err
		}
	}
	n.draining.Store(true)
	if report.LeaderID != n.cfg.ID {
		return nil
	}
	var target uint64
	for _, peer := range report.Members {
		if peer.ID != n.cfg.ID {
			target = peer.ID
			break
		}
	}
	return n.transferLeadership(ctx, target)
}

func (n *Node) transferLeadership(ctx context.Context, target uint64) error {
	leader := n.LeaderID()
	if target == 0 || target == leader {
		return fmt.Errorf("target must be a different voting replica")
	}
	status := n.raft.Status()
	if _, voter := status.Config.Voters.IDs()[target]; !voter {
		return fmt.Errorf("target is not a voter")
	}
	n.raft.TransferLeadership(ctx, leader, target)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.LeaderID() == target {
			peer, err := n.peerStatus(ctx, target)
			if err == nil && peer.Ready && peer.LeaderID == target && peer.Applied >= peer.Commit {
				return nil
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrUnavailable
		}
	}
}

func (n *Node) Draining() bool { return n.draining.Load() }

func (n *Node) LeaderAddress(ctx context.Context) (string, error) {
	for {
		if err := n.available(false); err != nil {
			return "", err
		}
		n.mu.Lock()
		changed, handoff := n.changed, n.handoff
		n.mu.Unlock()
		leader := n.LeaderID()
		if !handoff {
			n.peerMu.RLock()
			address := n.peers[leader]
			n.peerMu.RUnlock()
			if leader == 0 || leader == n.cfg.ID || address == "" {
				return "", ErrNotLeader
			}
			return address, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return "", ctx.Err()
		case <-n.ctx.Done():
			return "", ErrUnavailable
		}
	}
}

func (n *Node) rollingHandlers(mux *http.ServeMux) {
	mux.HandleFunc("POST /raft/upgrade/barrier", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if _, err := n.Propose(ctx, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /raft/upgrade", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(n.UpgradeStatus(r.Context()))
	})
	mux.HandleFunc("POST /raft/drain", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := n.Drain(ctx); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /raft/resume", func(w http.ResponseWriter, r *http.Request) {
		if !n.maintenanceMu.TryLock() {
			http.Error(w, "maintenance is running", http.StatusConflict)
			return
		}
		defer n.maintenanceMu.Unlock()
		n.draining.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /raft/transfer", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Target uint64 `json:"target"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&request) != nil {
			http.Error(w, "invalid leadership transfer", http.StatusBadRequest)
			return
		}
		report := n.UpgradeStatus(r.Context())
		if !report.Safe {
			http.Error(w, report.Reason, http.StatusConflict)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := n.transferLeadership(ctx, request.Target); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
