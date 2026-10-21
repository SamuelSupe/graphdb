package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type snapshotProbe struct {
	Index  uint64 `json:"index"`
	Leader uint64 `json:"leader"`
	Term   uint64 `json:"term"`
	ctx    context.Context
	done   chan error
}

func (n *Node) validateSnapshotProbe(p snapshotProbe) error {
	if err := n.available(false); err != nil {
		return err
	}
	status := n.raft.Status()
	_, leaderVoter := status.Config.Voters[0][p.Leader]
	_, selfVoter := status.Config.Voters[0][n.cfg.ID]
	if p.Index == 0 || p.Leader == n.cfg.ID || p.Leader != status.Lead || p.Term != status.Term ||
		!leaderVoter || !selfVoter || len(status.Config.Voters[1]) != 0 || status.Config.AutoLeave ||
		n.draining.Load() || n.applied.Load() < p.Index {
		return ErrUnavailable
	}
	return nil
}

func (n *Node) snapshotProbeHandler(w http.ResponseWriter, r *http.Request) {
	if !n.protocolCompatible(r.Header.Get(protocolHeader)) {
		http.Error(w, "incompatible Raft protocol", http.StatusUpgradeRequired)
		return
	}
	var probe snapshotProbe
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&probe) != nil {
		http.Error(w, "invalid snapshot probe", http.StatusBadRequest)
		return
	}
	if !n.snapshotProbeMu.TryLock() {
		http.Error(w, "snapshot probe already active", http.StatusServiceUnavailable)
		return
	}
	defer n.snapshotProbeMu.Unlock()
	if err := n.validateSnapshotProbe(probe); err != nil {
		http.Error(w, "snapshot probe is not eligible", http.StatusServiceUnavailable)
		return
	}
	probe.ctx, probe.done = r.Context(), make(chan error, 1)
	select {
	case n.snapshotProbes <- probe:
	case <-r.Context().Done():
		return
	case <-n.ctx.Done():
		return
	}
	select {
	case err := <-probe.done:
		if err != nil || n.snapshotIndex.Load() < probe.Index || n.validateSnapshotProbe(probe) != nil {
			http.Error(w, "snapshot probe did not persist a current snapshot", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case <-r.Context().Done():
	case <-n.ctx.Done():
	}
}

func (n *Node) probePeerSnapshot(ctx context.Context, id, index, term uint64) error {
	n.peerMu.RLock()
	address := n.peers[id]
	n.peerMu.RUnlock()
	data, _ := json.Marshal(snapshotProbe{Index: index, Leader: n.cfg.ID, Term: term})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(address, "/")+"/raft/snapshot/probe", bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+n.cfg.Token)
	request.Header.Set("X-Raft-Cluster", n.cfg.ClusterID)
	request.Header.Set(protocolHeader, fmt.Sprint(n.protocolVersion()))
	request.Header.Set("Content-Type", "application/json")
	response, err := n.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent || !n.protocolCompatible(response.Header.Get(protocolHeader)) {
		return fmt.Errorf("peer %d snapshot probe returned %d", id, response.StatusCode)
	}
	return nil
}
