package replication

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type transportClient struct{ *http.Client }

func newTransportClient() transportClient {
	return transportClient{&http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
}

type packet struct {
	data     []byte
	snapshot bool
}

func (n *Node) send(messages []raftpb.Message) {
	for _, message := range messages {
		if message.To == 0 || message.To == n.cfg.ID {
			continue
		}
		data, err := message.Marshal()
		if err != nil {
			n.fail(err)
			return
		}
		n.peerMu.Lock()
		queue := n.senders[message.To]
		if queue == nil {
			queue = make(chan packet, 128)
			n.senders[message.To] = queue
			n.workers.Add(1)
			go n.sendLoop(message.To, queue)
		}
		n.peerMu.Unlock()
		select {
		case queue <- packet{data: data, snapshot: message.Type == raftpb.MsgSnap}:
		default:
			n.raft.ReportUnreachable(message.To)
			if message.Type == raftpb.MsgSnap {
				n.raft.ReportSnapshot(message.To, raft.SnapshotFailure)
			}
		}
	}
}

func (n *Node) sendLoop(id uint64, queue <-chan packet) {
	defer n.workers.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case packet := <-queue:
			n.peerMu.RLock()
			address := n.peers[id]
			n.peerMu.RUnlock()
			err := func() error {
				if address == "" {
					return fmt.Errorf("no address for Raft peer %d", id)
				}
				request, err := http.NewRequestWithContext(n.ctx, http.MethodPost, strings.TrimRight(address, "/")+"/raft/message", bytes.NewReader(packet.data))
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
				if response.StatusCode != http.StatusNoContent {
					return fmt.Errorf("peer %d returned %d", id, response.StatusCode)
				}
				return nil
			}()
			if err != nil {
				n.raft.ReportUnreachable(id)
			}
			if packet.snapshot {
				status := raft.SnapshotFinish
				if err != nil {
					status = raft.SnapshotFailure
				}
				n.raft.ReportSnapshot(id, status)
			}
		}
	}
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/message", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, n.cfg.MaxSnapshotBytes+32<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		var message raftpb.Message
		if err := message.Unmarshal(data); err != nil {
			http.Error(w, "invalid Raft message", http.StatusBadRequest)
			return
		}
		n.peerMu.RLock()
		_, known := n.peers[message.From]
		n.peerMu.RUnlock()
		if !known || message.To != n.cfg.ID || message.From == n.cfg.ID || raft.IsLocalMsg(message.Type) {
			http.Error(w, "invalid Raft peer message", http.StatusBadRequest)
			return
		}
		if err := n.available(false); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if err := n.raft.Step(r.Context(), message); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /raft/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := n.Status()
		n.peerMu.RLock()
		peers := make(map[uint64]string, len(n.peers))
		for id, origin := range n.peers {
			peers[id] = origin
		}
		n.peerMu.RUnlock()
		status["peers"] = peers
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("POST /raft/members", n.changeMember)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+n.cfg.Token)) != 1 || r.Header.Get("X-Raft-Cluster") != n.cfg.ClusterID {
			http.Error(w, "unauthorized Raft request", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

type memberChange struct {
	ID     uint64 `json:"id"`
	URL    string `json:"url,omitempty"`
	Action string `json:"action"`
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
		var retired bool
		n.disk.db.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("retired"))
			retired = bucket != nil && bucket.Get(indexKey(change.ID)) != nil
			return nil
		})
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
	data, _ := json.Marshal(change)
	if err := n.raft.ProposeConfChange(r.Context(), raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: kind, NodeID: change.ID}}, Context: data}); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	// Membership APIs acknowledge only after the configuration was applied.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			http.Error(w, r.Context().Err().Error(), http.StatusGatewayTimeout)
			return
		case <-n.ctx.Done():
			http.Error(w, "node stopped", http.StatusServiceUnavailable)
			return
		case <-ticker.C:
			current := n.raft.Status()
			progress, exists := current.Progress[change.ID]
			complete := (change.Action == "remove" && !exists) || (change.Action == "add_learner" && exists && progress.IsLearner) || (change.Action == "promote" && exists && !progress.IsLearner)
			if complete {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
	}
}

func (n *Node) applyPeerChange(data []byte) error {
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
			retired, err := tx.CreateBucketIfNotExists([]byte("retired"))
			if err != nil {
				return err
			}
			if err := retired.Put(indexKey(change.ID), []byte{1}); err != nil {
				return err
			}
		}
		return tx.Bucket([]byte("meta")).Put([]byte("peers"), encoded)
	})
}

func (n *Node) loadPeers() error {
	return n.disk.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("meta")).Get([]byte("peers"))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &n.peers)
	})
}

func (n *Node) LeaderID() uint64                { return n.leader.Load() }
func (n *Node) ID() uint64                      { return n.cfg.ID }
func (n *Node) Check(ctx context.Context) error { return n.ReadBarrier(ctx) }

func (n *Node) installPeers(peers map[uint64]string) error {
	n.peerMu.Lock()
	defer n.peerMu.Unlock()
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	if err := n.disk.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Put([]byte("peers"), data) }); err != nil {
		return err
	}
	for id, address := range peers {
		n.peers[id] = address
	}
	return nil
}
