package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type membershipMachine struct {
	applied          atomic.Uint64
	pauseEnabled     atomic.Bool
	entered, release chan struct{}
}

func (m *membershipMachine) Applied() (uint64, error) { return m.applied.Load(), nil }
func (m *membershipMachine) Apply(ctx context.Context, index uint64, data []byte) ([]byte, error) {
	if string(data) == "hold application" && m.pauseEnabled.Swap(false) {
		close(m.entered)
		select {
		case <-m.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	m.applied.Store(index)
	return data, nil
}
func (m *membershipMachine) Snapshot(context.Context) ([]byte, error) { return []byte("state"), nil }
func (m *membershipMachine) Restore(_ context.Context, index uint64, _ []byte) error {
	m.applied.Store(index)
	return nil
}

func TestMembershipRejectsStaleChanges(t *testing.T) {
	for _, scenario := range []struct {
		action string
		legacy bool
	}{{"remove", false}, {"add_learner", false}, {"remove", true}, {"add_learner", true}, {"snapshot_metadata", false}} {
		name := scenario.action
		if scenario.legacy {
			name += "/legacy"
		}
		t.Run(name, func(t *testing.T) {
			action := scenario.action
			var endpoints [4]atomic.Pointer[Node]
			var endpointLocks [4]sync.RWMutex
			var rejectAppend [4]atomic.Bool
			var nodes [4]*Node
			var configs [4]Config
			var machines [4]membershipMachine
			peers := make(map[uint64]string)
			for i := range endpoints {
				i := i
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					endpointLocks[i].RLock()
					defer endpointLocks[i].RUnlock()
					if rejectAppend[i].Load() && r.URL.Path == "/raft/message" {
						data, err := io.ReadAll(r.Body)
						var message raftpb.Message
						if err != nil || message.Unmarshal(data) != nil || message.Type == raftpb.MsgApp {
							http.Error(w, "append isolated", http.StatusServiceUnavailable)
							return
						}
						r.Body = io.NopCloser(bytes.NewReader(data))
					}
					if node := endpoints[i].Load(); node != nil {
						node.Handler().ServeHTTP(w, r)
					} else {
						http.Error(w, "node stopped", http.StatusServiceUnavailable)
					}
				}))
				t.Cleanup(server.Close)
				peers[uint64(i+1)] = server.URL
			}
			t.Cleanup(func() {
				for i, node := range nodes {
					endpoints[i].Store(nil)
					if node != nil {
						node.Close()
					}
				}
			})
			start := func(i int) {
				t.Helper()
				node, err := Open(context.Background(), configs[i], &machines[i])
				if err != nil {
					t.Fatal(err)
				}
				nodes[i] = node
				endpoints[i].Store(node)
			}
			for i := range nodes {
				machines[i].entered, machines[i].release = make(chan struct{}), make(chan struct{})
				members := make(map[uint64]string)
				for id, origin := range peers {
					if id <= 3 || i == 3 {
						members[id] = origin
					}
				}
				configs[i] = Config{ID: uint64(i + 1), ClusterID: "membership", Dir: t.TempDir(), Peers: members, Bootstrap: i < 3, ElectionTicks: 10, Token: "membership-test-token-32-bytes-long", Tick: 20 * time.Millisecond, SnapshotEntries: 1000}
				start(i)
			}
			wait := func(description string, condition func() bool) {
				t.Helper()
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if condition() {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatalf("timed out waiting for %s", description)
			}
			var leader *Node
			wait("leader", func() bool {
				for _, node := range nodes {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					err := node.ReadBarrier(ctx)
					cancel()
					if err == nil {
						leader = node
						return true
					}
				}
				return false
			})
			request := func(change memberChange) {
				t.Helper()
				body, _ := json.Marshal(change)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				r := httptest.NewRequest(http.MethodPost, "/raft/members", bytes.NewReader(body)).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer "+leader.cfg.Token)
				r.Header.Set("X-Raft-Cluster", leader.cfg.ClusterID)
				w := httptest.NewRecorder()
				leader.Handler().ServeHTTP(w, r)
				if w.Code != http.StatusNoContent {
					t.Fatalf("membership %s: %d: %s", body, w.Code, w.Body.String())
				}
			}
			conf := func() raftpb.ConfState {
				progress := tracker.ProgressTracker{Config: leader.raft.Status().Config}
				return progress.ConfState()
			}
			if action == "snapshot_metadata" {
				victim := int(leader.ID() % 3)
				machines[victim].pauseEnabled.Store(true)
				defer close(machines[victim].release)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := leader.Propose(ctx, []byte("hold application")); err != nil {
					t.Fatal(err)
				}
				select {
				case <-machines[victim].entered:
				case <-ctx.Done():
					t.Fatal("follower application did not block")
				}
				// Drain existing handlers and keep queued appends isolated until
				// snapshot persistence; otherwise an append can make it obsolete.
				endpointLocks[victim].Lock()
				rejectAppend[victim].Store(true)
				endpoints[victim].Store(nil)
				endpointLocks[victim].Unlock()
				request(memberChange{ID: 4, URL: peers[4], Action: "add_learner"})
				index := leader.raft.Status().Commit
				wait("leader compacted old entries", func() bool {
					snapshot, _ := leader.disk.Snapshot()
					return snapshot.Metadata.Index >= index
				})
				endpoints[victim].Store(nodes[victim])
				wait("follower persisted snapshot while application blocked", func() bool {
					snapshot, _ := nodes[victim].disk.Snapshot()
					return snapshot.Metadata.Index >= index
				})
				rejectAppend[victim].Store(false)
				request(memberChange{ID: 5, URL: "http://127.0.0.1:1", Action: "add_learner"})
				index = leader.raft.Status().Commit
				wait("follower applied subsequent membership", func() bool {
					nodes[victim].peerMu.RLock()
					defer nodes[victim].peerMu.RUnlock()
					return nodes[victim].peers[5] != ""
				})
				machines[victim].release <- struct{}{}
				wait("follower restored snapshot and caught up", func() bool { return nodes[victim].applied.Load() >= index })
				if err := nodes[victim].disk.db.View(func(tx *bolt.Tx) error {
					var saved map[uint64]string
					if err := json.Unmarshal(tx.Bucket([]byte("meta")).Get([]byte("peers")), &saved); err != nil {
						return err
					}
					if saved[5] != "http://127.0.0.1:1" {
						return fmt.Errorf("snapshot erased subsequent member address: %v", saved)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				request(memberChange{ID: 5, Action: "remove"})
				data, err := leader.snapshotData([]byte("state before removal"), index)
				if err != nil {
					t.Fatal(err)
				}
				envelope, err := decodeSnapshot(data)
				if err != nil || len(envelope.Retired) != 0 {
					t.Fatalf("older graph snapshot contains a future retirement: %v, %v", envelope.Retired, err)
				}
				data, err = leader.snapshotData([]byte("state after removal"), leader.raft.Status().Commit)
				if err != nil {
					t.Fatal(err)
				}
				envelope, err = decodeSnapshot(data)
				if err != nil || len(envelope.Retired) != 1 || envelope.Retired[0] != 5 {
					t.Fatalf("current graph snapshot lost a retirement: %v, %v", envelope.Retired, err)
				}
				return
			}
			beforeAdd := conf()
			request(memberChange{ID: 4, URL: peers[4], Action: "add_learner"})
			var delayed memberChange
			var expected raftpb.ConfState
			if action == "add_learner" {
				delayed = memberChange{ID: 4, URL: "http://127.0.0.1:1", Action: action}
				expected = beforeAdd
			} else {
				wait("learner caught up", func() bool {
					status := leader.raft.Status()
					return status.Progress[4].Match >= status.Commit && nodes[3].applied.Load() >= status.Commit
				})
				request(memberChange{ID: 4, Action: "promote"})
				expected = conf()
				var removeIDs []uint64
				for id := uint64(1); id <= 4; id++ {
					if id != leader.ID() {
						removeIDs = append(removeIDs, id)
					}
				}
				request(memberChange{ID: removeIDs[0], Action: "remove"})
				delayed = memberChange{ID: removeIDs[1], Action: action}
			}
			// A concurrent handler can pause after admission, then submit after
			// another handler's configuration change has already been applied.
			proposal := memberProposal{memberChange: delayed, RequestID: "delayed-change", Expected: &expected}
			if scenario.legacy {
				proposal.Expected = nil
			}
			data, _ := json.Marshal(proposal)
			outcomes := make(chan result, 1)
			leader.mu.Lock()
			leader.proposals[proposal.RequestID] = outcomes
			leader.mu.Unlock()
			kind := raftpb.ConfChangeAddLearnerNode
			if action == "remove" {
				kind = raftpb.ConfChangeRemoveNode
			}
			committed := leader.raft.Status().Commit
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := leader.raft.ProposeConfChange(ctx, raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{NodeID: delayed.ID, Type: kind}}, Context: data}); err != nil {
				t.Fatal(err)
			}
			wait("delayed change applied", func() bool { return leader.applied.Load() > committed })
			select {
			case outcome := <-outcomes:
				if !errors.Is(outcome.err, errMembershipConflict) {
					t.Fatalf("stale membership acknowledged: %v", outcome.err)
				}
			case <-ctx.Done():
				t.Fatal("missing membership rejection result")
			}
			leader.mu.Lock()
			delete(leader.proposals, proposal.RequestID)
			leader.mu.Unlock()
			verify := func() {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if got := conf(); len(got.Voters) != 3 {
					t.Fatalf("delayed removal left %d voters: %v", len(got.Voters), got.Voters)
				}
				leader.peerMu.RLock()
				origin := leader.peers[4]
				leader.peerMu.RUnlock()
				if origin != peers[4] {
					t.Fatalf("delayed duplicate admission replaced peer address: %s", origin)
				}
				if data, err := leader.Propose(ctx, []byte("still available")); err != nil || string(data) != "still available" {
					t.Fatalf("proposal after rejected membership: %q, %v", data, err)
				}
			}
			verify()
			checkpoint := leader.applied.Load()
			for i, node := range nodes {
				endpoints[i].Store(nil)
				if err := node.Close(); err != nil {
					t.Fatal(err)
				}
				nodes[i] = nil
			}
			for i := range nodes {
				start(i)
			}
			wait("leader after restart", func() bool {
				for _, node := range nodes {
					if node.LeaderID() == node.ID() && node.applied.Load() >= checkpoint {
						leader = node
						return true
					}
				}
				return false
			})
			verify()
			if action == "add_learner" {
				wait("learner remains reachable", func() bool {
					status := leader.raft.Status()
					return status.Progress[4].Match >= status.Commit && nodes[3].applied.Load() >= status.Commit
				})
				request(memberChange{ID: 4, Action: "promote"})
				request(memberChange{ID: 4, Action: "remove"})
			}
		})
	}
}
