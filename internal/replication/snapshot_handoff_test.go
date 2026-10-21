package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPersistentSnapshotFailureHandsOffOnlyToHealthyReplica(t *testing.T) {
	for _, allFailed := range []bool{false, true} {
		t.Run(fmt.Sprintf("all-failed=%v", allFailed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			var endpoints [3]atomic.Pointer[Node]
			var nodes [3]*Node
			var machines [3]retrySnapshotMachine
			peers := map[uint64]string{}
			for i := range nodes {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if n := endpoints[i].Load(); n != nil {
						n.Handler().ServeHTTP(w, r)
					} else {
						w.WriteHeader(503)
					}
				}))
				defer server.Close()
				peers[uint64(i+1)] = server.URL
			}
			defer func() {
				for i, n := range nodes {
					if n != nil {
						n.Close()
						if machines[i].opened.Load() != machines[i].closed.Load() {
							t.Error("snapshot source retained after close")
						}
						temporary, _ := filepath.Glob(filepath.Join(n.cfg.Dir, "snapshots", ".build-*"))
						if len(temporary) != 0 {
							t.Error("snapshot temporary file retained")
						}
					}
				}
			}()
			for i := range nodes {
				n, err := Open(ctx, Config{ID: uint64(i + 1), ClusterID: "snapshot-handoff", Dir: t.TempDir(), Peers: peers, Token: strings.Repeat("t", 32), Bootstrap: true, Tick: 100 * time.Millisecond, ElectionTicks: 30, SnapshotEntries: 1}, &machines[i])
				if err != nil {
					t.Fatal(err)
				}
				nodes[i] = n
				endpoints[i].Store(n)
			}
			wait := func(ready func() bool) {
				t.Helper()
				for !ready() {
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			var leader *Node
			wait(func() bool {
				for _, n := range nodes {
					if n.LeaderID() == n.ID() && n.snapshotIndex.Load() >= 4 {
						leader = n
						return true
					}
				}
				return false
			})
			for i := range machines {
				if allFailed || nodes[i] == leader {
					machines[i].failCapture.Store(true)
				}
			}
			if _, err := leader.Propose(ctx, []byte("trigger snapshot failure")); err != nil {
				t.Fatal(err)
			}
			wait(func() bool { return leader.Status()["snapshot_error"] != nil })
			if _, err := leader.Propose(ctx, []byte("must reject")); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("failed snapshot accepted writes: %v", err)
			}
			if allFailed {
				select {
				case <-time.After(32 * time.Second):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if leader.LeaderID() != leader.ID() {
					t.Fatal("shared snapshot failure caused leadership churn")
				}
				if err := leader.ReadBarrier(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				wait(func() bool {
					id := leader.LeaderID()
					return id > 0 && id != leader.ID() && nodes[id-1].LeaderID() == id
				})
				target := nodes[leader.LeaderID()-1]
				if _, err := target.Propose(ctx, []byte("healthy leader resumes writes")); err != nil {
					t.Fatal(err)
				}
			}
			for _, n := range nodes {
				if n.ctx.Err() != nil {
					t.Fatal("snapshot preparation killed a replica")
				}
			}
		})
	}
}

type snapshotHandoffRaft struct {
	raft.Node
	node *Node
}

func (r snapshotHandoffRaft) Status() raft.Status {
	return raft.Status{BasicStatus: raft.BasicStatus{HardState: raftpb.HardState{Term: 3, Commit: 8}, SoftState: raft.SoftState{Lead: r.node.LeaderID()}}, Config: tracker.Config{Voters: quorum.JointConfig{quorum.MajorityConfig{1: {}, 2: {}, 3: {}}}}, Progress: map[uint64]tracker.Progress{2: {Match: 8, RecentActive: true}}}
}
func (r snapshotHandoffRaft) TransferLeadership(_ context.Context, _, target uint64) {
	r.node.leader.Store(target)
}

func TestSnapshotHandoffUsesHealthySnapshotAndRechecksRecovery(t *testing.T) {
	for _, scenario := range []string{"healthy", "stale-snapshot", "snapshot-error", "lagging", "recovered"} {
		t.Run(scenario, func(t *testing.T) {
			n := &Node{cfg: Config{ID: 1, ClusterID: "handoff"}, ctx: context.Background(), peers: map[uint64]string{}, client: newTransportClient()}
			n.raft = snapshotHandoffRaft{node: n}
			n.leader.Store(1)
			n.recordSnapshotFailure(errors.New("disk snapshot failed"), 6)
			since := n.snapshotFailedSince
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				peer := PeerStatus{ID: 2, ClusterID: "handoff", LeaderID: n.LeaderID(), Commit: 8, Applied: 8, SnapshotIndex: 6, Protocol: 1, Ready: n.LeaderID() == 2}
				switch scenario {
				case "stale-snapshot":
					peer.SnapshotIndex = 5
				case "snapshot-error":
					peer.SnapshotError = "shared capacity exhausted"
				case "lagging":
					peer.Applied = 7
				case "recovered":
					n.recordSnapshotFailure(nil, 0)
				}
				w.Header().Set(protocolHeader, "1")
				json.NewEncoder(w).Encode(peer)
			}))
			defer server.Close()
			defer n.client.CloseIdleConnections()
			n.peers[2] = server.URL
			n.handoffSnapshotLeadership(since.Add(29 * time.Second))
			if n.LeaderID() != 1 {
				t.Fatal("transient failure triggered handoff")
			}
			n.handoffSnapshotLeadership(since.Add(31 * time.Second))
			want := uint64(1)
			if scenario == "healthy" {
				want = 2
			}
			if n.LeaderID() != want {
				t.Fatalf("handoff leader=%d want %d", n.LeaderID(), want)
			}
		})
	}
}
