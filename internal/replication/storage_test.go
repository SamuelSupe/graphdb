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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type upgradeStatusRaft struct {
	raft.Node
	commit *atomic.Uint64
}

func (r upgradeStatusRaft) Status() raft.Status {
	return raft.Status{
		BasicStatus: raft.BasicStatus{HardState: raftpb.HardState{Commit: r.commit.Load()}, SoftState: raft.SoftState{Lead: 2}},
		Config:      tracker.Config{Voters: quorum.JointConfig{quorum.MajorityConfig{1: {}, 2: {}, 3: {}}}},
	}
}

func TestUpgradeStatusPinsCommitBeforeSamplingFollowers(t *testing.T) {
	for _, lagging := range []bool{false, true} {
		t.Run(fmt.Sprint("lagging=", lagging), func(t *testing.T) {
			var commit atomic.Uint64
			commit.Store(100)
			node := &Node{cfg: Config{ID: 3, ClusterID: "upgrade"}, ctx: context.Background(), raft: upgradeStatusRaft{commit: &commit}, peers: map[uint64]string{}, client: newTransportClient()}
			node.leader.Store(2)
			node.applied.Store(100)
			for _, id := range []uint64{1, 2} {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					position := commit.Load()
					applied := position
					if id == 1 && lagging {
						applied--
					}
					w.Header().Set(protocolHeader, "1")
					json.NewEncoder(w).Encode(PeerStatus{ID: id, ClusterID: "upgrade", LeaderID: 2, Commit: position, Applied: applied, Protocol: 1, Ready: id == 2})
					if id == 1 {
						// A write commits after this follower's response is sampled.
						node.applied.Store(commit.Add(1))
					}
				}))
				t.Cleanup(server.Close)
				node.peers[id] = server.URL
			}
			defer node.client.CloseIdleConnections()
			report := node.UpgradeStatus(context.Background())
			if report.Safe != !lagging || report.CatchingUp != lagging {
				t.Fatalf("upgrade preflight under continuing writes: %+v", report)
			}
		})
	}
}

func TestQuorumReadsContinueWhileAppendTransportIsBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var endpoints [3]atomic.Pointer[Node]
	var nodes [3]*Node
	var machines [3]membershipMachine
	var blocked atomic.Bool
	var entered [3]chan struct{}
	var once [3]sync.Once
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	peers := map[uint64]string{}
	for i := range endpoints {
		i := i
		entered[i] = make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/raft/message" {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(data))
				var message raftpb.Message
				if message.Unmarshal(data) == nil && message.Type == raftpb.MsgApp && len(message.Entries) > 0 && blocked.Load() {
					once[i].Do(func() { close(entered[i]) })
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
			}
			if node := endpoints[i].Load(); node != nil {
				node.Handler().ServeHTTP(w, r)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}))
		t.Cleanup(server.Close)
		peers[uint64(i+1)] = server.URL
	}
	t.Cleanup(func() {
		resume()
		for i, node := range nodes {
			endpoints[i].Store(nil)
			if node != nil {
				node.Close()
			}
		}
	})
	for i := range nodes {
		var err error
		nodes[i], err = Open(ctx, Config{ID: uint64(i + 1), ClusterID: "transport-isolation", Dir: t.TempDir(), Peers: peers, Bootstrap: true, ElectionTicks: 10, Token: strings.Repeat("t", 32), Tick: 50 * time.Millisecond, SnapshotEntries: 1000}, &machines[i])
		if err != nil {
			t.Fatal(err)
		}
		endpoints[i].Store(nodes[i])
	}
	var leader *Node
	for leader == nil {
		for _, node := range nodes {
			read, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			err := node.ReadBarrier(read)
			stop()
			if err == nil {
				leader = node
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	term := leader.raft.Status().Term
	blocked.Store(true)
	proposed := make(chan error, 1)
	go func() { _, err := leader.Propose(ctx, []byte("slow append")); proposed <- err }()
	for i, node := range nodes {
		if node == leader {
			continue
		}
		select {
		case <-entered[i]:
		case <-ctx.Done():
			t.Fatal("append did not block")
		}
	}
	// Hold both replication requests beyond the randomized election timeout.
	// The previously committed state must remain readable through ReadIndex.
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		read, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		err := leader.ReadBarrier(read)
		stop()
		if err != nil {
			t.Fatalf("read blocked behind append transport: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader.raft.Status().Term != term {
		t.Fatal("slow append caused an election despite healthy heartbeat transport")
	}
	resume()
	select {
	case err := <-proposed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("write did not finish after transport resumed")
	}
	// A stalled durable log blocks Raft's status channel as well as proposals.
	// Diagnostic HTTP traffic must still be served without bypassing ReadIndex.
	leader.disk.mu.Lock()
	unlock := sync.OnceFunc(leader.disk.mu.Unlock)
	defer unlock()
	data, err := json.Marshal(proposal{ID: "disk-stall", Data: []byte("blocked")})
	if err != nil {
		t.Fatal(err)
	}
	go leader.raft.Propose(ctx, data)
	deadline := time.Now().Add(2 * time.Second)
	for {
		status := make(chan raft.Status, 1)
		go func() { status <- leader.raft.Status() }()
		select {
		case <-status:
			if time.Now().After(deadline) {
				t.Fatal("durable storage did not block the Raft control loop")
			}
			time.Sleep(time.Millisecond)
		case <-time.After(50 * time.Millisecond):
			goto blockedStorage
		}
	}
blockedStorage:
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leader.WriteMetrics(w)
	}))
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(server.URL)
	if err != nil {
		unlock()
		t.Fatal("diagnostics blocked behind persistence: ", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal("diagnostics did not remain available")
	}
	read, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	err = leader.ReadBarrier(read)
	stop()
	if err == nil {
		t.Fatal("blocked persistence unexpectedly bypassed the strong-read barrier")
	}
	unlock()
}

func TestSnapshotCleanupFailurePreservesQuorumAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	var endpoints [3]atomic.Pointer[Node]
	var nodes [3]*Node
	var machines [3]membershipMachine
	var configs [3]Config
	peers := map[uint64]string{}
	for i := range endpoints {
		i := i
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if node := endpoints[i].Load(); node != nil {
				node.Handler().ServeHTTP(w, r)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
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
	for i := range nodes {
		configs[i] = Config{ID: uint64(i + 1), ClusterID: "cleanup", Dir: t.TempDir(), Peers: peers, Bootstrap: true, ElectionTicks: 10, Tick: 50 * time.Millisecond, Token: strings.Repeat("t", 32), SnapshotEntries: 5}
		var err error
		nodes[i], err = Open(ctx, configs[i], &machines[i])
		if err != nil {
			t.Fatal(err)
		}
		endpoints[i].Store(nodes[i])
	}
	leader := func() *Node {
		for ctx.Err() == nil {
			for _, node := range nodes {
				read, stop := context.WithTimeout(ctx, 100*time.Millisecond)
				err := node.ReadBarrier(read)
				stop()
				if err == nil {
					return node
				}
			}
		}
		t.Fatal("no quorum leader: ", ctx.Err())
		return nil
	}
	firstLeader := leader()
	// A nonempty orphan directory makes Remove fail, including when run as root.
	for _, cfg := range configs {
		orphan := filepath.Join(cfg.Dir, "snapshots", "00000000000000000000-orphan.snap")
		if err := os.MkdirAll(orphan, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(orphan, "unremovable"), []byte("orphan"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 15; i++ {
		write, stop := context.WithTimeout(ctx, time.Second)
		_, err := firstLeader.Propose(write, []byte("cleanup remains nonfatal"))
		stop()
		if err != nil {
			t.Fatal("snapshot cleanup interrupted writes: ", err)
		}
	}
	for _, node := range nodes {
		for node.Status()["snapshot_cleanup_error"] == nil {
			if ctx.Err() != nil {
				t.Fatal("cleanup failure was not diagnosed")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := firstLeader.ReadBarrier(ctx); err != nil {
		t.Fatal("cleanup interrupted strong reads: ", err)
	}
	// Existing durable data must reopen even if the orphan cannot be deleted.
	for i, node := range nodes {
		endpoints[i].Store(nil)
		if err := node.Close(); err != nil {
			t.Fatal(err)
		}
		nodes[i] = nil
	}
	for i := range nodes {
		configs[i].Bootstrap = false
		orphan := filepath.Join(configs[i].Dir, "snapshots", ".snapshot-crash-orphan")
		if err := os.Mkdir(orphan, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(orphan, "unremovable"), []byte("orphan"), 0600); err != nil {
			t.Fatal(err)
		}
		var err error
		nodes[i], err = Open(ctx, configs[i], &machines[i])
		if err != nil {
			t.Fatal("cleanup prevented restart: ", err)
		}
		endpoints[i].Store(nodes[i])
		if err := os.RemoveAll(filepath.Join(configs[i].Dir, "snapshots", "00000000000000000000-orphan.snap")); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(orphan, "unremovable")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := leader().Propose(ctx, []byte("after restart")); err != nil {
		t.Fatal(err)
	}
	for i, node := range nodes {
		for node.Status()["snapshot_cleanup_error"] != nil {
			if ctx.Err() != nil {
				t.Fatal("cleanup retry did not clear diagnostic")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err := os.Stat(filepath.Join(configs[i].Dir, "snapshots", ".snapshot-crash-orphan")); !os.IsNotExist(err) {
			t.Fatal("startup orphan was not reclaimed: ", err)
		}
	}
}

func TestSnapshotCleanupPreservesRestoreFilesAndReusedOrphanNames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{ctx: ctx, cfg: Config{Dir: t.TempDir()}, snapshotCleanupRequests: make(chan uint64, 1)}
	t.Cleanup(func() { cancel(); node.workers.Wait() })
	dir := filepath.Join(node.cfg.Dir, "snapshots")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	older := filepath.Join(dir, "00000000000000000005-old.snap")
	obsolete := filepath.Join(dir, "00000000000000000002-old.snap")
	latestFile := snapshotFile{SHA256: strings.Repeat("a", 64), Bytes: 32}
	latest := snapshotPath(node.cfg.Dir, 10, latestFile)
	for _, name := range []string{obsolete, older, latest} {
		if err := os.WriteFile(name, []byte("snapshot"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	orphan, err := os.Lstat(latest)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(dir, ".new-snapshot")
	if err := os.WriteFile(replacement, []byte("new snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, latest); err != nil {
		t.Fatal(err)
	}
	node.workers.Add(1)
	go node.snapshotCleanupLoop([]os.FileInfo{orphan})
	node.applied.Store(4)
	node.scheduleSnapshotCleanup(10)
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(obsolete); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cleanup did not reclaim the already applied file")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(older); err != nil {
		t.Fatal("unapplied restore file was removed: ", err)
	}
	node.applied.Store(10)
	node.scheduleSnapshotCleanup(10)
	deadline = time.Now().Add(time.Second)
	for {
		_, err := os.Stat(older)
		if os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("applied old snapshot was not reclaimed: ", err)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(latest); err != nil {
		t.Fatal("latest snapshot was removed: ", err)
	}
}

func TestUnsupportedReplicationProtocolDoesNotApply(t *testing.T) {
	data, err := json.Marshal(proposal{ID: "future", Data: []byte("mutate"), Protocol: MaxProtocolVersion + 1})
	if err != nil {
		t.Fatal(err)
	}
	machine := &membershipMachine{}
	node := &Node{machine: machine}
	if err := node.applyEntries([]raftpb.Entry{{Index: 1, Type: raftpb.EntryNormal, Data: data}}); err == nil {
		t.Fatal("future command protocol was applied")
	}
	if machine.applied.Load() != 0 {
		t.Fatal("unsupported command advanced the application checkpoint")
	}
	for _, protocol := range []int{0, ProtocolVersion, MaxProtocolVersion + 1} {
		data, err := json.Marshal(snapshotEnvelope{Version: 1, Protocol: protocol, State: []byte("state"), Peers: map[uint64]string{1: "http://node1:8081"}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = decodeSnapshot(data)
		if (err != nil) != (protocol > MaxProtocolVersion) {
			t.Fatalf("snapshot protocol %d: %v", protocol, err)
		}
	}
}

func TestApplicationBacklogAllowsAppliedIndexToAdvanceBetweenObservations(t *testing.T) {
	node := &Node{ctx: context.Background(), cfg: Config{ID: 1}}
	node.leader.Store(1)
	node.committed.Store(2000)
	node.raft = upgradeStatusRaft{commit: &node.committed}
	for _, applied := range []uint64{975, 976, 2001} {
		node.applied.Store(applied)
		err := node.available(true)
		if (err != nil) != (applied == 975) {
			t.Fatalf("durable commit 2000 / applied %d: %v", applied, err)
		}
	}
}

func TestDurableLogTruncationCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, existing, err := openStorage(dir, 1, "test")
	if err != nil || existing {
		t.Fatalf("open fresh: %v, %v", existing, err)
	}
	entries := []raftpb.Entry{}
	for i := uint64(1); i <= 5; i++ {
		entries = append(entries, raftpb.Entry{Index: i, Term: 1, Data: []byte("old")})
	}
	if err := s.save(raft.Ready{Entries: entries, HardState: raftpb.HardState{Term: 1, Commit: 2}}, nil); err != nil {
		t.Fatal(err)
	}
	// A new leader replaces an uncommitted suffix; it must disappear on disk.
	if err := s.save(raft.Ready{Entries: []raftpb.Entry{{Index: 3, Term: 2, Data: []byte("new")}, {Index: 4, Term: 2, Data: []byte("tail")}}, HardState: raftpb.HardState{Term: 2, Commit: 4}}, nil); err != nil {
		t.Fatal(err)
	}
	conf := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := s.saveConf(conf, 2); err != nil {
		t.Fatal(err)
	}
	s.conf, s.confIndex = conf, 2
	snapshot, err := s.CreateSnapshot(2, &conf, []byte("state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.saveSnapshot(snapshot, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, existing, err = openStorage(dir, 1, "test")
	if err != nil || !existing {
		t.Fatalf("reopen: %v, %v", existing, err)
	}
	defer s.Close()
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first != 3 || last != 4 {
		t.Fatalf("persisted range: %d..%d", first, last)
	}
	if _, err := s.Term(1); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted term: %v", err)
	}
	if term, err := s.Term(2); err != nil || term != 1 {
		t.Fatalf("snapshot term: %d, %v", term, err)
	}
	if _, err := s.Term(5); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("truncated term: %v", err)
	}
	loaded, err := s.Entries(3, 5, 0)
	if err != nil || len(loaded) != 1 || loaded[0].Term != 2 || string(loaded[0].Data) != "new" {
		t.Fatalf("bounded log read: %v, %v", loaded, err)
	}
	loaded, err = s.Entries(3, 5, ^uint64(0))
	if err != nil || len(loaded) != 2 {
		t.Fatalf("log suffix: %v, %v", loaded, err)
	}
	hard, restoredConf, err := s.InitialState()
	if err != nil || hard.Commit != 4 || len(restoredConf.Voters) != 3 {
		t.Fatalf("consensus state: %v, %v, %v", hard, restoredConf, err)
	}
}

type retrySnapshotMachine struct {
	membershipMachine
	failCapture atomic.Bool
}

func (m *retrySnapshotMachine) CaptureSnapshot(context.Context) (SnapshotSource, error) {
	if m.failCapture.Load() {
		return nil, fmt.Errorf("temporary snapshot disk failure")
	}
	return testSnapshotSource{}, nil
}
func (m *retrySnapshotMachine) RestoreSnapshot(_ context.Context, index uint64, _ io.ReadSeeker) error {
	m.applied.Store(index)
	return nil
}

type testSnapshotSource struct{}

func (testSnapshotSource) Close() error { return nil }
func (testSnapshotSource) WriteTo(_ context.Context, writer io.WriteSeeker) error {
	_, err := writer.Write([]byte(strings.Repeat("state", 16)))
	return err
}

func TestStreamingSnapshotRetriesAndRejectsCorruptDurableFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var endpoints [3]atomic.Pointer[Node]
	var legacy [3]atomic.Bool
	var nodes [3]*Node
	var machines [3]retrySnapshotMachine
	var configs [3]Config
	peers := map[uint64]string{}
	for i := range endpoints {
		i := i
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if node := endpoints[i].Load(); node != nil {
				if legacy[i].Load() && r.URL.Path == "/raft/status" {
					status := node.Status()
					status["protocol_version"] = 0
					status["protocol_max"] = 0
					json.NewEncoder(w).Encode(status)
				} else {
					node.Handler().ServeHTTP(w, r)
				}
			} else {
				w.WriteHeader(503)
			}
		}))
		defer server.Close()
		peers[uint64(i+1)] = server.URL
	}
	defer func() {
		for i, node := range nodes {
			endpoints[i].Store(nil)
			if node != nil {
				node.Close()
			}
		}
	}()
	for i := range nodes {
		machines[i].failCapture.Store(true)
		configs[i] = Config{Protocol: 2, ID: uint64(i + 1), ClusterID: "retry", Dir: t.TempDir(), Peers: peers, Token: strings.Repeat("t", 32), Bootstrap: true, ElectionTicks: 10, Tick: 100 * time.Millisecond, SnapshotEntries: 1, StreamSnapshots: true, AllowLegacyProtocol: true}
		node, err := Open(ctx, configs[i], &machines[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
		endpoints[i].Store(node)
	}
	wait := func(check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	leader := -1
	wait(func() bool {
		for i, node := range nodes {
			if node.LeaderID() == uint64(i+1) {
				leader = i
				return node.Status()["snapshot_error"] != nil
			}
		}
		return false
	})
	node := nodes[leader]
	if _, err := node.Propose(ctx, []byte("blocked")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed snapshot accepted writes: %v", err)
	}
	if err := node.ReadBarrier(ctx); err != nil {
		t.Fatalf("failed snapshot prevented reads: %v", err)
	}
	for i := range machines {
		machines[i].failCapture.Store(false)
	}
	wait(func() bool {
		snap, err := node.disk.Snapshot()
		return err == nil && snap.Metadata.Index > 0 && node.Status()["snapshot_error"] == nil
	})
	legacy[(leader+1)%3].Store(true)
	if _, err := node.peerStatus(ctx, uint64((leader+1)%3+1)); err == nil {
		t.Fatal("legacy window admitted an unversioned peer after protocol 2 configuration")
	}
	if _, err := node.Propose(ctx, []byte("premature protocol activation")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("old voter did not prevent protocol activation: %v", err)
	}
	legacy[(leader+1)%3].Store(false)
	if _, err := node.Propose(ctx, []byte("resumed")); err != nil {
		t.Fatal(err)
	}
	endpoints[leader].Store(nil)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	nodes[leader] = nil
	var replacement *Node
	wait(func() bool {
		for _, candidate := range nodes {
			if candidate != nil && candidate.LeaderID() == candidate.ID() {
				replacement = candidate
				return true
			}
		}
		return false
	})
	if _, err := replacement.Propose(ctx, []byte("activated protocol survives missing voter")); err != nil {
		t.Fatalf("activated protocol lost majority availability: %v", err)
	}
	cfg := configs[leader]
	cfg.Protocol = 1
	node, err := Open(ctx, cfg, &machines[leader])
	if err != nil {
		t.Fatal(err)
	}
	nodes[leader] = node
	if node.protocolVersion() != 2 {
		t.Fatal("reopen discarded durable protocol minimum")
	}
	node.Close()
	nodes[leader] = nil
	disk, _, err := openStorage(cfg.Dir, cfg.ID, "retry")
	if err != nil {
		t.Fatal(err)
	}
	snap, err := disk.Snapshot()
	disk.Close()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := decodeSnapshot(snap.Data)
	if err != nil || envelope.File == nil {
		t.Fatalf("stream snapshot: %v", err)
	}
	if err := os.WriteFile(snapshotPath(cfg.Dir, snap.Metadata.Index, *envelope.File), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(ctx, cfg, &machines[leader]); err == nil {
		reopened.Close()
		t.Fatal("already applied corrupt snapshot passed startup")
	}
}
