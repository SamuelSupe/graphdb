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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

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
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	s, existing, err = openStorage(dir, 1, "test")
	if err != nil || !existing {
		t.Fatalf("reopen: %v, %v", existing, err)
	}
	defer s.db.Close()
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
	disk.db.Close()
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
