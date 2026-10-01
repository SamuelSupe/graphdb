package replication

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var ErrNotLeader = errors.New("Raft node is not the leader")
var ErrUnavailable = errors.New("Raft node is unavailable")
var ErrSnapshotTooLarge = errors.New("replication snapshot exceeds configured byte budget")

type StateMachine interface {
	Applied() (uint64, error)
	Apply(context.Context, uint64, []byte) ([]byte, error)
	Snapshot(context.Context) ([]byte, error)
	Restore(context.Context, uint64, []byte) error
}

type Config struct {
	ID               uint64
	ClusterID        string
	Dir              string
	Peers            map[uint64]string
	Bootstrap        bool
	Token            string
	Tick             time.Duration
	SnapshotEntries  uint64
	MaxSnapshotBytes int64
}

type proposal struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
}

type result struct {
	data []byte
	err  error
}

type applicationBatch struct {
	configurationChanged bool
	snapshot             raftpb.Snapshot
	first, last          uint64
	conf                 raftpb.ConfState
}

type Node struct {
	cfg              Config
	raft             raft.Node
	disk             *diskStorage
	machine          StateMachine
	ctx              context.Context
	cancel           context.CancelFunc
	workers          sync.WaitGroup
	applied          atomic.Uint64
	applicationBytes atomic.Int64
	proposalBytes    atomic.Int64
	leader           atomic.Uint64
	mu               sync.Mutex
	changed          chan struct{}
	failure          error
	snapshotFailure  error
	proposals        map[string]chan result
	reads            map[string]chan uint64
	peerMu           sync.RWMutex
	peers            map[uint64]string
	senders          map[uint64]chan packet
	client           transportClient
	application      chan applicationBatch
	snapshotRequests chan snapshotRequest
}

type snapshotRequest struct {
	index uint64
	data  []byte
	conf  raftpb.ConfState
	done  chan error
}

func Open(parent context.Context, cfg Config, machine StateMachine) (*Node, error) {
	if cfg.ID == 0 || cfg.ClusterID == "" || cfg.Dir == "" || len(cfg.Token) < 32 {
		return nil, fmt.Errorf("Raft requires node ID, cluster ID, directory and a token of at least 32 bytes")
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 100 * time.Millisecond
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = 1000
	}
	if cfg.MaxSnapshotBytes <= 0 {
		cfg.MaxSnapshotBytes = 512 << 20
	}
	disk, existing, err := openStorage(cfg.Dir, cfg.ID, cfg.ClusterID)
	if err != nil {
		return nil, err
	}
	applied, err := machine.Applied()
	if err != nil {
		disk.db.Close()
		return nil, err
	}
	if !existing && applied > 0 {
		disk.db.Close()
		return nil, fmt.Errorf("application checkpoint exists without its Raft log; replace this replica using a fresh node ID and empty directories")
	}
	snapshot, err := disk.Snapshot()
	if err != nil {
		disk.db.Close()
		return nil, err
	}
	var snapshotPeers map[uint64]string
	var snapshotRetired []uint64
	if snapshot.Metadata.Index > 0 {
		envelope, decodeErr := decodeSnapshot(snapshot.Data)
		if decodeErr != nil {
			disk.db.Close()
			return nil, decodeErr
		}
		snapshotPeers = envelope.Peers
		snapshotRetired = envelope.Retired
		snapshot.Data = envelope.State
	}
	if snapshot.Metadata.Index > applied {
		if err := machine.Restore(parent, snapshot.Metadata.Index, snapshot.Data); err != nil {
			disk.db.Close()
			return nil, err
		}
		applied = snapshot.Metadata.Index
	}
	hard, _, err := disk.InitialState()
	if err != nil {
		disk.db.Close()
		return nil, err
	}
	if existing && applied > hard.Commit {
		disk.db.Close()
		return nil, fmt.Errorf("application checkpoint %d exceeds durable Raft commit %d", applied, hard.Commit)
	}
	ctx, cancel := context.WithCancel(parent)
	n := &Node{cfg: cfg, disk: disk, machine: machine, ctx: ctx, cancel: cancel, changed: make(chan struct{}), proposals: make(map[string]chan result), reads: make(map[string]chan uint64), peers: make(map[uint64]string), senders: make(map[uint64]chan packet), application: make(chan applicationBatch), snapshotRequests: make(chan snapshotRequest)}
	n.applied.Store(applied)
	for id, address := range cfg.Peers {
		n.peers[id] = address
	}
	for id, address := range snapshotPeers {
		n.peers[id] = address
	}
	if err := n.loadPeers(); err != nil {
		cancel()
		disk.db.Close()
		return nil, err
	}
	if err := n.installRetired(snapshotRetired); err != nil {
		cancel()
		disk.db.Close()
		return nil, err
	}
	n.client = newTransportClient()
	rc := &raft.Config{ID: cfg.ID, ElectionTick: 10, HeartbeatTick: 1, Storage: disk, Applied: applied, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 4 << 20, MaxInflightMsgs: 64, MaxUncommittedEntriesSize: 64 << 20, CheckQuorum: true, PreVote: true, DisableProposalForwarding: true, ReadOnlyOption: raft.ReadOnlySafe, StepDownOnRemoval: true}
	if existing || !cfg.Bootstrap {
		n.raft = raft.RestartNode(rc)
	} else {
		var peers []raft.Peer
		if cfg.Bootstrap {
			if len(cfg.Peers) != 3 {
				cancel()
				disk.db.Close()
				return nil, fmt.Errorf("bootstrap requires exactly three voting nodes")
			}
			for id := range cfg.Peers {
				peers = append(peers, raft.Peer{ID: id})
			}
		}
		sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
		n.raft = raft.StartNode(rc, peers)
	}
	n.workers.Add(3)
	go n.run()
	go n.applyLoop()
	go func() {
		defer n.workers.Done()
		ticker := time.NewTicker(cfg.Tick)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n.raft.Tick()
			case <-ctx.Done():
				return
			}
		}
	}()
	return n, nil
}

func randomID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func (n *Node) Propose(ctx context.Context, data []byte) ([]byte, error) {
	if err := n.available(true); err != nil {
		return nil, err
	}
	n.mu.Lock()
	snapshotFailure := n.snapshotFailure
	n.mu.Unlock()
	if snapshotFailure != nil {
		return nil, fmt.Errorf("%w: %v; increase the snapshot budget before resuming mutations", ErrUnavailable, snapshotFailure)
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(proposal{ID: id, Data: data})
	if err != nil {
		return nil, err
	}
	if len(payload) > 32<<20 {
		return nil, fmt.Errorf("Raft proposal exceeds 32 MiB")
	}
	bytes := int64(len(payload))
	if n.proposalBytes.Add(bytes) > 64<<20 {
		n.proposalBytes.Add(-bytes)
		return nil, fmt.Errorf("%w: pending proposals exceed 64 MiB", ErrUnavailable)
	}
	defer n.proposalBytes.Add(-bytes)
	ch := make(chan result, 1)
	n.mu.Lock()
	n.proposals[id] = ch
	n.mu.Unlock()
	defer func() { n.mu.Lock(); delete(n.proposals, id); n.mu.Unlock() }()
	if err := n.raft.Propose(ctx, payload); err != nil {
		return nil, err
	}
	for {
		n.mu.Lock()
		changed := n.changed
		n.mu.Unlock()
		if err := n.available(true); err != nil {
			return nil, err
		}
		select {
		case result := <-ch:
			return result.data, result.err
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-n.ctx.Done():
			return nil, n.available(false)
		}
	}
}

func (n *Node) ReadBarrier(ctx context.Context) error {
	if err := n.available(true); err != nil {
		return err
	}
	id, err := randomID()
	if err != nil {
		return err
	}
	ch := make(chan uint64, 1)
	n.mu.Lock()
	n.reads[id] = ch
	n.mu.Unlock()
	defer func() { n.mu.Lock(); delete(n.reads, id); n.mu.Unlock() }()
	if err := n.raft.ReadIndex(ctx, []byte(id)); err != nil {
		return err
	}
	var index uint64
	for index == 0 {
		n.mu.Lock()
		changed := n.changed
		n.mu.Unlock()
		if err := n.available(true); err != nil {
			return err
		}
		select {
		case index = <-ch:
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return n.available(false)
		}
	}
	for {
		n.mu.Lock()
		changed := n.changed
		n.mu.Unlock()
		if n.applied.Load() >= index {
			return n.available(true)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return n.available(false)
		}
	}
}

func (n *Node) available(leader bool) error {
	n.mu.Lock()
	failure := n.failure
	n.mu.Unlock()
	if failure != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, failure)
	}
	if n.ctx.Err() != nil {
		return ErrUnavailable
	}
	if leader && n.raft != nil && (n.raft.Status().Commit-n.applied.Load() > 1024 || n.applicationBytes.Load() > 64<<20) {
		return fmt.Errorf("%w: application backlog exceeds 1024 entries or 64 MiB", ErrUnavailable)
	}
	if leader && n.leader.Load() != n.cfg.ID {
		return ErrNotLeader
	}
	return nil
}

func (n *Node) fail(err error) {
	n.mu.Lock()
	if n.failure == nil {
		n.failure = err
	}
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
	n.cancel()
}

func (n *Node) run() {
	defer n.workers.Done()
	defer n.raft.Stop()
	var pending applicationBatch
	for {
		var application chan applicationBatch
		var next applicationBatch
		if pending.last > 0 || !raft.IsEmptySnap(pending.snapshot) {
			application = n.application
			next = pending
		}
		select {
		case application <- next:
			pending = applicationBatch{}
		case <-n.ctx.Done():
			return
		case request := <-n.snapshotRequests:
			snapshot, err := n.disk.CreateSnapshot(request.index, &request.conf, request.data)
			if err == nil {
				err = n.disk.saveSnapshot(snapshot, request.index)
			}
			request.done <- err
			if errors.Is(err, raft.ErrSnapOutOfDate) {
				continue
			}
			if err != nil {
				n.fail(err)
				return
			}
		case ready := <-n.raft.Ready():
			if err := n.disk.save(ready); err != nil {
				n.fail(err)
				return
			}
			if ready.SoftState != nil {
				n.leader.Store(ready.SoftState.Lead)
				n.signalChanged()
			}
			for _, state := range ready.ReadStates {
				n.mu.Lock()
				ch := n.reads[string(state.RequestCtx)]
				n.mu.Unlock()
				if ch != nil {
					select {
					case ch <- state.Index:
					default:
					}
				}
			}
			conf := n.disk.conf
			confIndex := n.disk.confIndex
			configurationChanged := false
			for _, entry := range ready.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryConfChange:
					if entry.Index <= confIndex {
						continue
					}
					var change raftpb.ConfChange
					if err := change.Unmarshal(entry.Data); err != nil {
						n.fail(err)
						return
					}
					conf = *n.raft.ApplyConfChange(change)
					confIndex = entry.Index
				case raftpb.EntryConfChangeV2:
					configurationChanged = true
					// Membership is durable independently of the graph checkpoint.
					// Replaying an older add/promote/remove against a later saved
					// configuration can resurrect a member or demote a voter.
					if entry.Index <= confIndex {
						continue
					}
					var change raftpb.ConfChangeV2
					if err := change.Unmarshal(entry.Data); err != nil {
						n.fail(err)
						return
					}
					conf = *n.raft.ApplyConfChange(change)
					confIndex = entry.Index
					if len(change.Context) > 0 {
						if err := n.applyPeerChange(change.Context); err != nil {
							n.fail(err)
							return
						}
					}
				}
			}
			if confIndex != n.disk.confIndex {
				if err := n.disk.saveConf(conf, confIndex); err != nil {
					n.fail(err)
					return
				}
			}
			n.disk.conf = conf
			n.disk.confIndex = confIndex
			if len(ready.Messages) > 0 {
				n.send(ready.Messages)
			}
			if len(ready.CommittedEntries) > 0 || !raft.IsEmptySnap(ready.Snapshot) {
				// Only retain positions in the durable log. A slow application
				// cannot grow an in-memory queue or stop persisted Raft heartbeats.
				if !raft.IsEmptySnap(ready.Snapshot) {
					pending = applicationBatch{snapshot: ready.Snapshot}
				}
				if len(ready.CommittedEntries) > 0 {
					if pending.first == 0 {
						pending.first = ready.CommittedEntries[0].Index
					}
					pending.last = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
				}
				pending.conf = conf
				pending.configurationChanged = pending.configurationChanged || configurationChanged
			}
			n.raft.Advance()
		}
	}
}

func (n *Node) applyLoop() {
	defer n.workers.Done()
	lastSnapshot, _ := n.disk.Snapshot()
	snapshotIndex := lastSnapshot.Metadata.Index
	for {
		select {
		case <-n.ctx.Done():
			return
		case batch := <-n.application:
			if !raft.IsEmptySnap(batch.snapshot) && batch.snapshot.Metadata.Index > n.applied.Load() {
				envelope, err := decodeSnapshot(batch.snapshot.Data)
				if err != nil {
					n.fail(err)
					return
				}
				if err := n.installRetired(envelope.Retired); err != nil {
					n.fail(err)
					return
				}
				if err := n.installPeers(envelope.Peers); err != nil {
					n.fail(err)
					return
				}
				if err := n.machine.Restore(n.ctx, batch.snapshot.Metadata.Index, envelope.State); err != nil {
					n.fail(err)
					return
				}
				n.progress(batch.snapshot.Metadata.Index)
				snapshotIndex = batch.snapshot.Metadata.Index
			}
			for first := max(batch.first, n.applied.Load()+1); first <= batch.last; {
				entries, err := n.disk.Entries(first, batch.last+1, 4<<20)
				if errors.Is(err, raft.ErrCompacted) {
					// A received snapshot superseded this range while applying it.
					// The run loop will deliver that snapshot next.
					break
				}
				if err != nil {
					n.fail(err)
					return
				}
				for _, entry := range entries {
					n.applicationBytes.Add(int64(len(entry.Data)))
				}
				for _, entry := range entries {
					var command proposal
					if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
						if err := json.Unmarshal(entry.Data, &command); err != nil {
							n.fail(err)
							return
						}
					}
					data, err := n.machine.Apply(n.ctx, entry.Index, command.Data)
					if err != nil {
						n.fail(fmt.Errorf("apply Raft entry %d: %w", entry.Index, err))
						return
					}
					n.progress(entry.Index)
					n.applicationBytes.Add(-int64(len(entry.Data)))
					n.mu.Lock()
					ch := n.proposals[command.ID]
					n.mu.Unlock()
					if ch != nil {
						ch <- result{data: data}
					}
					first = entry.Index + 1
				}
			}
			if n.applied.Load()-snapshotIndex >= n.cfg.SnapshotEntries || batch.configurationChanged {
				data, err := n.machine.Snapshot(n.ctx)
				if err == nil && int64(len(data)) > n.cfg.MaxSnapshotBytes {
					err = ErrSnapshotTooLarge
				}
				if err == nil {
					data, err = n.snapshotData(data)
				}
				if err == nil && int64(len(data)) > n.cfg.MaxSnapshotBytes {
					err = ErrSnapshotTooLarge
				}
				if errors.Is(err, ErrSnapshotTooLarge) {
					n.mu.Lock()
					n.snapshotFailure = err
					n.mu.Unlock()
					snapshotIndex = n.applied.Load()
					continue
				}
				if err != nil {
					n.fail(err)
					return
				}
				n.mu.Lock()
				n.snapshotFailure = nil
				n.mu.Unlock()
				request := snapshotRequest{index: n.applied.Load(), data: data, conf: batch.conf, done: make(chan error, 1)}
				select {
				case n.snapshotRequests <- request:
				case <-n.ctx.Done():
					return
				}
				select {
				case err = <-request.done:
				case <-n.ctx.Done():
					return
				}
				if errors.Is(err, raft.ErrSnapOutOfDate) {
					continue
				}
				if err != nil {
					n.fail(err)
					return
				}
				snapshotIndex = request.index
			}
		}
	}
}

func (n *Node) progress(index uint64) {
	n.applied.Store(index)
	n.signalChanged()
}

func (n *Node) signalChanged() {
	n.mu.Lock()
	close(n.changed)
	n.changed = make(chan struct{})
	n.mu.Unlock()
}

func (n *Node) Close() error {
	n.cancel()
	n.workers.Wait()
	n.client.CloseIdleConnections()
	return n.disk.db.Close()
}

func (n *Node) Status() map[string]any {
	n.mu.Lock()
	failure := n.failure
	snapshotFailure := n.snapshotFailure
	n.mu.Unlock()
	raftStatus := n.raft.Status()
	applied := n.applied.Load()
	status := map[string]any{"node_id": n.cfg.ID, "leader_id": n.leader.Load(), "term": raftStatus.Term, "commit_index": raftStatus.Commit, "applied_index": applied, "application_lag": raftStatus.Commit - min(raftStatus.Commit, applied), "application_bytes": n.applicationBytes.Load(), "proposal_bytes": n.proposalBytes.Load(), "ready": failure == nil && n.ctx.Err() == nil && n.leader.Load() == n.cfg.ID}
	if failure != nil {
		status["error"] = failure.Error()
	}
	if snapshotFailure != nil {
		status["snapshot_error"] = snapshotFailure.Error()
	}
	return status
}
