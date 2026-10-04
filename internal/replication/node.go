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

	"github.com/SamuelSupe/graphdb/v2/internal/observability"
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
	Protocol            int
	ID                  uint64
	ClusterID           string
	Dir                 string
	Peers               map[uint64]string
	Bootstrap           bool
	Token               string
	Tick                time.Duration
	ElectionTicks       int
	SnapshotEntries     uint64
	MaxSnapshotBytes    int64
	StreamSnapshots     bool
	SnapshotPreflight   func(context.Context, int64) error
	AllowLegacyProtocol bool
}

type proposal struct {
	ID       string `json:"id"`
	Data     []byte `json:"data"`
	Protocol int    `json:"protocol,omitempty"`
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
	cfg                     Config
	raft                    raft.Node
	disk                    *diskStorage
	machine                 StateMachine
	ctx                     context.Context
	cancel                  context.CancelFunc
	workers                 sync.WaitGroup
	applied                 atomic.Uint64
	committed               atomic.Uint64
	applicationBytes        atomic.Int64
	applicationCommits      atomic.Uint64
	applicationEntries      atomic.Uint64
	proposalBytes           atomic.Int64
	leader                  atomic.Uint64
	mu                      sync.Mutex
	changed                 chan struct{}
	failure                 error
	snapshotFailure         error
	snapshotCleanupFailure  error
	proposals               map[string]chan result
	reads                   map[string]chan uint64
	peerMu                  sync.RWMutex
	peers                   map[uint64]string
	senders                 map[uint64]chan packet
	controlSenders          map[uint64]chan packet
	client                  transportClient
	application             chan applicationBatch
	snapshotRequests        chan snapshotRequest
	snapshotCleanupRequests chan uint64
	maintenanceMu           sync.Mutex
	handoff                 bool
	activeProposals         int
	draining                atomic.Bool
	protocolValidated       atomic.Bool
	metrics                 *observability.OperationMetrics
	leaderChanges           atomic.Uint64
	snapshotIndex           atomic.Uint64
	snapshotBytes           atomic.Int64
	diagnostics             atomic.Pointer[diagnosticObservation]
}

type snapshotRequest struct {
	index uint64
	data  []byte
	conf  raftpb.ConfState
	done  chan error
}

func Open(parent context.Context, cfg Config, machine StateMachine) (*Node, error) {
	if cfg.Protocol == 0 {
		cfg.Protocol = ProtocolVersion
	}
	if cfg.Protocol < ProtocolVersion || cfg.Protocol > MaxProtocolVersion {
		return nil, fmt.Errorf("unsupported configured Raft protocol")
	}
	if cfg.ID == 0 || cfg.ClusterID == "" || cfg.Dir == "" || len(cfg.Token) < 32 {
		return nil, fmt.Errorf("Raft requires node ID, cluster ID, directory and a token of at least 32 bytes")
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 100 * time.Millisecond
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 30
	}
	if cfg.ElectionTicks < 2 || cfg.ElectionTicks > 1000 {
		return nil, fmt.Errorf("Raft election ticks must be between 2 and 1000")
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = 1000
	}
	if cfg.MaxSnapshotBytes <= 0 {
		cfg.MaxSnapshotBytes = 512 << 20
	}
	if cfg.MaxSnapshotBytes > 1<<40 {
		return nil, fmt.Errorf("snapshot budget cannot exceed 1 TiB")
	}
	disk, existing, err := openStorage(cfg.Dir, cfg.ID, cfg.ClusterID)
	if err != nil {
		return nil, err
	}
	minimum, err := disk.minimumProtocol()
	if err != nil || minimum > MaxProtocolVersion {
		disk.Close()
		return nil, fmt.Errorf("Raft data requires protocol %d: %v", minimum, err)
	}
	cfg.Protocol = max(cfg.Protocol, minimum)
	applied, err := machine.Applied()
	if err != nil {
		disk.Close()
		return nil, err
	}
	if !existing && applied > 0 {
		disk.Close()
		return nil, fmt.Errorf("application checkpoint exists without its Raft log; replace this replica using a fresh node ID and empty directories")
	}
	snapshot, err := disk.Snapshot()
	if err != nil {
		disk.Close()
		return nil, err
	}
	var snapshotPeers map[uint64]string
	var snapshotRetired []uint64
	if snapshot.Metadata.Index > 0 {
		envelope, decodeErr := decodeSnapshot(snapshot.Data)
		if decodeErr != nil {
			disk.Close()
			return nil, decodeErr
		}
		if envelope.File != nil {
			if err := validateSnapshotFile(cfg.Dir, snapshot.Metadata.Index, *envelope.File); err != nil {
				disk.Close()
				return nil, err
			}
		}
		snapshotPeers = envelope.Peers
		snapshotRetired = envelope.Retired
	}
	if snapshot.Metadata.Index > applied {
		if err := restoreSnapshot(parent, machine, cfg.Dir, snapshot); err != nil {
			disk.Close()
			return nil, err
		}
		applied = snapshot.Metadata.Index
	}
	cleanupOrphans, cleanupErr := pruneSnapshotFiles(cfg.Dir, snapshot, true)
	hard, _, err := disk.InitialState()
	if err != nil {
		disk.Close()
		return nil, err
	}
	if existing && applied > hard.Commit {
		disk.Close()
		return nil, fmt.Errorf("application checkpoint %d exceeds durable Raft commit %d", applied, hard.Commit)
	}
	ctx, cancel := context.WithCancel(parent)
	n := &Node{cfg: cfg, disk: disk, machine: machine, ctx: ctx, cancel: cancel, changed: make(chan struct{}), proposals: make(map[string]chan result), reads: make(map[string]chan uint64), peers: make(map[uint64]string), senders: make(map[uint64]chan packet), controlSenders: make(map[uint64]chan packet), application: make(chan applicationBatch), snapshotRequests: make(chan snapshotRequest)}
	n.metrics = observability.NewOperationMetrics()
	n.snapshotCleanupRequests = make(chan uint64, 1)
	n.snapshotCleanupFailure = cleanupErr
	n.scheduleSnapshotCleanup(snapshot.Metadata.Index)
	n.observeSnapshot(snapshot)
	n.applied.Store(applied)
	n.committed.Store(hard.Commit)
	for id, address := range cfg.Peers {
		n.peers[id] = address
	}
	for id, address := range snapshotPeers {
		n.peers[id] = address
	}
	if err := n.loadPeers(); err != nil {
		cancel()
		disk.Close()
		return nil, err
	}
	// Recover snapshots saved by versions that persisted peers separately.
	if len(snapshotPeers) > 0 && disk.confIndex <= snapshot.Metadata.Index {
		if err := n.installPeers(snapshotPeers); err != nil {
			cancel()
			disk.Close()
			return nil, err
		}
	}
	if err := n.installRetired(snapshotRetired, snapshot.Metadata.Index); err != nil {
		cancel()
		disk.Close()
		return nil, err
	}
	n.client = newTransportClient()
	rc := &raft.Config{ID: cfg.ID, ElectionTick: cfg.ElectionTicks, HeartbeatTick: 1, Storage: disk, Applied: applied, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 4 << 20, MaxInflightMsgs: 64, MaxUncommittedEntriesSize: 64 << 20, CheckQuorum: true, PreVote: true, DisableProposalForwarding: true, ReadOnlyOption: raft.ReadOnlySafe, StepDownOnRemoval: true}
	if existing || !cfg.Bootstrap {
		n.raft = raft.RestartNode(rc)
	} else {
		var peers []raft.Peer
		if cfg.Bootstrap {
			if len(cfg.Peers) != 3 {
				cancel()
				disk.Close()
				return nil, fmt.Errorf("bootstrap requires exactly three voting nodes")
			}
			for id := range cfg.Peers {
				peers = append(peers, raft.Peer{ID: id})
			}
		}
		sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
		n.raft = raft.StartNode(rc, peers)
	}
	n.observeDiagnostics()
	n.workers.Add(5)
	go n.run()
	go n.applyLoop()
	go n.snapshotCleanupLoop(cleanupOrphans)
	go n.diagnosticsLoop()
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

func (n *Node) Propose(ctx context.Context, data []byte) (response []byte, err error) {
	finish := n.metrics.Start("proposal")
	defer func() { finish(err) }()
	if err := n.beginProposal(ctx); err != nil {
		return nil, err
	}
	defer n.endProposal()
	return n.propose(ctx, data)
}

func (n *Node) propose(ctx context.Context, data []byte) ([]byte, error) {
	if err := n.available(true); err != nil {
		return nil, err
	}

	n.mu.Lock()
	snapshotFailure := n.snapshotFailure
	n.mu.Unlock()
	if snapshotFailure != nil {
		return nil, fmt.Errorf("%w: %v; resolve the snapshot error before resuming mutations", ErrUnavailable, snapshotFailure)
	}
	if n.protocolVersion() > ProtocolVersion && !n.protocolValidated.Load() {
		minimum, err := n.disk.minimumProtocol()
		if err != nil {
			return nil, err
		}
		// A durable command already records activation by a checked leader.
		// Requiring an offline voter again would defeat majority failover.
		if minimum < n.protocolVersion() {
			for id := range n.raft.Status().Config.Voters.IDs() {
				if id == n.cfg.ID {
					continue
				}
				peer, err := n.peerStatus(ctx, id)
				if err != nil || max(peer.Protocol, peer.MaxProtocol) < n.protocolVersion() {
					return nil, fmt.Errorf("%w: upgrade every voter before activating protocol %d (peer %d: %v)", ErrUnavailable, n.protocolVersion(), id, err)
				}
			}
		}
		n.protocolValidated.Store(true)
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(proposal{ID: id, Data: data, Protocol: n.protocolVersion()})
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
			return nil, fmt.Errorf("proposal outcome is unknown after submission: %v", err)
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

func (n *Node) ReadBarrier(ctx context.Context) (err error) {
	finish := n.metrics.Start("read_barrier")
	defer func() { finish(err) }()
	index, err := n.readIndex(ctx)
	if err != nil {
		return err
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

// QuorumBarrier checks current leadership without waiting for application.
// It keeps the entry point available during maintenance; graph operations
// must still use ReadBarrier before accessing the state machine.
func (n *Node) QuorumBarrier(ctx context.Context) (err error) {
	finish := n.metrics.Start("quorum_barrier")
	defer func() { finish(err) }()
	if _, err := n.readIndex(ctx); err != nil {
		return err
	}
	return n.available(true)
}

func (n *Node) readIndex(ctx context.Context) (uint64, error) {
	if err := n.available(true); err != nil {
		return 0, err
	}
	id, err := randomID()
	if err != nil {
		return 0, err
	}
	ch := make(chan uint64, 1)
	n.mu.Lock()
	n.reads[id] = ch
	n.mu.Unlock()
	defer func() { n.mu.Lock(); delete(n.reads, id); n.mu.Unlock() }()
	if err := n.raft.ReadIndex(ctx, []byte(id)); err != nil {
		return 0, err
	}
	var index uint64
	for index == 0 {
		n.mu.Lock()
		changed := n.changed
		n.mu.Unlock()
		if err := n.available(true); err != nil {
			return 0, err
		}
		select {
		case index = <-ch:
		case <-changed:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-n.ctx.Done():
			return 0, n.available(false)
		}
	}
	return index, nil
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
	// Request deadlines must not wait on Raft's synchronous status channel.
	// Application can advance between the two atomic progress observations.
	committed := n.committed.Load()
	if leader && (committed-min(committed, n.applied.Load()) > 1024 || n.applicationBytes.Load() > 64<<20) {
		return fmt.Errorf("%w: application backlog exceeds 1024 entries or 64 MiB", ErrUnavailable)
	}
	if leader && (n.leader.Load() != n.cfg.ID || n.draining.Load()) {
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
			finish := n.metrics.Start("snapshot_persist")
			snapshot, err := n.disk.CreateSnapshot(request.index, &request.conf, request.data)
			if err == nil {
				err = n.disk.saveSnapshot(snapshot, request.index)
			}
			observed := err
			if errors.Is(err, raft.ErrSnapOutOfDate) {
				observed = nil
				n.metrics.Event("snapshot_superseded")
			}
			finish(observed)
			if err == nil {
				n.observeSnapshot(snapshot)
				n.scheduleSnapshotCleanup(snapshot.Metadata.Index)
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
			var envelope *snapshotEnvelope
			if !raft.IsEmptySnap(ready.Snapshot) {
				decoded, err := decodeSnapshot(ready.Snapshot.Data)
				if err != nil {
					n.fail(err)
					return
				}
				envelope = &decoded
			}
			finish := n.metrics.Start("persist")
			err := n.disk.save(ready, envelope)
			finish(err)
			if err != nil {
				n.fail(err)
				return
			}
			if !raft.IsEmptyHardState(ready.HardState) {
				n.committed.Store(ready.HardState.Commit)
			}
			if envelope != nil {
				n.scheduleSnapshotCleanup(ready.Snapshot.Metadata.Index)
				n.snapshotIndex.Store(ready.Snapshot.Metadata.Index)
				n.snapshotBytes.Store(snapshotPayloadBytes(ready.Snapshot.Data, envelope))
				n.peerMu.Lock()
				for id, address := range envelope.Peers {
					n.peers[id] = address
				}
				n.peerMu.Unlock()
			}
			if ready.SoftState != nil {
				if n.leader.Swap(ready.SoftState.Lead) != ready.SoftState.Lead {
					n.leaderChanges.Add(1)
				}
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
			var membershipResults []membershipResult
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
					n.protocolValidated.Store(false)
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
					var outcome membershipResult
					var err error
					conf, outcome, err = n.applyMembership(change, conf, entry.Index)
					if err != nil {
						n.fail(err)
						return
					}
					membershipResults = append(membershipResults, outcome)
					confIndex = entry.Index
				}
			}
			if confIndex != n.disk.confIndex {
				finish := n.metrics.Start("configuration_persist")
				err := n.disk.saveConf(conf, confIndex)
				finish(err)
				if err != nil {
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
			// A reply can trigger the next change immediately. Advance must
			// clear Raft's pending configuration before that proposal arrives.
			for _, outcome := range membershipResults {
				n.mu.Lock()
				ch := n.proposals[outcome.id]
				n.mu.Unlock()
				if ch != nil {
					ch <- result{err: outcome.err}
				}
			}
		}
	}
}

func (n *Node) applyLoop() {
	defer n.workers.Done()
	lastSnapshot, _ := n.disk.Snapshot()
	snapshotIndex := lastSnapshot.Metadata.Index
	var building chan snapshotBuild
	lastConf := lastSnapshot.Metadata.ConfState
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case built := <-building:
			building = nil
			if built.err != nil {
				n.mu.Lock()
				n.snapshotFailure = built.err
				n.mu.Unlock()
				continue
			}
			if built.request.index <= snapshotIndex {
				continue
			}
			select {
			case n.snapshotRequests <- built.request:
			case <-n.ctx.Done():
				return
			}
			select {
			case err := <-built.request.done:
				if err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
					n.fail(err)
					return
				}
				if err == nil {
					snapshotIndex = built.request.index
					n.mu.Lock()
					n.snapshotFailure = nil
					n.mu.Unlock()
				}
			case <-n.ctx.Done():
				return
			}
		case <-retry.C:
			n.mu.Lock()
			failed := n.snapshotFailure != nil
			n.mu.Unlock()
			if failed && n.cfg.StreamSnapshots && building == nil && n.applied.Load() > snapshotIndex {
				var err error
				building, err = n.startStreamSnapshot(lastConf)
				if err != nil {
					n.mu.Lock()
					n.snapshotFailure = err
					n.mu.Unlock()
				}
			}
		case batch := <-n.application:
			lastConf = batch.conf
			if !raft.IsEmptySnap(batch.snapshot) && batch.snapshot.Metadata.Index > n.applied.Load() {
				finish := n.metrics.Start("snapshot_restore")
				err := restoreSnapshot(n.ctx, n.machine, n.cfg.Dir, batch.snapshot)
				finish(err)
				if err != nil {
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
				if err := n.applyEntries(entries); err != nil {
					n.fail(err)
					return
				}
				first = entries[len(entries)-1].Index + 1
			}
			if n.applied.Load()-snapshotIndex >= n.cfg.SnapshotEntries || batch.configurationChanged {
				if n.cfg.StreamSnapshots {
					if building != nil {
						continue
					}
					var err error
					building, err = n.startStreamSnapshot(lastConf)
					if err != nil {
						n.mu.Lock()
						n.snapshotFailure = err
						n.mu.Unlock()
					}
					continue
				}
				finish := n.metrics.Start("snapshot_build")
				data, err := n.machine.Snapshot(n.ctx)
				if err == nil && int64(len(data)) > n.cfg.MaxSnapshotBytes {
					err = ErrSnapshotTooLarge
				}
				if err == nil {
					data, err = n.snapshotData(data, n.applied.Load())
				}
				if err == nil && int64(len(data)) > n.cfg.MaxSnapshotBytes {
					err = ErrSnapshotTooLarge
				}
				finish(err)
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
	for _, queue := range n.senders {
		drainPackets(queue)
	}
	for _, queue := range n.controlSenders {
		drainPackets(queue)
	}
	n.client.CloseIdleConnections()
	return n.disk.Close()
}
