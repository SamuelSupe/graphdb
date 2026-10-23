package replication

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
	"slices"
)

func (n *Node) startSnapshot(conf raftpb.ConfState) (built chan snapshotBuild, err error) {
	_, capture := n.machine.(StreamingStateMachine)
	if n.cfg.StreamSnapshots || capture {
		return n.startStreamSnapshot(conf)
	}
	finish := n.metrics.Start("snapshot_build")
	defer func() { finish(err) }()
	data, err := n.machine.Snapshot(n.ctx)
	if err == nil && int64(len(data)) > n.cfg.MaxSnapshotBytes {
		err = ErrSnapshotTooLarge
	}
	index := n.applied.Load()
	if err == nil {
		data, err = n.snapshotData(data, index)
	}
	if err == nil && int64(len(data)) > inlineSnapshotLimit(n.cfg.MaxSnapshotBytes) {
		err = ErrSnapshotTooLarge
	}
	if err != nil {
		return nil, err
	}
	built = make(chan snapshotBuild, 1)
	built <- snapshotBuild{request: snapshotRequest{index: index, data: data, conf: conf, done: make(chan error, 1)}}
	return built, nil
}

type snapshotEnvelope struct {
	Protocol int               `json:"protocol,omitempty"`
	Retired  []uint64          `json:"retired,omitempty"`
	Version  int               `json:"version"`
	State    []byte            `json:"state"`
	File     *snapshotFile     `json:"file,omitempty"`
	Peers    map[uint64]string `json:"peers"`
}

func decodeSnapshot(data []byte) (snapshotEnvelope, error) {
	var snapshot snapshotEnvelope
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return snapshot, err
	}
	if snapshot.Protocol < 0 || snapshot.Protocol > MaxProtocolVersion || len(snapshot.Peers) == 0 ||
		(snapshot.Version != 1 && snapshot.Version != 2) ||
		(snapshot.Version == 1 && (len(snapshot.State) == 0 || snapshot.File != nil)) ||
		(snapshot.Version == 2 && (len(snapshot.State) != 0 || snapshot.File == nil || !snapshot.File.valid())) {
		return snapshot, fmt.Errorf("invalid Raft snapshot envelope")
	}
	return snapshot, nil
}

func (n *Node) snapshotMetadata(index uint64) (snapshotEnvelope, error) {
	data, err := n.snapshotData(nil, index)
	var envelope snapshotEnvelope
	if err == nil {
		err = json.Unmarshal(data, &envelope)
	}
	return envelope, err
}

func (n *Node) snapshotData(state []byte, index uint64) ([]byte, error) {
	members, err := n.retiredMembers()
	if err != nil {
		return nil, err
	}
	var retired []uint64
	for id, removedAt := range members {
		if removedAt <= index {
			retired = append(retired, id)
		}
	}
	slices.Sort(retired)
	minimum, err := n.disk.minimumProtocol()
	if err != nil {
		return nil, err
	}
	n.peerMu.RLock()
	defer n.peerMu.RUnlock()
	return json.Marshal(snapshotEnvelope{Version: 1, Protocol: max(ProtocolVersion, minimum), State: state, Peers: n.peers, Retired: retired})
}

func (n *Node) installRetired(ids []uint64, index uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return n.disk.db.Update(func(tx *bolt.Tx) error {
		return persistRetired(tx, ids, index)
	})
}

func persistRetired(tx *bolt.Tx, ids []uint64, index uint64) error {
	if len(ids) == 0 {
		return nil
	}
	bucket, err := tx.CreateBucketIfNotExists([]byte("retired"))
	if err != nil {
		return err
	}
	for _, id := range ids {
		if bucket.Get(indexKey(id)) == nil {
			if err := bucket.Put(indexKey(id), indexKey(index)); err != nil {
				return err
			}
		}
	}
	return nil
}

// MaxSnapshotBytes measures raw state. JSON base64 and bounded peer metadata
// are wire overhead, not a reduction of the state machine's snapshot budget.
func inlineSnapshotLimit(raw int64) int64 {
	return (raw+2)/3*4 + snapshotMetadataLimit
}
