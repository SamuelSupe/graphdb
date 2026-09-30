package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
)

type snapshotEnvelope struct {
	Retired []uint64          `json:"retired,omitempty"`
	Version int               `json:"version"`
	State   []byte            `json:"state"`
	Peers   map[uint64]string `json:"peers"`
}

func decodeSnapshot(data []byte) (snapshotEnvelope, error) {
	var snapshot snapshotEnvelope
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return snapshot, err
	}
	if snapshot.Version != 1 || len(snapshot.State) == 0 || len(snapshot.Peers) == 0 {
		return snapshot, fmt.Errorf("invalid Raft snapshot envelope")
	}
	return snapshot, nil
}

func (n *Node) snapshotData(state []byte) ([]byte, error) {
	n.peerMu.RLock()
	defer n.peerMu.RUnlock()
	var retired []uint64
	if err := n.disk.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("retired"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error { retired = append(retired, binary.BigEndian.Uint64(key)); return nil })
	}); err != nil {
		return nil, err
	}
	return json.Marshal(snapshotEnvelope{Version: 1, State: state, Peers: n.peers, Retired: retired})
}

func (n *Node) installRetired(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	return n.disk.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("retired"))
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := bucket.Put(indexKey(id), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	})
}
