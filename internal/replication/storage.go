package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/dirlock"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	verifiedProtocolFloorSnapshot = "protocol-floor-snapshot"
	snapshotMetaKey               = "snapshot-meta"
	compactedMetaKey              = "compacted-meta"
)

func encodeSnapshotMeta(index, term uint64) []byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], index)
	binary.BigEndian.PutUint64(buf[8:16], term)
	return buf[:]
}

func readTxSnapshotMeta(meta *bolt.Bucket) (index, term uint64, ok bool) {
	if data := meta.Get([]byte(snapshotMetaKey)); len(data) == 16 {
		return binary.BigEndian.Uint64(data[0:8]), binary.BigEndian.Uint64(data[8:16]), true
	}
	if data := meta.Get([]byte("snapshot")); data != nil {
		var snapshot raftpb.Snapshot
		if err := snapshot.Unmarshal(data); err == nil {
			return snapshot.Metadata.Index, snapshot.Metadata.Term, true
		}
	}
	return 0, 0, false
}

func readTxCompactedMeta(meta *bolt.Bucket) (index, term uint64, ok bool) {
	if data := meta.Get([]byte(compactedMetaKey)); len(data) == 16 {
		return binary.BigEndian.Uint64(data[0:8]), binary.BigEndian.Uint64(data[8:16]), true
	}
	return readTxSnapshotMeta(meta)
}

func retainedTailValid(entries *bolt.Bucket, compIdx, snapIndex, snapTerm uint64) bool {
	if compIdx >= snapIndex {
		return false
	}
	expected := compIdx + 1
	cursor := entries.Cursor()
	for key, value := cursor.Seek(indexKey(expected)); key != nil; key, value = cursor.Next() {
		if len(key) != 8 {
			continue
		}
		idx := binary.BigEndian.Uint64(key)
		if idx != expected {
			return false
		}
		if idx == snapIndex {
			var entry raftpb.Entry
			if err := entry.Unmarshal(value); err != nil {
				return false
			}
			return entry.Index == snapIndex && entry.Term == snapTerm
		}
		expected++
	}
	return false
}

type diskStorage struct {
	dir            string
	mu             sync.RWMutex
	writeMu        sync.Mutex
	snapshotMeta   raftpb.SnapshotMetadata
	snapIndex      uint64
	snapTerm       uint64
	compactedIndex uint64
	compactedTerm  uint64
	terms          []uint64
	db             *bolt.DB
	lock           *os.File
	closeOnce      sync.Once
	closeErr       error
	conf           raftpb.ConfState
	confIndex      uint64
}

func openStorage(dir string, id uint64, cluster string) (*diskStorage, bool, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, false, err
	}
	lock, err := dirlock.Acquire(filepath.Join(dir, ".lock"))
	if err != nil {
		return nil, false, err
	}
	db, err := bolt.Open(filepath.Join(dir, "raft.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		lock.Close()
		return nil, false, err
	}
	disk := &diskStorage{db: db, dir: dir, lock: lock}
	existing := false
	err = db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		identity := fmt.Sprintf("%s/%d", cluster, id)
		if saved := meta.Get([]byte("identity")); saved != nil && string(saved) != identity {
			return fmt.Errorf("Raft data directory belongs to %s", saved)
		}
		if err := meta.Put([]byte("identity"), []byte(identity)); err != nil {
			return err
		}
		entries, err := tx.CreateBucketIfNotExists([]byte("entries"))
		if err != nil {
			return err
		}
		var snapshot raftpb.Snapshot
		if data := meta.Get([]byte("snapshot")); data != nil {
			if err := snapshot.Unmarshal(data); err != nil {
				return err
			}
			disk.snapshotMeta = snapshot.Metadata
			disk.snapIndex = snapshot.Metadata.Index
			disk.snapTerm = snapshot.Metadata.Term
			if err := meta.Put([]byte(snapshotMetaKey), encodeSnapshotMeta(snapshot.Metadata.Index, snapshot.Metadata.Term)); err != nil {
				return err
			}
			existing = true
		}
		if disk.snapIndex > 0 {
			disk.compactedIndex = disk.snapIndex
			disk.compactedTerm = disk.snapTerm
			if compIdx, compTerm, ok := readTxCompactedMeta(meta); ok && compIdx < disk.snapIndex {
				// Older releases update meta["snapshot"] and prune compacted
				// entries without updating compacted-meta. Only trust a
				// pre-snapshot compacted floor when the retained log tail up
				// through the canonical snapshot is still intact and term-matched.
				if retainedTailValid(entries, compIdx, disk.snapIndex, disk.snapTerm) {
					disk.compactedIndex = compIdx
					disk.compactedTerm = compTerm
				}
			}
			if err := meta.Put([]byte(compactedMetaKey), encodeSnapshotMeta(disk.compactedIndex, disk.compactedTerm)); err != nil {
				return err
			}
		} else {
			if err := meta.Delete([]byte(snapshotMetaKey)); err != nil {
				return err
			}
			if err := meta.Delete([]byte(compactedMetaKey)); err != nil {
				return err
			}
		}
		cursor := entries.Cursor()
		for key, value := cursor.Seek(indexKey(disk.compactedIndex + 1)); key != nil; key, value = cursor.Next() {
			if len(key) != 8 {
				continue
			}
			idx := binary.BigEndian.Uint64(key)
			var entry raftpb.Entry
			if err := entry.Unmarshal(value); err != nil {
				return err
			}
			if idx == disk.compactedIndex+1+uint64(len(disk.terms)) {
				disk.terms = append(disk.terms, entry.Term)
			}
		}
		if data := meta.Get([]byte("hard")); data != nil {
			var hard raftpb.HardState
			if err := hard.Unmarshal(data); err != nil {
				return err
			}

			existing = true
		}
		if data := meta.Get([]byte("conf")); data != nil {
			if err := disk.conf.Unmarshal(data); err != nil {
				return err
			}
		}
		if data := meta.Get([]byte("conf-index")); data != nil {
			if len(data) != 8 {
				return fmt.Errorf("invalid persisted Raft configuration position")
			}
			disk.confIndex = binary.BigEndian.Uint64(data)
		}
		return repairSnapshotProtocolFloor(meta, entries, snapshot.Metadata.Index)
	})
	if err != nil {
		disk.Close()
		return nil, false, err
	}
	file, err := os.Open(dir)
	if err == nil {
		err = file.Sync()
		file.Close()
	}
	if err != nil {
		disk.Close()
		return nil, false, err
	}
	return disk, existing, nil
}

func (s *diskStorage) Close() error {
	s.closeOnce.Do(func() { s.closeErr = errors.Join(s.db.Close(), s.lock.Close()) })
	return s.closeErr
}

func (s *diskStorage) confState() (raftpb.ConfState, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conf, s.confIndex
}

func (s *diskStorage) InitialState() (hard raftpb.HardState, conf raftpb.ConfState, err error) {
	s.mu.RLock()
	conf = s.conf
	s.mu.RUnlock()
	err = s.db.View(func(tx *bolt.Tx) error {
		if data := tx.Bucket([]byte("meta")).Get([]byte("hard")); data != nil {
			return hard.Unmarshal(data)
		}
		return nil
	})
	return hard, conf, err
}

// Older releases could persist a delayed snapshot after a newer protocol entry,
// replacing the durable floor. Reconcile their retained log once per unverified snapshot.
func repairSnapshotProtocolFloor(meta, entries *bolt.Bucket, snapshotIndex uint64) error {
	if snapshotIndex == 0 || bytes.Equal(meta.Get([]byte(verifiedProtocolFloorSnapshot)), indexKey(snapshotIndex)) {
		return nil
	}
	minimum := uint64(ProtocolVersion)
	err := entries.ForEach(func(_, value []byte) error {
		var entry raftpb.Entry
		if err := entry.Unmarshal(value); err != nil {
			return err
		}
		minimum = max(minimum, entryMinimumProtocol(entry))
		return nil
	})
	if err != nil {
		return err
	}
	if err := raiseMinimumProtocol(meta, minimum); err != nil {
		return err
	}
	return meta.Put([]byte(verifiedProtocolFloorSnapshot), indexKey(snapshotIndex))
}

func entryMinimumProtocol(entry raftpb.Entry) uint64 {
	if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
		return 0
	}
	// Decode the protocol field without allocating the command body.
	var header struct {
		Protocol int `json:"protocol"`
	}
	if json.Unmarshal(entry.Data, &header) != nil || header.Protocol < 0 {
		return 0
	}
	return uint64(header.Protocol)
}

func persistSnapshot(meta *bolt.Bucket, index, term uint64, data []byte) error {
	if err := meta.Put([]byte("snapshot"), data); err != nil {
		return err
	}
	if err := meta.Put([]byte(snapshotMetaKey), encodeSnapshotMeta(index, term)); err != nil {
		return err
	}
	return meta.Put([]byte(verifiedProtocolFloorSnapshot), indexKey(index))
}

func raiseMinimumProtocol(meta *bolt.Bucket, minimum uint64) error {
	key := []byte("minimum-protocol")
	if data := meta.Get(key); data != nil {
		if len(data) != 8 {
			return fmt.Errorf("invalid minimum protocol")
		}
		if binary.BigEndian.Uint64(data) >= minimum {
			return nil
		}
	}
	return meta.Put(key, indexKey(minimum))
}

func (s *diskStorage) minimumProtocol() (minimum int, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("meta")).Get([]byte("minimum-protocol"))
		if len(data) > 0 {
			if len(data) != 8 {
				return fmt.Errorf("invalid minimum protocol")
			}
			minimum = int(binary.BigEndian.Uint64(data))
		}
		return nil
	})
	return
}

func (s *diskStorage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	index := s.compactedIndex + 1
	s.mu.RUnlock()
	return index, nil
}

func (s *diskStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	index := s.compactedIndex + uint64(len(s.terms))
	s.mu.RUnlock()
	return index, nil
}

func (s *diskStorage) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == s.compactedIndex {
		return s.compactedTerm, nil
	}
	if index < s.compactedIndex {
		return 0, raft.ErrCompacted
	}
	offset := index - s.compactedIndex - 1
	if offset >= uint64(len(s.terms)) {
		return 0, raft.ErrUnavailable
	}
	return s.terms[offset], nil
}

func (s *diskStorage) Entries(lo, hi, maxSize uint64) (entries []raftpb.Entry, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		compactedIndex, _, _ := readTxCompactedMeta(tx.Bucket([]byte("meta")))
		if lo <= compactedIndex {
			return raft.ErrCompacted
		}
		cursor := tx.Bucket([]byte("entries")).Cursor()
		last, _ := cursor.Last()
		if hi > compactedIndex+1 && (last == nil || hi-1 > binary.BigEndian.Uint64(last)) {
			return raft.ErrUnavailable
		}
		var size uint64
		expected := lo
		for key, value := cursor.Seek(indexKey(lo)); key != nil && binary.BigEndian.Uint64(key) < hi; key, value = cursor.Next() {
			var entry raftpb.Entry
			if err := entry.Unmarshal(value); err != nil {
				return err
			}
			if entry.Index != expected {
				return raft.ErrUnavailable
			}
			entrySize := uint64(entry.Size())
			if len(entries) > 0 && (size > maxSize || entrySize > maxSize-size) {
				break
			}
			entries = append(entries, entry)
			size += entrySize
			expected++
		}
		if lo < hi && len(entries) == 0 {
			return raft.ErrUnavailable
		}
		return nil
	})
	return entries, err
}

func (s *diskStorage) Snapshot() (snapshot raftpb.Snapshot, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		if data := tx.Bucket([]byte("meta")).Get([]byte("snapshot")); data != nil {
			return snapshot.Unmarshal(data)
		}
		return nil
	})
	return snapshot, err
}

func (s *diskStorage) CreateSnapshot(index uint64, conf *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	if conf == nil || len(conf.Voters) == 0 {
		return raftpb.Snapshot{}, fmt.Errorf("refusing to create Raft snapshot with empty voter configuration")
	}
	s.mu.RLock()
	confIndex := s.confIndex
	s.mu.RUnlock()
	if index < confIndex {
		return raftpb.Snapshot{}, raft.ErrSnapOutOfDate
	}
	term, err := s.Term(index)
	if err != nil {
		if err == raft.ErrCompacted {
			err = raft.ErrSnapOutOfDate
		}
		return raftpb.Snapshot{}, err
	}
	return raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: *conf}}, nil
}

func indexKey(index uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], index)
	return key[:]
}

func (s *diskStorage) save(ready raft.Ready, envelope *snapshotEnvelope) error {
	// Heartbeats and ReadIndex responses contain only volatile state. A disk
	// transaction is needed only when Raft has changed its durable state.
	if raft.IsEmptySnap(ready.Snapshot) && len(ready.Entries) == 0 && raft.IsEmptyHardState(ready.HardState) {
		return nil
	}
	var peers []byte
	if !raft.IsEmptySnap(ready.Snapshot) {
		if envelope == nil {
			return fmt.Errorf("snapshot membership metadata is missing")
		}
		var err error
		peers, err = json.Marshal(envelope.Peers)
		if err != nil {
			return err
		}
	}
	// JSON must still validate the entire log entry, including legacy bodies.
	// Do that before blocking storage readers or starting the Bolt write.
	minimum := uint64(ProtocolVersion)
	if envelope != nil {
		minimum = max(minimum, uint64(envelope.Protocol))
	}
	for _, entry := range ready.Entries {
		minimum = max(minimum, entryMinimumProtocol(entry))
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		if err := raiseMinimumProtocol(meta, minimum); err != nil {
			return err
		}
		entries := tx.Bucket([]byte("entries"))
		if !raft.IsEmptySnap(ready.Snapshot) {
			data, err := ready.Snapshot.Marshal()
			if err != nil {
				return err
			}
			if err := persistSnapshot(meta, ready.Snapshot.Metadata.Index, ready.Snapshot.Metadata.Term, data); err != nil {
				return err
			}
			if err := meta.Put([]byte(compactedMetaKey), encodeSnapshotMeta(ready.Snapshot.Metadata.Index, ready.Snapshot.Metadata.Term)); err != nil {
				return err
			}
			conf, err := ready.Snapshot.Metadata.ConfState.Marshal()
			if err != nil {
				return err
			}
			if err := meta.Put([]byte("conf"), conf); err != nil {
				return err
			}
			if err := meta.Put([]byte("conf-index"), indexKey(ready.Snapshot.Metadata.Index)); err != nil {
				return err
			}
			// Transport metadata must become durable with the configuration,
			// before later log entries can apply while graph restore is pending.
			if err := meta.Put([]byte("peers"), peers); err != nil {
				return err
			}
			if err := persistRetired(tx, envelope.Retired, ready.Snapshot.Metadata.Index); err != nil {
				return err
			}
			// Installing an external snapshot replaces the entire prior log per
			// etcd/raft's MemoryStorage.ApplySnapshot contract. Retaining tail
			// entries > snapshot.Metadata.Index from a diverged term would violate
			// monotonic terms and election safety.
			cursor := entries.Cursor()
			for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		if len(ready.Entries) > 0 {
			cursor := entries.Cursor()
			for key, _ := cursor.Seek(indexKey(ready.Entries[0].Index)); key != nil; key, _ = cursor.Next() {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
			for _, entry := range ready.Entries {
				data, err := entry.Marshal()
				if err != nil {
					return err
				}
				if err := entries.Put(indexKey(entry.Index), data); err != nil {
					return err
				}
			}
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			data, err := ready.HardState.Marshal()
			if err != nil {
				return err
			}
			return meta.Put([]byte("hard"), data)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !raft.IsEmptySnap(ready.Snapshot) || len(ready.Entries) > 0 {
		s.mu.Lock()
		if !raft.IsEmptySnap(ready.Snapshot) {
			newSnapIdx := ready.Snapshot.Metadata.Index
			// All prior entries were cleared above; any entries following the
			// snapshot in this Ready will be appended below.
			s.terms = nil
			s.snapshotMeta = ready.Snapshot.Metadata
			s.snapIndex = newSnapIdx
			s.snapTerm = ready.Snapshot.Metadata.Term
			s.compactedIndex = newSnapIdx
			s.compactedTerm = ready.Snapshot.Metadata.Term
			s.conf = ready.Snapshot.Metadata.ConfState
			s.confIndex = newSnapIdx
		}
		if len(ready.Entries) > 0 {
			firstNew := ready.Entries[0].Index
			base := s.compactedIndex + 1
			if firstNew >= base {
				offset := firstNew - base
				if offset < uint64(len(s.terms)) {
					s.terms = s.terms[:offset]
				}
			} else {
				s.terms = s.terms[:0]
			}
			for _, entry := range ready.Entries {
				if entry.Index <= s.compactedIndex {
					continue
				}
				if entry.Index == s.compactedIndex+1+uint64(len(s.terms)) {
					s.terms = append(s.terms, entry.Term)
				}
			}
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *diskStorage) saveConf(conf raftpb.ConfState, index uint64) error {
	return s.saveConfWithMembership(conf, index, nil, nil)
}

func (s *diskStorage) saveConfWithMembership(conf raftpb.ConfState, index uint64, fallbackPeers map[uint64]string, peerChanges []pendingPeerChange) error {
	data, err := conf.Marshal()
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		if len(peerChanges) > 0 {
			peers := map[uint64]string{}
			for id, addr := range fallbackPeers {
				peers[id] = addr
			}
			// Read the durable table under the writer transaction so the initial
			// in-memory clone cannot overwrite newer persisted addresses.
			if raw := meta.Get([]byte("peers")); len(raw) > 0 {
				var persisted map[uint64]string
				if err := json.Unmarshal(raw, &persisted); err != nil {
					return err
				}
				if persisted == nil {
					return errors.New("invalid persisted peer table")
				}
				peers = persisted
			}
			for _, pc := range peerChanges {
				if pc.change.Action == "add_learner" {
					peers[pc.change.ID] = pc.change.URL
				}
				// Retain removed IDs as tombstones atomically with conf/conf-index.
				if pc.change.Action == "remove" {
					if err := persistRetired(tx, []uint64{pc.change.ID}, pc.index); err != nil {
						return err
					}
				}
			}
			encoded, err := json.Marshal(peers)
			if err != nil {
				return err
			}
			if err := meta.Put([]byte("peers"), encoded); err != nil {
				return err
			}
		}
		if err := meta.Put([]byte("conf"), data); err != nil {
			return err
		}
		return meta.Put([]byte("conf-index"), indexKey(index))
	}); err != nil {
		return err
	}
	s.mu.Lock()
	s.conf = conf
	s.confIndex = index
	s.mu.Unlock()
	return nil
}

func (s *diskStorage) saveSnapshot(snapshot raftpb.Snapshot, retain uint64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	currentIndex := s.snapshotMeta.Index
	newCompactedIdx := s.compactedIndex
	newCompactedTerm := s.compactedTerm
	target := min(retain, snapshot.Metadata.Index)
	if target > newCompactedIdx {
		if target == snapshot.Metadata.Index {
			newCompactedIdx = snapshot.Metadata.Index
			newCompactedTerm = snapshot.Metadata.Term
		} else {
			offset := target - s.compactedIndex - 1
			if offset < uint64(len(s.terms)) {
				newCompactedIdx = target
				newCompactedTerm = s.terms[offset]
			} else {
				newCompactedIdx = snapshot.Metadata.Index
				newCompactedTerm = snapshot.Metadata.Term
			}
		}
	}
	s.mu.RUnlock()
	if snapshot.Metadata.Index < currentIndex {
		return raft.ErrSnapOutOfDate
	}
	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	var snapshotProtocol uint64
	if envelope, decodeErr := decodeSnapshot(snapshot.Data); decodeErr == nil && envelope.Protocol > ProtocolVersion {
		snapshotProtocol = uint64(envelope.Protocol)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		if snapshotProtocol > 0 {
			if err := raiseMinimumProtocol(meta, snapshotProtocol); err != nil {
				return err
			}
		}
		if err := persistSnapshot(meta, snapshot.Metadata.Index, snapshot.Metadata.Term, data); err != nil {
			return err
		}
		if err := meta.Put([]byte(compactedMetaKey), encodeSnapshotMeta(newCompactedIdx, newCompactedTerm)); err != nil {
			return err
		}
		cursor := tx.Bucket([]byte("entries")).Cursor()
		for key, _ := cursor.First(); key != nil && binary.BigEndian.Uint64(key) <= newCompactedIdx; key, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	if newCompactedIdx > s.compactedIndex {
		drop := newCompactedIdx - s.compactedIndex
		if drop >= uint64(len(s.terms)) {
			s.terms = nil
		} else {
			s.terms = append([]uint64(nil), s.terms[drop:]...)
		}
		s.compactedIndex = newCompactedIdx
		s.compactedTerm = newCompactedTerm
	}
	s.snapshotMeta = snapshot.Metadata
	s.snapIndex = snapshot.Metadata.Index
	s.snapTerm = snapshot.Metadata.Term
	s.mu.Unlock()
	return nil
}
