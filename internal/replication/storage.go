package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type diskStorage struct {
	dir          string
	mu           sync.RWMutex
	snapshotMeta raftpb.SnapshotMetadata
	db           *bolt.DB
	conf         raftpb.ConfState
	confIndex    uint64
}

func openStorage(dir string, id uint64, cluster string) (*diskStorage, bool, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, false, err
	}
	db, err := bolt.Open(filepath.Join(dir, "raft.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, false, err
	}
	disk := &diskStorage{db: db, dir: dir}
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
			existing = true
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
		_ = entries
		return nil
	})
	if err != nil {
		db.Close()
		return nil, false, err
	}
	file, err := os.Open(dir)
	if err == nil {
		err = file.Sync()
		file.Close()
	}
	if err != nil {
		db.Close()
		return nil, false, err
	}
	return disk, existing, nil
}

func (s *diskStorage) InitialState() (hard raftpb.HardState, conf raftpb.ConfState, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	err = s.db.View(func(tx *bolt.Tx) error {
		if data := tx.Bucket([]byte("meta")).Get([]byte("hard")); data != nil {
			return hard.Unmarshal(data)
		}
		return nil
	})
	return hard, s.conf, err
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
	defer s.mu.RUnlock()
	return s.snapshotMeta.Index + 1, nil
}

func (s *diskStorage) LastIndex() (index uint64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	index = s.snapshotMeta.Index
	err = s.db.View(func(tx *bolt.Tx) error {
		if key, _ := tx.Bucket([]byte("entries")).Cursor().Last(); key != nil {
			index = binary.BigEndian.Uint64(key)
		}
		return nil
	})
	return index, err
}

func (s *diskStorage) Term(index uint64) (term uint64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == s.snapshotMeta.Index {
		return s.snapshotMeta.Term, nil
	}
	if index < s.snapshotMeta.Index {
		return 0, raft.ErrCompacted
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("entries")).Get(indexKey(index))
		if data == nil {
			return raft.ErrUnavailable
		}
		var entry raftpb.Entry
		if err := entry.Unmarshal(data); err != nil {
			return err
		}
		term = entry.Term
		return nil
	})
	return term, err
}

func (s *diskStorage) Entries(lo, hi, maxSize uint64) (entries []raftpb.Entry, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if lo <= s.snapshotMeta.Index {
		return nil, raft.ErrCompacted
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte("entries")).Cursor()
		last, _ := cursor.Last()
		if hi > s.snapshotMeta.Index+1 && (last == nil || hi-1 > binary.BigEndian.Uint64(last)) {
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	err = s.db.View(func(tx *bolt.Tx) error {
		if data := tx.Bucket([]byte("meta")).Get([]byte("snapshot")); data != nil {
			return snapshot.Unmarshal(data)
		}
		return nil
	})
	return snapshot, err
}

func (s *diskStorage) CreateSnapshot(index uint64, conf *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		minimum := uint64(ProtocolVersion)
		if data := meta.Get([]byte("minimum-protocol")); len(data) == 8 {
			minimum = binary.BigEndian.Uint64(data)
		}
		if envelope != nil {
			minimum = max(minimum, uint64(envelope.Protocol))
		}
		for _, entry := range ready.Entries {
			if entry.Type == raftpb.EntryNormal && len(entry.Data) > 0 {
				var proposal proposal
				if json.Unmarshal(entry.Data, &proposal) == nil {
					minimum = max(minimum, uint64(max(0, proposal.Protocol)))
				}
			}
		}
		if err := meta.Put([]byte("minimum-protocol"), indexKey(minimum)); err != nil {
			return err
		}
		entries := tx.Bucket([]byte("entries"))
		if !raft.IsEmptySnap(ready.Snapshot) {
			data, err := ready.Snapshot.Marshal()
			if err != nil {
				return err
			}
			if err := meta.Put([]byte("snapshot"), data); err != nil {
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
			cursor := entries.Cursor()
			for key, _ := cursor.First(); key != nil && binary.BigEndian.Uint64(key) <= ready.Snapshot.Metadata.Index; key, _ = cursor.Next() {
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
	if !raft.IsEmptySnap(ready.Snapshot) {
		s.snapshotMeta = ready.Snapshot.Metadata
		s.conf = ready.Snapshot.Metadata.ConfState
		s.confIndex = ready.Snapshot.Metadata.Index
		if err := pruneSnapshotFiles(s.dir, ready.Snapshot, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *diskStorage) saveConf(conf raftpb.ConfState, index uint64) error {
	data, err := conf.Marshal()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		if err := meta.Put([]byte("conf"), data); err != nil {
			return err
		}
		return meta.Put([]byte("conf-index"), indexKey(index))
	})
}

func (s *diskStorage) saveSnapshot(snapshot raftpb.Snapshot, retain uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snapshot.Metadata.Index < s.snapshotMeta.Index {
		return raft.ErrSnapOutOfDate
	}
	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		envelope, decodeErr := decodeSnapshot(snapshot.Data)
		if decodeErr == nil && envelope.Protocol > ProtocolVersion {
			if err := tx.Bucket([]byte("meta")).Put([]byte("minimum-protocol"), indexKey(uint64(envelope.Protocol))); err != nil {
				return err
			}
		}
		if err := tx.Bucket([]byte("meta")).Put([]byte("snapshot"), data); err != nil {
			return err
		}
		cursor := tx.Bucket([]byte("entries")).Cursor()
		for key, _ := cursor.First(); key != nil && binary.BigEndian.Uint64(key) <= retain; key, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.snapshotMeta = snapshot.Metadata
	return pruneSnapshotFiles(s.dir, snapshot, false)
}
