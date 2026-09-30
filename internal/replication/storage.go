package replication

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type diskStorage struct {
	*raft.MemoryStorage
	db        *bolt.DB
	conf      raftpb.ConfState
	confIndex uint64
}

func openStorage(dir string, id uint64, cluster string) (*diskStorage, bool, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, false, err
	}
	db, err := bolt.Open(filepath.Join(dir, "raft.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, false, err
	}
	disk := &diskStorage{MemoryStorage: raft.NewMemoryStorage(), db: db}
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
			if err := disk.ApplySnapshot(snapshot); err != nil {
				return err
			}
			existing = true
		}
		if data := meta.Get([]byte("hard")); data != nil {
			var hard raftpb.HardState
			if err := hard.Unmarshal(data); err != nil {
				return err
			}
			if err := disk.SetHardState(hard); err != nil {
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
		return entries.ForEach(func(key, value []byte) error {
			var entry raftpb.Entry
			if err := entry.Unmarshal(value); err != nil {
				return err
			}
			if entry.Index > snapshot.Metadata.Index {
				return disk.Append([]raftpb.Entry{entry})
			}
			return nil
		})
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

func (s *diskStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hard, _, err := s.MemoryStorage.InitialState()
	return hard, s.conf, err
}

func indexKey(index uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], index)
	return key[:]
}

func (s *diskStorage) save(ready raft.Ready) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
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
		if err := s.MemoryStorage.ApplySnapshot(ready.Snapshot); err != nil {
			return err
		}
		s.conf = ready.Snapshot.Metadata.ConfState
		s.confIndex = ready.Snapshot.Metadata.Index
	}
	if err := s.Append(ready.Entries); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		return s.SetHardState(ready.HardState)
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
	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
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
	return s.Compact(retain)
}
