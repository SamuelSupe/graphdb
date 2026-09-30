package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const replicationDirectory = ".graphdb-replication"

type ReplicationCheckpoint struct {
	Index    uint64 `json:"index"`
	Response []byte `json:"response,omitempty"`
}

type replicationJournal struct {
	db      *bolt.DB
	files   *FileStore
	mu      sync.Mutex
	failure error
}

type replicationJournalKey struct{}

func (s *FileStore) ReplicationCheckpoint() (ReplicationCheckpoint, error) {
	data, err := os.ReadFile(filepath.Join(s.root, replicationDirectory, "checkpoint.json"))
	if os.IsNotExist(err) {
		return ReplicationCheckpoint{}, nil
	}
	if err != nil {
		return ReplicationCheckpoint{}, err
	}
	var checkpoint ReplicationCheckpoint
	err = json.Unmarshal(data, &checkpoint)
	return checkpoint, err
}

// ApplyReplicated journals before-images before touching live objects. The
// commit marker covers both graph publication and the applied log position.
// Startup rolls back an interrupted application before any reader is admitted.
func (s *FileStore) ApplyReplicated(ctx context.Context, index uint64, id string, at time.Time, apply func(context.Context) ([]byte, error)) ([]byte, error) {
	checkpoint, err := s.ReplicationCheckpoint()
	if err != nil {
		return nil, err
	}
	if index <= checkpoint.Index {
		return checkpoint.Response, nil
	}
	dir := filepath.Join(s.root, replicationDirectory)
	if err := ensureDurableDirectory(dir); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "pending.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	journal := &replicationJournal{db: db, files: s}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("objects"))
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		db.Close()
		return nil, err
	}
	ctx = context.WithValue(ReplicatedContext(ctx, id, at), replicationJournalKey{}, journal)
	response, applyErr := apply(ctx)
	journal.mu.Lock()
	applyErr = errors.Join(applyErr, journal.failure)
	journal.mu.Unlock()
	if applyErr != nil {
		db.Close()
		return nil, errors.Join(applyErr, s.recoverReplication())
	}
	if err := s.syncPendingDirectories(); err != nil {
		db.Close()
		return nil, err
	}
	checkpoint = ReplicationCheckpoint{Index: index, Response: response}
	data, err := json.Marshal(checkpoint)
	if err == nil {
		err = db.Update(func(tx *bolt.Tx) error {
			meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
			if err != nil {
				return err
			}
			return meta.Put([]byte("committed"), data)
		})
	}
	err = errors.Join(err, db.Close())
	if err != nil {
		return nil, err
	}
	if err := s.recoverReplication(); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *FileStore) journalObject(ctx context.Context, key string) error {
	journal, ok := ctx.Value(replicationJournalKey{}).(*replicationJournal)
	if !ok || journal.files != s {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("objects"))
		if bucket.Get([]byte(key)) != nil {
			return nil
		}
		filename, err := s.path(key)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			return bucket.Put([]byte(key), []byte{0})
		}
		if err != nil {
			return err
		}
		return bucket.Put([]byte(key), append([]byte{1}, data...))
	})
}

func (s *FileStore) journalDirectory(ctx context.Context, targetKey, incoming string) error {
	if _, ok := ctx.Value(replicationJournalKey{}).(*replicationJournal); !ok {
		return nil
	}
	target, err := s.path(targetKey)
	if err != nil {
		return err
	}
	for _, root := range []string{target, incoming} {
		err := filepath.WalkDir(root, func(filename string, entry os.DirEntry, walkErr error) error {
			if os.IsNotExist(walkErr) {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("non-regular replication object %s", filename)
			}
			relative, err := filepath.Rel(root, filename)
			if err != nil {
				return err
			}
			return s.journalObject(ctx, filepath.ToSlash(filepath.Join(targetKey, relative)))
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) recoverReplication() error {
	dir := filepath.Join(s.root, replicationDirectory)
	filename := filepath.Join(dir, "pending.db")
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := bolt.Open(filename, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	err = db.View(func(tx *bolt.Tx) error {
		if meta := tx.Bucket([]byte("meta")); meta != nil {
			if data := meta.Get([]byte("committed")); data != nil {
				var checkpoint ReplicationCheckpoint
				if err := json.Unmarshal(data, &checkpoint); err != nil {
					return err
				}
				return writeFileAtomic(filepath.Join(dir, "checkpoint.json"), data)
			}
		}
		bucket := tx.Bucket([]byte("objects"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			name := string(key)
			if err := validateObjectKey(name); err != nil {
				return err
			}
			filename, err := s.path(name)
			if err != nil {
				return err
			}
			if err := s.ensureSafeParent(filename); err != nil {
				return err
			}
			if len(value) == 0 {
				return fmt.Errorf("empty replication before-image")
			}
			if value[0] == 0 {
				if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
					return err
				}
				if err := syncDir(filepath.Dir(filename)); err != nil {
					return err
				}
			} else if value[0] == 1 {
				if err := writeFileAtomic(filename, value[1:]); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("invalid replication before-image")
			}
			s.changed(name, "")
			return nil
		})
	})
	err = errors.Join(err, db.Close())
	if err != nil {
		return err
	}
	if err := os.Remove(filename); err != nil {
		return err
	}
	return syncDir(dir)
}

func recordReplicationFailure(ctx context.Context, err error) {
	if err == nil || errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return
	}
	if journal, ok := ctx.Value(replicationJournalKey{}).(*replicationJournal); ok {
		journal.mu.Lock()
		journal.failure = errors.Join(journal.failure, err)
		journal.mu.Unlock()
	}
}

var ErrReplicationWriteRequired = errors.New("HA data changes require a committed Raft application")

func (s *FileStore) RequireReplicatedWrites() { s.replicatedWrites.Store(true) }
func (s *FileStore) checkReplicatedWrite(ctx context.Context) error {
	if !s.replicatedWrites.Load() {
		return nil
	}
	journal, ok := ctx.Value(replicationJournalKey{}).(*replicationJournal)
	if !ok || journal.files != s {
		return ErrReplicationWriteRequired
	}
	return nil
}
