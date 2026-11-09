package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

const receiptDatabase = "receipts.v1.db"

var receiptBucket = []byte("receipts")

type looseReceiptKey struct{}

type receiptLocation struct {
	database string
	key      string
	ingest   bool
}

// EnableLocalReceiptPacking is called before standalone storage begins serving.
// Raft continues to journal the original per-object files.
func (s *FileStore) EnableLocalReceiptPacking(prefix string) {
	s.receiptPrefix = path.Join(cleanPrefix(prefix), "tenants") + "/"
}

func (s *FileStore) receipt(ctx context.Context, key string) (receiptLocation, bool) {
	if s.receiptPrefix == "" || s.replicatedWrites.Load() || ctx.Value(looseReceiptKey{}) != nil {
		return receiptLocation{}, false
	}
	relative, ok := strings.CutPrefix(key, s.receiptPrefix)
	tenant, name, found := strings.Cut(relative, "/")
	if !ok || !found || !strings.HasSuffix(name, ".parquet") {
		return receiptLocation{}, false
	}
	location := receiptLocation{database: s.receiptPrefix + tenant + "/" + receiptDatabase, key: name}
	if strings.HasPrefix(name, "idempotency/commits/") {
		return location, true
	}
	parts := strings.Split(name, "/")
	location.ingest = len(parts) >= 4 && parts[0] == "ingest" && (parts[2] == "batches" || parts[2] == "idempotency")
	return location, location.ingest
}

// The caller holds a directory IO permit. Open handles stay inside the permit
// so purge and directory replacement never leave a writer on a retired inode.
func (s *FileStore) withReceiptDB(ctx context.Context, key string, write bool, operation func(*bolt.DB) error) error {
	unlock, err := s.lockObjectContext(ctx, key)
	if err != nil {
		return err
	}
	defer unlock()
	filename, err := s.path(key)
	if err != nil {
		return err
	}
	if write {
		if err := s.ensureSafeParent(filename, nil); err != nil {
			return err
		}
	} else if err := s.verifySafeParent(filename); err != nil {
		return err
	}
	options := &bolt.Options{ReadOnly: !write, Timeout: time.Second,
		OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
			return os.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, mode)
		}}
	info, err := os.Lstat(filename)
	if err == nil && !info.Mode().IsRegular() {
		return fmt.Errorf("receipt database is not a regular file")
	}
	if err != nil && (!write || !os.IsNotExist(err)) {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	db, err := bolt.Open(filename, 0600, options)
	if err != nil {
		return err
	}
	if write {
		// A previous create may have lost its directory barrier. The database
		// must be durable before publishing a record or deleting its loose copy.
		err = syncDir(filepath.Dir(filename))
	}
	if err == nil {
		err = operation(db)
	}
	if closeErr := db.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

func (s *FileStore) packedReceipt(ctx context.Context, key string) ([]byte, error) {
	location, ok := s.receipt(ctx, key)
	if !ok {
		return nil, ErrNotFound
	}
	var data []byte
	err := s.withReceiptDB(ctx, location.database, false, func(db *bolt.DB) error {
		return db.View(func(tx *bolt.Tx) error {
			if bucket := tx.Bucket(receiptBucket); bucket != nil {
				if stored := bucket.Get([]byte(location.key)); stored != nil {
					data = bytes.Clone(stored)
					return nil
				}
			}
			return ErrNotFound
		})
	})
	return data, err
}

// New ingest receipts go straight into the pack. Existing loose keys keep
// their original CAS path until the maintenance copy is durable.
func (s *FileStore) putReceiptIfPacked(ctx context.Context, key, filename string, data []byte, condition PutCondition) (ObjectMeta, bool, error) {
	location, ok := s.receipt(ctx, key)
	if !ok {
		return ObjectMeta{}, false, nil
	}
	if err := s.verifySafeParent(filename); err != nil && !errors.Is(err, ErrNotFound) {
		return ObjectMeta{}, true, err
	}
	if _, err := os.Lstat(filename); err == nil {
		return ObjectMeta{}, false, nil
	} else if !os.IsNotExist(err) {
		return ObjectMeta{}, true, err
	}
	if !location.ingest {
		// Only terminal direct-commit receipts are packed. A replay must find
		// one before it can reserve another pending receipt at the same key.
		stored, err := s.packedReceipt(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return ObjectMeta{}, false, nil
		}
		if err != nil {
			return ObjectMeta{}, true, err
		}
		return ObjectMeta{Key: key, ETag: sha256Hex(stored), Exists: true}, true, ErrConflict
	}
	if err := s.syncPendingDirectories(); err != nil {
		return ObjectMeta{}, true, err
	}
	meta := ObjectMeta{Key: key}
	err := s.withReceiptDB(ctx, location.database, true, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists(receiptBucket)
			if err != nil {
				return err
			}
			name := []byte(location.key)
			if previous := bucket.Get(name); previous != nil {
				meta.Exists, meta.ETag = true, sha256Hex(previous)
			}
			if err := checkCondition(condition, meta.ETag, meta.Exists); err != nil {
				return err
			}
			return bucket.Put(name, data)
		})
	})
	if err == nil {
		meta.Exists, meta.ETag = true, sha256Hex(data)
		s.changed(location.database, "", false)
		s.changed(key, meta.ETag, false)
	}
	return meta, true, err
}

func (s *FileStore) deletePackedReceipt(ctx context.Context, key string, condition PutCondition) error {
	location, ok := s.receipt(ctx, key)
	if !ok {
		return checkCondition(condition, "", false)
	}
	if _, err := s.packedReceipt(ctx, key); err != nil {
		if errors.Is(err, ErrNotFound) {
			return checkCondition(condition, "", false)
		}
		return err
	}
	err := s.withReceiptDB(ctx, location.database, true, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(receiptBucket)
			if bucket == nil {
				return checkCondition(condition, "", false)
			}
			name := []byte(location.key)
			stored := bucket.Get(name)
			etag := ""
			if stored != nil && condition.IfMatch != "" {
				etag = sha256Hex(stored)
			}
			if err := checkCondition(condition, etag, stored != nil); err != nil {
				return err
			}
			return bucket.Delete(name)
		})
	})
	if err == nil {
		s.changed(location.database, "", false)
		s.changed(key, "", true)
	}
	return err
}

// Collector history is the only logical listing of packed records. Tenant
// usage, purge and offline archives continue to enumerate physical objects.
func (s *FileStore) mergePackedIngestPage(ctx context.Context, prefix, after string, limit int, loose []ObjectInfo, next string) ([]ObjectInfo, string, error) {
	location, ok := s.receipt(ctx, prefix+"entry.parquet")
	if !ok || !location.ingest || !strings.Contains(location.key, "/batches/") {
		return loose, next, nil
	}
	unlock, err := s.beginOperation(ctx, location.database)
	if err != nil {
		return nil, "", err
	}
	defer unlock()
	base := strings.TrimSuffix(path.Dir(location.database), "/") + "/"
	start := []byte(strings.TrimSuffix(location.key, "entry.parquet"))
	lower := start
	if value, ok := strings.CutPrefix(after, base); ok && value > string(lower) {
		lower = []byte(value)
	}
	var packed []ObjectInfo
	err = s.withReceiptDB(ctx, location.database, false, func(db *bolt.DB) error {
		return db.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(receiptBucket)
			if bucket == nil {
				return nil
			}
			cursor := bucket.Cursor()
			for k, v := cursor.Seek(lower); k != nil && bytes.HasPrefix(k, start); k, v = cursor.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				key := base + string(k)
				if key > after {
					packed = append(packed, ObjectInfo{Key: key, Size: int64(len(v))})
					if limit > 0 && len(packed) > limit {
						break
					}
				}
			}
			return nil
		})
	})
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, "", err
	}
	if len(packed) == 0 {
		return loose, next, nil
	}
	merged := append(loose, packed...)
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })
	items := merged[:0]
	for _, item := range merged {
		if len(items) == 0 || item.Key != items[len(items)-1].Key {
			items = append(items, item)
		}
	}
	if limit > 0 && (len(items) > limit || next != "") {
		items = items[:min(limit, len(items))]
		next = items[len(items)-1].Key
	}
	return items, next, nil
}
