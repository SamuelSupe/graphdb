package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

const receiptDatabase = "receipts.v1.db"

var receiptBucket = []byte("receipts")

type receiptLocation struct {
	database string
	key      string
	ingest   bool
}

func (s *FileStore) receipt(key string) (receiptLocation, bool) {
	if s.receiptPrefix == "" || s.replicatedWrites.Load() {
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

func (s *FileStore) packedReceipt(ctx context.Context, key string) ([]byte, error) {
	location, ok := s.receipt(key)
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
	location, ok := s.receipt(key)
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
	// Ingest metadata is written after the graph publication returns.
	// Its own transaction does not publish pending graph renames.
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

// A new ingest outcome and its idempotency alias share one transaction and
// durability barrier. Existing loose or packed keys use the usual CAS path.
func (s *FileStore) putNewIngestReceiptPair(ctx context.Context, batchKey, aliasKey string, data []byte) (ObjectMeta, bool, error) {
	batch, batchOK := s.receipt(batchKey)
	alias, aliasOK := s.receipt(aliasKey)
	if !batchOK || !aliasOK || !batch.ingest || !alias.ingest || batch.database != alias.database {
		return ObjectMeta{}, false, nil
	}
	release, err := s.beginOperation(ctx, batch.database)
	if err != nil {
		return ObjectMeta{}, false, err
	}
	defer release()
	// Batch paths sort before idempotency paths, as in loose compaction.
	for _, key := range []string{batchKey, aliasKey} {
		unlock, err := s.lockObjectContext(ctx, key)
		if err != nil {
			return ObjectMeta{}, false, err
		}
		defer unlock()
		filename, err := s.path(key)
		if err != nil {
			return ObjectMeta{}, false, err
		}
		if err := s.verifySafeParent(filename); err != nil && !errors.Is(err, ErrNotFound) {
			return ObjectMeta{}, false, err
		}
		if _, err := os.Lstat(filename); !os.IsNotExist(err) {
			return ObjectMeta{}, false, err
		}
	}
	err = s.withReceiptDB(ctx, batch.database, true, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			bucket, err := tx.CreateBucketIfNotExists(receiptBucket)
			if err != nil {
				return err
			}
			batchName, aliasName := []byte(batch.key), []byte(alias.key)
			if bucket.Get(batchName) != nil || bucket.Get(aliasName) != nil {
				return ErrConflict
			}
			if err := bucket.Put(batchName, data); err != nil {
				return err
			}
			return bucket.Put(aliasName, data)
		})
	})
	if errors.Is(err, ErrConflict) {
		return ObjectMeta{}, false, nil
	}
	if err != nil {
		return ObjectMeta{}, false, err
	}
	meta := ObjectMeta{Key: batchKey, ETag: sha256Hex(data), Exists: true}
	s.changed(batch.database, "", false)
	s.changed(batchKey, meta.ETag, false)
	s.changed(aliasKey, meta.ETag, false)
	return meta, true, nil
}

func (s *FileStore) deletePackedReceipt(ctx context.Context, key string, condition PutCondition) error {
	location, ok := s.receipt(key)
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
	location, ok := s.receipt(prefix + "entry.parquet")
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
