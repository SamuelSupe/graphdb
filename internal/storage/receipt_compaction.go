package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func (s *TenantStore) LocalReceiptPackingEnabled() bool {
	files := s.localFileStore()
	return files != nil && files.receiptPrefix != "" && !files.replicatedWrites.Load()
}

// Discover pre-upgrade receipts once. Subsequent sweeps are requested by a
// successful loose write or tenant directory replacement.
func (s *TenantStore) QueueExistingLocalReceiptPacks(ctx context.Context) error {
	tenants, err := s.ListManagedTenants(ctx)
	if err != nil {
		return err
	}
	files := s.localFileStore()
	for _, tenant := range tenants {
		files.queueReceiptDirectory(s.tenantObjectPrefix(tenant))
	}
	return nil
}

func (s *FileStore) queueReceiptDirectory(prefix string) {
	tenant, ok := strings.CutPrefix(prefix, s.receiptPrefix)
	tenant = strings.TrimSuffix(tenant, "/")
	if !ok || s.receiptPrefix == "" || s.replicatedWrites.Load() || tenant == "" || strings.Contains(tenant, "/") {
		return
	}
	s.receiptMu.Lock()
	defer s.receiptMu.Unlock()
	if s.receiptWork == nil {
		s.receiptWork = make(map[string]uint64)
	}
	s.receiptRevision++
	s.receiptWork[tenant] = s.receiptRevision
}

func (s *TenantStore) PendingLocalReceiptPacks() map[string]uint64 {
	files := s.localFileStore()
	files.receiptMu.Lock()
	defer files.receiptMu.Unlock()
	return maps.Clone(files.receiptWork)
}

func (s *TenantStore) CompleteLocalReceiptPack(tenant string, revision uint64) {
	files := s.localFileStore()
	files.receiptMu.Lock()
	defer files.receiptMu.Unlock()
	if files.receiptWork[tenant] == revision {
		delete(files.receiptWork, tenant)
	}
}

// PackLocalReceipts migrates one bounded page. The cursor is exclusive and
// callers may restart it from the beginning after a crash.
func (s *TenantStore) PackLocalReceipts(ctx context.Context, tenant, cursor string) (next string, reclaimed int, err error) {
	if !s.LocalReceiptPackingEnabled() {
		return "", 0, nil
	}
	if err := ValidateTenantID(tenant); err != nil {
		return "", 0, err
	}
	started := time.Now()
	defer s.recordMaintenance("pack_local_receipts", started)
	releaseView, err := s.lockReadViews(ctx, tenant, false)
	if err != nil {
		return "", 0, err
	}
	defer releaseView()
	release, err := s.lockTenantMaintenance(ctx, tenant)
	if err != nil {
		return "", 0, err
	}
	defer release()
	if err := s.EnsureTenantWritable(ctx, tenant); err != nil && !errors.Is(err, ErrTenantDisabled) {
		if errors.Is(err, ErrTenantDeleted) {
			return "", 0, nil
		}
		return "", 0, err
	}
	files := s.localFileStore()
	base := s.tenantObjectPrefix(tenant)
	prefix := base + "idempotency/commits/"
	ingest := base + "ingest/"
	if strings.HasPrefix(cursor, ingest) {
		prefix = ingest
	} else if cursor != "" && !strings.HasPrefix(cursor, prefix) {
		return "", 0, fmt.Errorf("%w: invalid receipt compaction cursor", ErrInvalidArgument)
	}
	var descend func(string) bool
	if prefix == ingest {
		descend = func(subtree string) bool {
			_, remainder, _ := strings.Cut(strings.TrimPrefix(subtree, ingest), "/")
			return remainder == "" || strings.HasPrefix(remainder, "batches/") || strings.HasPrefix(remainder, "idempotency/")
		}
	}
	objects, next, err := files.listLoosePageMatching(ctx, prefix, cursor, 256, descend)
	if err != nil {
		return "", 0, err
	}
	reclaimed, next, err = files.compactReceiptPage(ctx, base+receiptDatabase, objects, next)
	if err != nil {
		return cursor, reclaimed, err
	}
	if next == "" && prefix != ingest {
		next = ingest
	}
	return next, reclaimed, nil
}

func (s *FileStore) compactReceiptPage(ctx context.Context, database string, objects []ObjectInfo, next string) (count int, continuation string, err error) {
	if len(objects) == 0 {
		return 0, next, nil
	}
	releaseIO, err := s.beginOperation(ctx, database)
	if err != nil {
		return 0, next, err
	}
	var unlocks []func()
	var removed []string
	defer func() {
		for _, unlock := range unlocks {
			unlock()
		}
		releaseIO()
		if len(removed) != 0 {
			err = errors.Join(err, s.pruneEmptyObjectParents(ctx, removed))
		}
	}()
	type looseReceipt struct {
		objectKey, filename, key string
		data                     []byte
	}
	deadline, limited := ctx.Deadline()
	var entries []looseReceipt
	used := int64(0)
	for i, object := range objects {
		location, ok := s.receipt(object.Key)
		if !ok || location.database != database {
			continue
		}
		if used != 0 && (used+object.Size > 8<<20 || limited && time.Until(deadline) < 100*time.Millisecond) {
			next = objects[i-1].Key
			break
		}
		unlock, err := s.lockObjectContext(ctx, object.Key)
		if err != nil {
			return 0, next, err
		}
		unlocks = append(unlocks, unlock)
		filename, err := s.path(object.Key)
		if err != nil {
			return 0, next, err
		}
		if err := s.verifySafeParent(filename); err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return 0, next, err
		}
		info, err := os.Lstat(filename)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return 0, next, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filename)
		if err != nil {
			return 0, next, err
		}
		if !isParquetBytes(data) {
			return 0, next, fmt.Errorf("invalid receipt object %q", object.Key)
		}
		if !location.ingest {
			record, err := decodeParquetDirectCommitRecord(ctx, data)
			if err != nil {
				return 0, next, err
			}
			// Pending/prepared records remain visible to compaction's
			// publication settlement and are never moved to the pack.
			if directCommitRecordStatus(record) != directCommitStatusCommitted {
				continue
			}
		}
		used += int64(len(data))
		entries = append(entries, looseReceipt{object.Key, filename, location.key, data})
	}
	if len(entries) == 0 {
		return 0, next, nil
	}
	err = s.withReceiptDB(ctx, database, true, func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists(receiptBucket)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				key := []byte(entry.key)
				if previous := bucket.Get(key); previous != nil && !bytes.Equal(previous, entry.data) {
					return fmt.Errorf("%w: packed receipt differs from %q", ErrConflict, entry.objectKey)
				}
				if err := bucket.Put(key, entry.data); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return 0, next, err
	}
	s.changed(database, "", false)
	directories := map[string]bool{}
	for _, entry := range entries {
		if err := os.Remove(entry.filename); err != nil && !os.IsNotExist(err) {
			return len(removed), next, err
		}
		removed = append(removed, entry.objectKey)
		directories[filepath.Dir(entry.filename)] = true
		s.changed(entry.objectKey, "", true)
	}
	for directory := range directories {
		err = errors.Join(err, syncDir(directory))
	}
	return len(removed), next, err
}
