package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

// CheckLocalReceiptDatabase checks reachable receipt records under offline
// ownership. The data is never rewritten or inferred from missing receipts.
func CheckLocalReceiptDatabase(ctx context.Context, root, prefix, tenant string) (count int, err error) {
	if err := ValidateTenantID(tenant); err != nil {
		return 0, err
	}
	files, err := OpenFileStore(root)
	if err != nil {
		return 0, err
	}
	defer files.Close()
	files.receiptPrefix = path.Join(cleanPrefix(prefix), "tenants") + "/"
	base := files.receiptPrefix + tenant + "/"
	filename, err := files.path(base + receiptDatabase)
	if err != nil {
		return 0, err
	}
	if err := files.verifySafeParent(filename); err != nil {
		return 0, err
	}
	info, err := os.Lstat(filename)
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return 0, fmt.Errorf("%w: receipt database is not a nonempty regular file", ErrReceiptStoreUnavailable)
	}
	var file *os.File
	var db *bolt.DB
	defer func() {
		if value := recover(); value != nil {
			if file != nil {
				_ = closePoisonedReceiptFile(file)
			}
			count = 0
			err = fmt.Errorf("%w: %s: %v", ErrReceiptStoreUnavailable, tenant, value)
		} else if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	db, err = bolt.Open(filename, 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second,
		OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
			f, err := os.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, mode)
			file = f
			return f, err
		}})
	if err != nil {
		return 0, err
	}
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(receiptBucket)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			location, ok := files.receipt(base + string(key))
			if !ok || value == nil || !isParquetBytes(value) {
				return fmt.Errorf("invalid packed receipt %q", key)
			}
			if location.ingest {
				if _, err := decodeParquetIngestRecord(ctx, value); err != nil {
					return fmt.Errorf("receipt %q: %w", key, err)
				}
			} else {
				record, err := decodeParquetDirectCommitRecord(ctx, value)
				if err != nil {
					return fmt.Errorf("receipt %q: %w", key, err)
				}
				if directCommitRecordStatus(record) != directCommitStatusCommitted {
					return fmt.Errorf("nonterminal direct receipt %q", key)
				}
			}
			count++
			return nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %s: %w", ErrReceiptStoreUnavailable, tenant, err)
	}
	return count, nil
}
