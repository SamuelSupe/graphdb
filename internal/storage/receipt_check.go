package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime/debug"

	bolt "go.etcd.io/bbolt"
)

// CheckLocalReceiptDatabase verifies packed records and bbolt page consistency
// under offline ownership. Run it in an isolated process when checking a
// potentially corrupt database because SetPanicOnFault does not apply to
// bbolt's checker goroutine.
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
		if errors.Is(err, ErrNotFound) {
			return 0, fmt.Errorf("tenant %q directory: %w", tenant, err)
		}
		return 0, err
	}
	info, err := os.Lstat(filename)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return 0, fmt.Errorf("%w: receipt database is not a nonempty regular file", ErrReceiptStoreUnavailable)
	}
	h, err := openBoltReceipt(filename, true, true)
	if err != nil {
		return 0, err
	}
	defer func() {
		if value := recover(); value != nil {
			h.err = fmt.Errorf("%w: %s: %v", ErrReceiptStoreUnavailable, tenant, value)
			count, err = 0, h.err
		}
		err = errors.Join(err, h.close())
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	err = h.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(receiptBucket)
		if bucket != nil {
			if err := bucket.ForEach(func(key, value []byte) error {
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
			}); err != nil {
				return err
			}
		}
		// The checker owns an asynchronous walker. Drain it before closing
		// the read transaction, even if an inconsistency has already been found.
		var first error
		for issue := range tx.Check(bolt.WithKVStringer(receiptCheckStrings{})) {
			if first == nil {
				first = issue
			}
		}
		return errors.Join(first, ctx.Err())
	})
	if err != nil {
		return 0, fmt.Errorf("%w: %s: %w", ErrReceiptStoreUnavailable, tenant, err)
	}
	return count, nil
}

type receiptCheckStrings struct{}

func (receiptCheckStrings) KeyToString([]byte) string   { return "<receipt key>" }
func (receiptCheckStrings) ValueToString([]byte) string { return "<receipt value>" }
