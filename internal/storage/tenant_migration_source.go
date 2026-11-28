package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// ValidateTenantMigrationSource cold-loads the published graph and relation
// schemas, including required objects and the current graph digest. It also
// requires valid management metadata and validates the remaining tenant controls
// and encodings that copy will rewrite. The caller
// must pin or freeze the input until it is copied. Invalid imported data is a
// rejected input; local object IO failures still poison a replicated application.
// declared, when non-nil, is the complete set of objects decoded from a transfer:
// losing an object in that set is local corruption, not an omitted input object.
func ValidateTenantMigrationSource(ctx context.Context, source *TenantStore, tenantID string, declared map[string]bool) error {
	if source == nil {
		return fmt.Errorf("source store is required")
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	objects := &tenantMigrationValidationStore{
		ObjectStore: unwrapTenantMigrationStore(source.Objects),
		journal:     ctx.Value(replicationJournalKey{}),
		declared:    declared,
	}
	// Decoding an unpublished input must not be treated as corruption of this
	// replica's live graph. Object reads restore the journal for actual IO errors.
	ctx = context.WithValue(ctx, replicationJournalKey{}, struct{}{})
	cold := NewTenantStore(objects, source.Prefix)
	manifest, meta, err := cold.readManifest(ctx, tenantID, true)
	if err != nil {
		return err
	}
	if !meta.Exists {
		return ErrNotFound
	}
	// Reject incomplete management state before decoding the graph, which can
	// be large even though its manifest and tenant metadata are small.
	if metadata, configured, _, err := cold.getTenantMetadataWithMeta(ctx, tenantID); err != nil {
		return err
	} else if !configured {
		return fmt.Errorf("%w: migration source tenant %q has no management metadata", ErrTenantMetadataMissing, tenantID)
	} else if metadata.TenantID != tenantID {
		return fmt.Errorf("migration metadata tenant mismatch: want %q, got %q", tenantID, metadata.TenantID)
	}
	if _, _, err := cold.GetTenantConfig(ctx, tenantID); err != nil {
		return err
	}
	if _, _, err := cold.GetSourcePolicy(ctx, tenantID); err != nil {
		return err
	}
	loaded, err := cold.loadManifestGraph(ctx, tenantID, manifest, meta)
	if err != nil {
		return err
	}
	schemas, err := cold.GetRelationSchemas(ctx, tenantID)
	if err != nil {
		return err
	}
	if err := validateRelationSchemaGraph(loaded.Graph, schemas); err != nil {
		return err
	}
	catalog, err := cold.GetIndexCatalog(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil && catalog.Version > manifest.Version {
		return fmt.Errorf("migration index catalog version %d exceeds manifest version %d", catalog.Version, manifest.Version)
	}
	if revCatalog, err := cold.GetReverseIndexCatalog(ctx, tenantID, catalog.Version); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	} else if err == nil && revCatalog.Version > manifest.Version {
		return fmt.Errorf("migration reverse index catalog version %d exceeds manifest version %d", revCatalog.Version, manifest.Version)
	}
	if err := validateMigrationReceiptDB(ctx, objects, cold.tenantObjectPrefix(tenantID)+receiptDatabase); err != nil {
		return err
	}
	return objects.validateEncodings(ctx, cold, tenantID)
}

func validateMigrationReceiptDB(ctx context.Context, objects ObjectStore, key string) error {
	data, err := objects.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("invalid tenant receipt database: empty file")
	}
	tmp, err := os.CreateTemp("", "graphdb-validate-receipts-*.db")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return checkMigrationReceiptDBFile(ctx, name)
}

// checkMigrationReceiptDBFile validates the bbolt freelist, every reachable
// B+tree branch/leaf page, and every packed receipt payload synchronously on
// the calling goroutine so SetPanicOnFault and recover() reliably contain
// mmap/bbolt faults without calling tx.Check()'s unprotected child goroutine.
func checkMigrationReceiptDBFile(ctx context.Context, name string) (err error) {
	var handle *receiptDBHandle
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("corrupt tenant receipt database: %v", value)
			if handle != nil && receiptDatabasePanic(value) {
				handle.err = err
			}
		}
		if handle != nil {
			_ = handle.close()
		}
	}()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	handle, err = openBoltReceipt(name, true, true)
	if err != nil {
		return fmt.Errorf("invalid tenant receipt database: %w", err)
	}
	return handle.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(bucketName []byte, b *bolt.Bucket) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			isReceipts := bytes.Equal(bucketName, receiptBucket)
			return b.ForEach(func(k, v []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !isReceipts {
					return nil
				}
				if len(v) == 0 || !isParquetBytes(v) {
					return fmt.Errorf("corrupt tenant receipt database: invalid packed receipt %q", k)
				}
				keyName := string(k)
				switch {
				case strings.HasPrefix(keyName, "idempotency/commits/"):
					record, err := decodeParquetDirectCommitRecord(ctx, v)
					if err != nil {
						return fmt.Errorf("corrupt tenant receipt database: receipt %q: %w", keyName, err)
					}
					if directCommitRecordStatus(record) != directCommitStatusCommitted {
						return fmt.Errorf("corrupt tenant receipt database: nonterminal direct receipt %q", keyName)
					}
				case strings.HasPrefix(keyName, "ingest/"):
					if _, err := decodeParquetIngestRecord(ctx, v); err != nil {
						return fmt.Errorf("corrupt tenant receipt database: receipt %q: %w", keyName, err)
					}
				default:
					return fmt.Errorf("corrupt tenant receipt database: unexpected receipt key %q", keyName)
				}
				return nil
			})
		})
	})
}

type tenantMigrationValidationStore struct {
	ObjectStore
	journal  any
	declared map[string]bool
}

func (s *tenantMigrationValidationStore) readContext(ctx context.Context) context.Context {
	if s.journal != nil {
		return context.WithValue(ctx, replicationJournalKey{}, s.journal)
	}
	return ctx
}

func (s *tenantMigrationValidationStore) Get(ctx context.Context, key string) ([]byte, error) {
	ctx = s.readContext(ctx)
	data, err := s.ObjectStore.Get(ctx, key)
	s.recordReadFailure(ctx, key, err)
	return data, err
}

func (s *tenantMigrationValidationStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	ctx = s.readContext(ctx)
	data, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	s.recordReadFailure(ctx, key, err)
	return data, meta, err
}

func (s *tenantMigrationValidationStore) recordReadFailure(ctx context.Context, key string, err error) {
	if s.declared[key] && errors.Is(err, ErrNotFound) {
		// The payload already supplied this file. Preserve the replica-failure
		// marker even though missing undeclared dependencies are business errors.
		recordReplicationFailure(ctx, fmt.Errorf("declared migration object %q was lost locally: %v", key, err))
		return
	}
	recordReplicationFailure(ctx, err)
}

func (s *tenantMigrationValidationStore) validateEncodings(ctx context.Context, cold *TenantStore, tenantID string) error {
	prefix := cold.tenantObjectPrefix(tenantID)
	var keys []string
	appendKey := func(key string) {
		if strings.HasPrefix(key, prefix) && key != cold.manifestKey(tenantID) && tenantMigrationObjectNeedsRewrite(strings.TrimPrefix(key, prefix)) {
			keys = append(keys, key)
		}
	}
	if s.declared != nil {
		for key, present := range s.declared {
			if present {
				appendKey(key)
			}
		}
	} else {
		if err := scanObjectPrefix(s.readContext(ctx), s.ObjectStore, prefix, func(objects []ObjectInfo) error {
			for _, object := range objects {
				appendKey(object.Key)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		data, err := s.Get(ctx, key)
		if err != nil {
			return err
		}
		// The same decoder/encoder used by copy must succeed before an install
		// changes live incarnation controls. Orphan and optional catalogs can
		// otherwise turn a deterministic bad payload into a group-wide apply failure.
		if _, _, _, err := cold.rewriteTenantMigrationObject(ctx, data, key, tenantID, tenantID, prefix, prefix, nil, writerFenceRef{}); err != nil {
			return fmt.Errorf("migration object %q: %w", key, err)
		}
	}
	return nil
}
