package storage

import (
	"context"
	"errors"
	"strings"
)

func (s *TenantStore) cleanupIndexOrphansLocked(ctx context.Context, tenantID string, checkpoint *gcCheckpointRunner, report *GCReport) error {
	// Prefix order must match the exclusive object-key cursor used by GC.
	if err := s.cleanupReverseIndexOrphans(ctx, tenantID, checkpoint); err != nil {
		return err
	}
	if !checkpoint.options.SkipEntityRecordCleanup {
		deleted, _, err := s.cleanupEntityRecordsLocked(ctx, tenantID, checkpoint)
		report.DeletedEntityRecords += deleted
		if err != nil {
			return err
		}
	}
	prefix := s.parquetVersionRootPrefix(tenantID)
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil || skip {
		return err
	}
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		return checkpoint.pauseAfterPage(next)
	}
	if err != nil {
		return err
	}
	keep := indexObjectKeys(s, tenantID, catalog)
	for _, object := range objects {
		if err := checkpoint.visit(object); err != nil {
			return err
		}
		if _, referenced := keep[object.Key]; referenced {
			continue
		}
		version, ok := s.parquetVersionFromKey(tenantID, object.Key)
		if !ok || version >= catalog.Version || !strings.HasSuffix(object.Key, ".parquet") {
			continue
		}
		// A pinned orphan cannot be removed in this pass. Retire it before
		// decoding so active queries do not pay for repeated, unusable GC work.
		// Delete rechecks the view in case roots change during validation.
		if !checkpoint.options.view.canDelete(object.Key) {
			checkpoint.checkpoint.ScannedKeys++
			checkpoint.checkpoint.LastKey = object.Key
			checkpoint.checkpoint.DeferredFiles++
			continue
		}
		source, err := openFileReader(ctx, s.Objects, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		safe := s.listedParquetReaderSafeToDelete(ctx, tenantID, object.Key, source, version, catalog.Version)
		closeErr := source.Close()
		if closeErr != nil {
			return closeErr
		}
		if !safe {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		if _, err := checkpoint.deleteKey(ctx, s.Objects, object.Key); err != nil {
			return err
		}
	}
	return checkpoint.pauseAfterPage(next)
}
