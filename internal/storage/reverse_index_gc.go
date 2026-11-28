package storage

import (
	"context"
	"errors"
	"strings"
)

func (s *TenantStore) cleanupReverseIndexOrphans(ctx context.Context, tenantID string, checkpoint *gcCheckpointRunner) error {
	catalog, _, err := s.readReverseIndexCatalog(ctx, unwrapTenantMigrationStore(s.Objects), tenantID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	referenced := map[string]struct{}{s.reverseIndexCatalogKey(tenantID): {}}
	for _, shard := range catalog.EdgeShards {
		for _, object := range shard.Objects {
			referenced[object.Key] = struct{}{}
		}
	}
	prefix := s.reverseIndexPrefix(tenantID)
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, prefix)
	if err != nil || skip {
		return err
	}
	for _, object := range objects {
		if err := checkpoint.visit(object); err != nil {
			return err
		}
		if _, keep := referenced[object.Key]; keep {
			continue
		}
		version, ok := versionFromObjectKey(prefix, object.Key)
		if !ok || version >= catalog.Version || !strings.HasSuffix(object.Key, ".parquet") {
			continue
		}
		if !checkpoint.options.view.canDelete(object.Key) {
			checkpoint.checkpoint.ScannedKeys++
			checkpoint.checkpoint.LastKey = object.Key
			checkpoint.checkpoint.DeferredFiles++
			if checkpoint.options.view.stale() {
				checkpoint.pause()
				return errGCPaused
			}
			continue
		}
		if _, err := checkpoint.deleteKey(ctx, s.Objects, object.Key); err != nil {
			return err
		}
	}
	return checkpoint.pauseAfterPage(next)
}
