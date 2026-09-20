package storage

import (
	"context"
	"errors"
)

func (s *TenantStore) cleanupReverseIndexOrphans(ctx context.Context, tenantID string, checkpoint *gcCheckpointRunner) error {
	referenced := map[string]struct{}{s.reverseIndexCatalogKey(tenantID): {}}
	catalog, err := s.GetReverseIndexCatalog(ctx, tenantID, 0)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if err == nil {
		for _, shard := range catalog.EdgeShards {
			for _, object := range shard.Objects {
				referenced[object.Key] = struct{}{}
			}
		}
	}
	objects, next, skip, err := checkpoint.listPage(ctx, s.Objects, s.reverseIndexPrefix(tenantID))
	if err != nil || skip {
		return err
	}
	for _, object := range objects {
		if _, keep := referenced[object.Key]; keep {
			continue
		}
		if _, err := checkpoint.deleteKey(ctx, s.Objects, object.Key); err != nil {
			return err
		}
	}
	return checkpoint.pauseAfterPage(next)
}
