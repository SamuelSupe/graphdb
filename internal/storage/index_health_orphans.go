package storage

import (
	"context"
	"errors"
	"strings"
)

func (s *TenantStore) checkOrphanIndexObjects(ctx context.Context, tenantID string, catalog IndexCatalog, health *IndexHealth) {
	keep := indexObjectKeys(s, tenantID, catalog)
	for _, prefix := range []string{s.secondaryIndexPrefix(tenantID), s.edgeShardPrefix(tenantID), s.entityPagePrefix(tenantID), s.parquetVersionRootPrefix(tenantID)} {
		err := scanObjectPrefix(
			ctx, s.Objects, prefix,
			func(objects []ObjectInfo) error {
				for _, object := range objects {
					if _, ok := keep[object.Key]; ok {
						continue
					}
					if version, ok := s.parquetVersionFromKey(
						tenantID, object.Key,
					); ok && version <= catalog.Version &&
						isVersionedIndexCatalogKey(object.Key) {
						continue
					}
					if version, ok := s.parquetVersionFromKey(
						tenantID, object.Key,
					); ok && version < catalog.Version {
						continue
					}
					if _, err := objectMeta(
						ctx, s.Objects, object.Key,
					); errors.Is(err, ErrNotFound) {
						continue
					} else if err != nil {
						health.Issues = append(
							health.Issues, err.Error(),
						)
						continue
					}
					health.Issues = append(
						health.Issues,
						"orphan index object "+object.Key,
					)
				}
				return nil
			},
		)
		if err != nil {
			health.Issues = append(health.Issues, err.Error())
		}
	}
}

func isVersionedIndexCatalogKey(key string) bool {
	return strings.HasSuffix(key, "/catalog.parquet") || strings.Contains(key, "/catalogs/")
}
