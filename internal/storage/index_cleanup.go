package storage

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
)

func (s *TenantStore) cleanupObsoleteIndexObjects(ctx context.Context, tenantID string, previous IndexCatalog, current IndexCatalog) error {
	keep := indexObjectKeys(s, tenantID, current)
	previousTargets := indexObjectTargets(s, tenantID, previous)
	for key := range indexObjectKeys(s, tenantID, previous) {
		if _, ok := keep[key]; !ok {
			if err := s.deleteObsoleteIndexObjectIfSafe(ctx, previousTargets[key], current.Version); err != nil {
				return err
			}
		}
	}
	for _, prefix := range []string{s.secondaryIndexPrefix(tenantID), s.edgeShardPrefix(tenantID), s.entityPagePrefix(tenantID), s.parquetVersionRootPrefix(tenantID)} {
		err := scanObjectPrefix(
			ctx, s.Objects, prefix,
			func(objects []ObjectInfo) error {
				for _, object := range objects {
					if _, ok := keep[object.Key]; ok {
						continue
					}
					if err := s.deleteListedObsoleteIndexObjectIfSafe(
						ctx, tenantID, object.Key, current.Version,
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) cleanupCatalogObjectsRemovedFromCurrent(ctx context.Context, tenantID string, previous IndexCatalog, current IndexCatalog) error {
	keep := indexObjectKeys(s, tenantID, current)
	targets := indexObjectTargets(s, tenantID, previous)
	for key, target := range targets {
		if _, ok := keep[key]; ok {
			continue
		}
		if err := s.deleteObsoleteIndexObjectIfSafe(ctx, target, current.Version); err != nil {
			return err
		}
	}
	return nil
}

type indexObjectTarget struct {
	Key          string
	Type         string
	Immutable    bool
	TenantID     string
	Kind         string
	Field        string
	Unique       bool
	RelationType string
	Shard        string
	ContentHash  string
}

func indexObjectTargets(s *TenantStore, tenantID string, catalog IndexCatalog) map[string]indexObjectTarget {
	targets := map[string]indexObjectTarget{}
	for _, index := range catalog.Indexes {
		if specFormat(index.Format) == IndexFormatParquet && len(index.Objects) > 0 {
			for _, object := range index.Objects {
				_, immutable := s.parquetVersionFromKey(tenantID, object.Key)
				targets[object.Key] = indexObjectTarget{Key: object.Key, Type: "parquet_secondary_index", Immutable: immutable, TenantID: tenantID, Kind: index.Kind, Field: index.Field, Unique: secondaryIndexSpecUnique(index), ContentHash: object.ContentHash}
			}
		}
	}
	for _, shard := range catalog.EdgeShards {
		if specFormat(shard.Format) == IndexFormatParquet && len(shard.Objects) > 0 {
			for _, object := range shard.Objects {
				_, immutable := s.parquetVersionFromKey(tenantID, object.Key)
				targets[object.Key] = indexObjectTarget{Key: object.Key, Type: "parquet_edge_shard", Immutable: immutable, TenantID: tenantID, RelationType: shard.RelationType, Shard: shard.Shard, ContentHash: object.ContentHash}
			}
		}
	}
	for _, page := range catalog.EntityPages {
		if specFormat(page.Format) == IndexFormatParquet && len(page.Objects) > 0 {
			for _, object := range page.Objects {
				_, immutable := s.parquetVersionFromKey(tenantID, object.Key)
				targets[object.Key] = indexObjectTarget{Key: object.Key, Type: "parquet_entity_page", Immutable: immutable, TenantID: tenantID, Shard: page.Shard, ContentHash: object.ContentHash}
			}
		}
	}
	return targets
}

func (s *TenantStore) deleteObsoleteIndexObjectIfSafe(ctx context.Context, target indexObjectTarget, currentVersion int64) error {
	if target.Key == "" {
		return nil
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, target.Key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.deleteObsoleteIndexObjectDataIfSafe(ctx, target, currentVersion, data, meta)
}

func (s *TenantStore) deleteObsoleteIndexObjectDataIfSafe(ctx context.Context, target indexObjectTarget, currentVersion int64, data []byte, meta ObjectMeta) error {
	ok := obsoleteIndexObjectSafeToDelete(data, target, currentVersion)
	if !ok {
		return nil
	}
	if target.Immutable {
		return s.Objects.Delete(ctx, target.Key)
	}
	err := s.Objects.DeleteConditional(ctx, target.Key, PutCondition{IfMatch: meta.ETag})
	if errors.Is(err, ErrConditionalDeleteUnsupported) {
		return s.deleteObsoleteIndexObjectWithLease(ctx, target, currentVersion, data)
	}
	if errors.Is(err, ErrConflict) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (s *TenantStore) deleteListedObsoleteIndexObjectIfSafe(ctx context.Context, tenantID string, key string, currentVersion int64) error {
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	target, ok := s.indexObjectTargetFromContent(tenantID, key, data, currentVersion)
	if !ok {
		return nil
	}
	return s.deleteObsoleteIndexObjectDataIfSafe(ctx, target, currentVersion, data, meta)
}

func (s *TenantStore) deleteObsoleteIndexObjectWithLease(ctx context.Context, target indexObjectTarget, currentVersion int64, expected []byte) error {
	if err := s.ensureIncrementalIndexCurrent(ctx, target.TenantID, currentVersion); err != nil {
		return err
	}
	latest, err := s.Objects.Get(ctx, target.Key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(latest, expected) || !obsoleteIndexObjectSafeToDelete(latest, target, currentVersion) {
		return nil
	}
	if err := s.Objects.Delete(ctx, target.Key); err != nil {
		return err
	}
	return s.ensureIncrementalIndexCurrent(ctx, target.TenantID, currentVersion)
}

func obsoleteIndexObjectSafeToDelete(data []byte, target indexObjectTarget, currentVersion int64) bool {
	switch target.Type {
	case "parquet_index_catalog":
		catalog, err := decodeParquetIndexCatalog(context.Background(), data)
		if err != nil {
			return false
		}
		if !indexTenantMatches(catalog.TenantID, target.TenantID) ||
			!indexObjectVersionSafeToDelete(catalog.Version, currentVersion, target.Immutable) {
			return false
		}
		hash, err := indexCatalogContentHash(catalog)
		return err == nil && target.ContentHash != "" && hash == target.ContentHash
	case "parquet_secondary_index_pack":
		index, err := decodeParquetSecondaryIndex(context.Background(), data, target.TenantID, target.Kind, target.Field, 0, target.Unique)
		if err != nil {
			return false
		}
		if !indexTenantMatches(index.TenantID, target.TenantID) ||
			!indexObjectVersionSafeToDelete(index.Version, currentVersion, target.Immutable) ||
			index.Kind != target.Kind || index.Field != target.Field {
			return false
		}
		return target.ContentHash != "" && secondaryIndexContentHash(index) == target.ContentHash
	case "parquet_secondary_index":
		index, err := decodeParquetSecondaryIndex(context.Background(), data, target.TenantID, target.Kind, target.Field, 0, target.Unique)
		if err != nil {
			return false
		}
		if !indexTenantMatches(index.TenantID, target.TenantID) ||
			!indexObjectVersionSafeToDelete(index.Version, currentVersion, target.Immutable) {
			return false
		}
		if index.Kind != target.Kind || index.Field != target.Field {
			return false
		}
		return target.ContentHash != "" && secondaryIndexContentHash(index) == target.ContentHash
	case "parquet_edge_shard_pack":
		shard, err := decodeParquetEdgeShard(context.Background(), data, target.TenantID, target.RelationType, "", 0)
		if err != nil {
			return false
		}
		if !indexTenantMatches(shard.TenantID, target.TenantID) ||
			!indexObjectVersionSafeToDelete(shard.Version, currentVersion, target.Immutable) ||
			shard.RelationType != target.RelationType {
			return false
		}
		return target.ContentHash != "" && edgeShardContentHash(shard) == target.ContentHash
	case "parquet_edge_shard":
		shard, err := decodeParquetEdgeShard(context.Background(), data, target.TenantID, target.RelationType, target.Shard, 0)
		if err != nil {
			return false
		}
		if !indexTenantMatches(shard.TenantID, target.TenantID) ||
			!indexObjectVersionSafeToDelete(shard.Version, currentVersion, target.Immutable) {
			return false
		}
		if shard.RelationType != target.RelationType || shard.Shard != target.Shard {
			return false
		}
		return target.ContentHash != "" && edgeShardContentHash(shard) == target.ContentHash
	case "parquet_entity_page_pack":
		page, err := decodeParquetEntityPage(context.Background(), data, target.TenantID, "", 0)
		if err != nil {
			return false
		}
		if !indexTenantMatches(page.TenantID, target.TenantID) ||
			!indexObjectVersionSafeToDelete(page.Version, currentVersion, target.Immutable) {
			return false
		}
		return target.ContentHash != "" && entityPageContentHash(page) == target.ContentHash
	case "parquet_entity_page":
		page, err := decodeParquetEntityPage(context.Background(), data, target.TenantID, target.Shard, 0)
		if err != nil {
			return false
		}
		if !indexTenantMatches(page.TenantID, target.TenantID) {
			return false
		}
		if !indexObjectVersionSafeToDelete(page.Version, currentVersion, target.Immutable) ||
			page.Shard != target.Shard {
			return false
		}
		return target.ContentHash != "" && entityPageContentHash(page) == target.ContentHash
	default:
		return false
	}
}

func indexObjectVersionSafeToDelete(version int64, currentVersion int64, immutable bool) bool {
	if version > currentVersion {
		return false
	}
	return !immutable || version < currentVersion
}

func (s *TenantStore) indexObjectTargetFromContent(tenantID string, key string, data []byte, currentVersion int64) (indexObjectTarget, bool) {
	if target, ok := s.versionedParquetObjectTarget(tenantID, key, data, currentVersion); ok {
		return target, true
	}

	return indexObjectTarget{}, false
}

func (s *TenantStore) versionedParquetObjectTarget(tenantID string, key string, data []byte, currentVersion int64) (indexObjectTarget, bool) {
	if !strings.HasSuffix(key, ".parquet") {
		return indexObjectTarget{}, false
	}
	version, ok := s.parquetVersionFromKey(tenantID, key)
	if !ok || version > currentVersion {
		return indexObjectTarget{}, false
	}
	if catalog, err := decodeParquetIndexCatalog(context.Background(), data); err == nil {
		if !indexTenantMatches(catalog.TenantID, tenantID) || catalog.Version != version || catalog.Version > currentVersion {
			return indexObjectTarget{}, false
		}
		hash, err := indexCatalogContentHash(catalog)
		if err != nil {
			return indexObjectTarget{}, false
		}
		return indexObjectTarget{Key: key, Type: "parquet_index_catalog", Immutable: true, TenantID: tenantID, ContentHash: hash}, true
	}
	kind, field, _ := s.parquetSecondaryIndexIdentityFromKey(tenantID, key)
	if index, err := decodeParquetSecondaryIndex(context.Background(), data, tenantID, kind, field, version, false); err == nil && index.Kind != "" && index.Field != "" {
		if !indexTenantMatches(index.TenantID, tenantID) || index.Version > currentVersion {
			return indexObjectTarget{}, false
		}
		targetType := "parquet_secondary_index"
		if strings.Contains(key, "/shards/pack_") {
			targetType = "parquet_secondary_index_pack"
		}
		return indexObjectTarget{Key: key, Type: targetType, Immutable: true, TenantID: tenantID, Kind: index.Kind, Field: index.Field, Unique: index.Unique, ContentHash: secondaryIndexContentHash(index)}, true
	}
	if shard, err := decodeParquetEdgeShard(context.Background(), data, tenantID, "", "", version); err == nil && shard.RelationType != "" && shard.Shard != "" {
		if !indexTenantMatches(shard.TenantID, tenantID) || shard.Version > currentVersion {
			return indexObjectTarget{}, false
		}
		targetType := "parquet_edge_shard"
		if strings.Contains(key, "/packs/") {
			targetType = "parquet_edge_shard_pack"
		}
		return indexObjectTarget{Key: key, Type: targetType, Immutable: true, TenantID: tenantID, RelationType: shard.RelationType, Shard: shard.Shard, ContentHash: edgeShardContentHash(shard)}, true
	}
	page, err := decodeParquetEntityPage(context.Background(), data, tenantID, "", version)
	if err != nil {
		return indexObjectTarget{}, false
	}
	if !indexTenantMatches(page.TenantID, tenantID) || page.Version > currentVersion || page.Shard == "" {
		return indexObjectTarget{}, false
	}
	targetType := "parquet_entity_page"
	if strings.Contains(key, "/pages/packs/") {
		targetType = "parquet_entity_page_pack"
	}
	return indexObjectTarget{Key: key, Type: targetType, Immutable: true, TenantID: tenantID, Shard: page.Shard, ContentHash: entityPageContentHash(page)}, true
}

func (s *TenantStore) parquetVersionFromKey(tenantID string, key string) (int64, bool) {
	prefix := s.parquetVersionRootPrefix(tenantID) + "v"
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(key, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return 0, false
	}
	version, err := strconv.ParseInt(rest[:slash], 10, 64)
	return version, err == nil
}

func indexObjectKeys(s *TenantStore, tenantID string, catalog IndexCatalog) map[string]struct{} {
	keys := map[string]struct{}{}
	if catalog.Version > 0 {
		keys[s.indexCatalogVersionKey(tenantID, catalog.Version)] = struct{}{}
		if hash, err := indexCatalogContentHash(catalog); err == nil && hash != "" {
			keys[s.indexCatalogVersionHashKey(tenantID, catalog.Version, hash)] = struct{}{}
		}
	}
	for _, index := range catalog.Indexes {
		if len(index.Objects) > 0 {
			for _, object := range index.Objects {
				keys[object.Key] = struct{}{}
			}
		}
	}
	for _, shard := range catalog.EdgeShards {
		if len(shard.Objects) > 0 {
			for _, object := range shard.Objects {
				keys[object.Key] = struct{}{}
			}
		}
	}
	for _, page := range catalog.EntityPages {
		if len(page.Objects) > 0 {
			for _, object := range page.Objects {
				keys[object.Key] = struct{}{}
			}
		}
	}
	return keys
}
