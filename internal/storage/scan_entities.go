package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) ListEntities(ctx context.Context, tenantID string, options EntityScanOptions) (EntityScanResult, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return EntityScanResult{}, err
	}
	options.normalize()
	cursorVersion, cursorCatalogHash, hasCursorVersion, err := scanCursorPinnedCatalog(options.Cursor)
	if err != nil {
		return EntityScanResult{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return EntityScanResult{}, err
	}
	if hasCursorVersion {
		if options.MinVersion > 0 && cursorVersion < options.MinVersion {
			return EntityScanResult{}, fmt.Errorf("cursor version %d is below required version %d", cursorVersion, options.MinVersion)
		}
		if catalog, err := s.GetIndexCatalogSnapshot(ctx, tenantID, cursorVersion, cursorCatalogHash); err == nil {
			cursor, err := parseScanCursor(options.Cursor, cursorVersion, entityScanQueryHash(options))
			if err != nil {
				return EntityScanResult{}, err
			}
			result, ok, err := s.listEntitiesFromPages(ctx, tenantID, cursorVersion, catalog, options, cursor)
			if err != nil {
				return EntityScanResult{}, err
			}
			if ok {
				return result, nil
			}
		} else if !errors.Is(err, ErrNotFound) {
			return EntityScanResult{}, err
		}
		if manifest.Version != cursorVersion {
			return EntityScanResult{}, fmt.Errorf("cursor version %d is no longer available", cursorVersion)
		}
	}
	catalog, indexedRead, err := s.scanCatalog(ctx, tenantID, manifest.Version)
	if err != nil {
		return EntityScanResult{}, err
	}
	scanVersion := manifest.Version
	if indexedRead {
		scanVersion = catalog.Version
	}
	if options.MinVersion > 0 && scanVersion < options.MinVersion {
		indexedRead = false
		scanVersion = manifest.Version
	}
	cursor, err := parseScanCursor(options.Cursor, scanVersion, entityScanQueryHash(options))
	if err != nil {
		return EntityScanResult{}, err
	}
	if indexedRead {
		result, ok, err := s.listEntitiesFromPages(ctx, tenantID, scanVersion, catalog, options, cursor)
		if err != nil {
			return EntityScanResult{}, err
		}
		if ok {
			return result, nil
		}
	}
	minVersion := manifest.Version
	if options.MinVersion > minVersion {
		minVersion = options.MinVersion
	}
	g, loaded, err := s.LoadAtLeast(ctx, tenantID, minVersion)
	if err != nil {
		return EntityScanResult{}, err
	}
	entities, next := pageEntities(sortedScanEntities(g.Entities), loaded.Version, options, cursor)
	return EntityScanResult{TenantID: tenantID, Version: loaded.Version, Entities: entities, NextCursor: next}, nil
}

func (s *TenantStore) listEntitiesFromPages(ctx context.Context, tenantID string, version int64, catalog IndexCatalog, options EntityScanOptions, cursor scanCursor) (EntityScanResult, bool, error) {
	items := make([]graph.Entity, 0, normalizedScanLimit(options.Limit)+1)
	shards := entityScanShards(catalog, options.Shard)
	for i, shard := range shards {
		spec, ok := entityPageSpec(catalog, shard)
		if !ok {
			return EntityScanResult{}, false, nil
		}
		if specFormat(spec.Format) == IndexFormatParquet {
			if i+1 < len(shards) {
				if next, ok := entityPageSpec(catalog, shards[i+1]); ok && specFormat(next.Format) == IndexFormatParquet {
					s.prefetchParquetEntityPageObject(ctx, tenantID, version, next)
				}
			}
			data, meta, ok, cached, err := s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
			if err != nil || !ok {
				return EntityScanResult{}, ok, err
			}
			candidates := map[string]struct{}{}
			if shouldScanParquetEntityCandidates(options, cursor) {
				scan, err := scanParquetEntityPageCandidates(ctx, data, spec.Shard, options, cursor)
				if err != nil {
					return EntityScanResult{}, false, err
				}
				if len(scan.IDs) == 0 {
					continue
				}
				candidates = scan.IDs
			}
			page, err := decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, version)
			if (err != nil || !entityPageReadable(page, tenantID, version, spec)) && cached {
				key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
				s.dropCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
				data, meta, ok, _, err = s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
				if err != nil || !ok {
					return EntityScanResult{}, ok, err
				}
				if shouldScanParquetEntityCandidates(options, cursor) {
					scan, err := scanParquetEntityPageCandidates(ctx, data, spec.Shard, options, cursor)
					if err != nil {
						return EntityScanResult{}, false, err
					}
					if len(scan.IDs) == 0 {
						continue
					}
					candidates = scan.IDs
				}
				page, err = decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, version)
			}
			if err != nil {
				return EntityScanResult{}, false, err
			}
			if !entityPageReadable(page, tenantID, version, spec) {
				return EntityScanResult{}, false, nil
			}
			key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
			s.putCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash, data, meta)
			for _, entity := range page.Entities {
				if len(candidates) > 0 {
					if _, ok := candidates[entity.ID]; !ok {
						continue
					}
				}
				if !entityMatchesScan(entity, options) || scanKey(shard, entity.ID) <= cursor.After {
					continue
				}
				items = append(items, entity)
				if len(items) > normalizedScanLimit(options.Limit) {
					return entityScanPage(tenantID, version, items, options, true, scanCatalogContentHash(catalog)), true, nil
				}
			}
			continue
		}
		return EntityScanResult{}, false, nil
	}
	return entityScanPage(tenantID, version, items, options, false, scanCatalogContentHash(catalog)), true, nil
}

func pageEntities(candidates []graph.Entity, version int64, options EntityScanOptions, cursor scanCursor) ([]graph.Entity, string) {
	items := make([]graph.Entity, 0, normalizedScanLimit(options.Limit)+1)
	for _, entity := range candidates {
		key := scanKey(entityShardID(entity.ID), entity.ID)
		if !entityMatchesScan(entity, options) || key <= cursor.After {
			continue
		}
		items = append(items, entity)
		if len(items) > normalizedScanLimit(options.Limit) {
			break
		}
	}
	page := entityScanPage("", version, items, options, len(items) > normalizedScanLimit(options.Limit), "")
	return page.Entities, page.NextCursor
}

func entityScanPage(tenantID string, version int64, items []graph.Entity, options EntityScanOptions, hasMore bool, catalogHash string) EntityScanResult {
	limit := normalizedScanLimit(options.Limit)
	page := items
	if len(page) > limit {
		page = page[:limit]
	}
	next := ""
	if hasMore && len(page) > 0 {
		last := page[len(page)-1]
		next = encodeScanCursor(scanCursor{Version: version, CatalogHash: catalogHash, After: scanKey(entityShardID(last.ID), last.ID), Query: entityScanQueryHash(options)})
	}
	return EntityScanResult{TenantID: tenantID, Version: version, Entities: append([]graph.Entity(nil), page...), NextCursor: next, IndexedRead: true}
}

func sortedScanEntities(entities map[string]graph.Entity) []graph.Entity {
	items := make([]graph.Entity, 0, len(entities))
	for _, entity := range entities {
		items = append(items, entity)
	}
	sort.Slice(items, func(i, j int) bool {
		left := scanKey(entityShardID(items[i].ID), items[i].ID)
		right := scanKey(entityShardID(items[j].ID), items[j].ID)
		return left < right
	})
	return items
}

func entityMatchesScan(entity graph.Entity, options EntityScanOptions) bool {
	if options.Kind != "" && entity.Kind != options.Kind {
		return false
	}
	if options.Shard != "" && !indexShardIDMatches(entity.ID, options.Shard) {
		return false
	}
	return entityMatchesSource(entity, options.Source)
}

func entityMatchesSource(entity graph.Entity, source string) bool {
	if source == "" {
		return true
	}
	if entity.Source == source || (entity.ExistenceSource != nil && entity.ExistenceSource.Source == source) {
		return true
	}
	for _, item := range entity.Sources {
		if item.Source == source {
			return true
		}
	}
	return false
}

func entityScanShards(catalog IndexCatalog, requested string) []string {
	if requested != "" {
		return []string{requested}
	}
	shards := make([]string, 0, len(catalog.EntityPages))
	for _, page := range catalog.EntityPages {
		shards = append(shards, page.Shard)
	}
	sort.Strings(shards)
	return shards
}

func entityPageSpec(catalog IndexCatalog, shard string) (EntityPageSpec, bool) {
	for _, spec := range catalog.EntityPages {
		if spec.Shard == shard {
			return spec, true
		}
	}
	return EntityPageSpec{}, false
}

func entityPageReadable(page EntityPageData, tenantID string, version int64, spec EntityPageSpec) bool {
	if !indexTenantMatches(page.TenantID, tenantID) || page.Shard != spec.Shard || page.Version > version {
		return false
	}
	return spec.ContentHash != "" && entityPageContentHash(page) == spec.ContentHash
}

func (options *EntityScanOptions) normalize() {
	options.Kind = strings.TrimSpace(options.Kind)
	options.Source = strings.TrimSpace(options.Source)
	options.Shard = strings.TrimSpace(options.Shard)
}
