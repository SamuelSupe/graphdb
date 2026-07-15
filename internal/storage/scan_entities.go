package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

func (s *TenantStore) ListEntities(ctx context.Context, tenantID string, options EntityScanOptions) (result EntityScanResult, err error) {
	stats := newEntityScanTraceStats()
	ctx, span := startStorageSpan(ctx, "graphdb.storage.scan.entities", entityScanTraceAttrs(tenantID, options)...)
	defer func() {
		span.SetAttributes(append(entityScanResultTraceAttrs(result), stats.attrs()...)...)
		endStorageSpan(span, err)
	}()
	if err := ValidateTenantID(tenantID); err != nil {
		return EntityScanResult{}, err
	}
	options.normalize()
	cursorVersion, cursorCatalogHash, hasCursorVersion, err := scanCursorPinnedCatalog(options.Cursor)
	if err != nil {
		return EntityScanResult{}, err
	}
	manifestCtx, manifestSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.get_manifest", tenantTraceAttr(tenantID))
	manifest, _, manifestErr := s.getManifest(manifestCtx, tenantID)
	manifestSpan.SetAttributes(attribute.Int64("graphdb.scan.manifest_version", manifest.Version))
	endStorageSpan(manifestSpan, manifestErr)
	if manifestErr != nil {
		return EntityScanResult{}, manifestErr
	}
	stats.manifestVersion = manifest.Version
	if hasCursorVersion {
		if options.MinVersion > 0 && cursorVersion < options.MinVersion {
			return EntityScanResult{}, fmt.Errorf("cursor version %d is below required version %d", cursorVersion, options.MinVersion)
		}
		catalogCtx, catalogSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.get_cursor_catalog",
			tenantTraceAttr(tenantID),
			attribute.Int64("graphdb.scan.cursor_version", cursorVersion),
		)
		catalog, catalogErr := s.GetIndexCatalogSnapshot(catalogCtx, tenantID, cursorVersion, cursorCatalogHash)
		catalogSpan.SetAttributes(
			attribute.Int64("graphdb.scan.catalog_version", catalog.Version),
			attribute.Int("graphdb.scan.catalog_pages", len(catalog.EntityPages)),
		)
		spanErr := catalogErr
		if errors.Is(catalogErr, ErrNotFound) {
			spanErr = nil
		}
		endStorageSpan(catalogSpan, spanErr)
		if catalogErr == nil {
			stats.path = "cursor_index_pages"
			stats.catalogVersion = catalog.Version
			stats.catalogPages = len(catalog.EntityPages)
			cursor, err := parseScanCursor(options.Cursor, cursorVersion, entityScanQueryHash(options))
			if err != nil {
				return EntityScanResult{}, err
			}
			result, ok, err := s.listEntitiesFromPages(ctx, tenantID, cursorVersion, catalog, options, cursor, stats)
			if err != nil {
				return EntityScanResult{}, err
			}
			if ok {
				return result, nil
			}
			stats.fallbackReason = "cursor_page_unavailable"
		} else if !errors.Is(catalogErr, ErrNotFound) {
			return EntityScanResult{}, catalogErr
		}
		if manifest.Version != cursorVersion {
			return EntityScanResult{}, fmt.Errorf("cursor version %d is no longer available", cursorVersion)
		}
	}
	catalogCtx, catalogSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.get_catalog",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.scan.max_version", manifest.Version),
	)
	catalog, indexedRead, catalogErr := s.scanCatalog(catalogCtx, tenantID, manifest.Version)
	catalogSpan.SetAttributes(
		attribute.Bool("graphdb.scan.catalog_indexed", indexedRead),
		attribute.Int64("graphdb.scan.catalog_version", catalog.Version),
		attribute.Int("graphdb.scan.catalog_pages", len(catalog.EntityPages)),
	)
	endStorageSpan(catalogSpan, catalogErr)
	if catalogErr != nil {
		return EntityScanResult{}, catalogErr
	}
	stats.catalogVersion = catalog.Version
	stats.catalogPages = len(catalog.EntityPages)
	scanVersion := manifest.Version
	if indexedRead {
		scanVersion = catalog.Version
	} else if stats.fallbackReason == "" {
		stats.fallbackReason = "catalog_unavailable_or_newer"
	}
	if options.MinVersion > 0 && scanVersion < options.MinVersion {
		indexedRead = false
		scanVersion = manifest.Version
		stats.fallbackReason = "catalog_below_min_version"
	}
	cursor, err := parseScanCursor(options.Cursor, scanVersion, entityScanQueryHash(options))
	if err != nil {
		return EntityScanResult{}, err
	}
	if indexedRead {
		stats.path = "index_pages"
		result, ok, err := s.listEntitiesFromPages(ctx, tenantID, scanVersion, catalog, options, cursor, stats)
		if err != nil {
			return EntityScanResult{}, err
		}
		if ok {
			return result, nil
		}
		stats.fallbackReason = "index_page_unavailable"
	}
	stats.path = "graph_fallback"
	minVersion := manifest.Version
	if options.MinVersion > minVersion {
		minVersion = options.MinVersion
	}
	loadCtx, loadSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.load_graph",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.scan.required_version", minVersion),
	)
	g, loaded, loadErr := s.LoadAtLeast(loadCtx, tenantID, minVersion)
	if g != nil {
		loadSpan.SetAttributes(attribute.Int("graphdb.scan.loaded_entities", len(g.Entities)))
	}
	loadSpan.SetAttributes(attribute.Int64("graphdb.scan.loaded_version", loaded.Version))
	endStorageSpan(loadSpan, loadErr)
	if loadErr != nil {
		return EntityScanResult{}, loadErr
	}
	_, sortSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.sort_fallback",
		attribute.Int("graphdb.scan.input_entities", len(g.Entities)),
	)
	entities, next := pageEntities(sortedScanEntities(g.Entities), loaded.Version, options, cursor)
	sortSpan.SetAttributes(attribute.Int("graphdb.scan.returned", len(entities)))
	endStorageSpan(sortSpan, nil)
	return EntityScanResult{TenantID: tenantID, Version: loaded.Version, Entities: entities, NextCursor: next}, nil
}

type loadedEntityScanPage struct {
	page       EntityPageData
	data       []byte
	meta       ObjectMeta
	candidates map[string]struct{}
	available  bool
	cached     bool
	skip       bool
}

type entityScanObjectBytes struct {
	data   []byte
	meta   ObjectMeta
	cached bool
}

func (s *TenantStore) listEntitiesFromPages(ctx context.Context, tenantID string, version int64, catalog IndexCatalog, options EntityScanOptions, cursor scanCursor, stats *entityScanTraceStats) (result EntityScanResult, available bool, err error) {
	if stats == nil {
		stats = newEntityScanTraceStats()
	}
	ctx, span := startStorageSpan(ctx, "graphdb.storage.scan.entities.pages",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.scan.version", version),
		attribute.Int("graphdb.scan.catalog_pages", len(catalog.EntityPages)),
	)
	decodeStats := &parquetDecodeTraceStats{}
	ctx = withParquetDecodeTraceStats(ctx, decodeStats)
	defer func() {
		admissions, wait := decodeStats.snapshot()
		stats.parquetAdmissions += admissions
		stats.parquetAdmissionWait += wait
		span.SetAttributes(append(entityScanResultTraceAttrs(result), stats.attrs()...)...)
		span.SetAttributes(attribute.Bool("graphdb.scan.pages_available", available))
		endStorageSpan(span, err)
	}()

	compiled, err := s.compiledScanCatalog(tenantID, catalog, cursor.CatalogHash)
	if err != nil {
		return EntityScanResult{}, false, err
	}
	items := make([]graph.Entity, 0, normalizedScanLimit(options.Limit)+1)
	shards := compiled.entityTargets(options.Shard)
	stats.shardsTotal = len(shards)
	specs := compiled.entitySpecs
	shards = shards[entityScanStart(shards, cursor.After):]
	objectRefCounts := make(map[string]int)
	for _, shard := range shards {
		if spec, ok := specs[shard]; ok && specFormat(spec.Format) == IndexFormatParquet {
			key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
			objectRefCounts[key]++
		}
	}
	candidateScans := make(map[string]parquetEntityCandidateScan)
	objectBytes := make(map[string]entityScanObjectBytes)
	consumePage := func(page EntityPageData, shard string, candidates map[string]struct{}, candidateFiltered bool) bool {
		for _, entity := range page.Entities {
			stats.entitiesExamined++
			if candidateFiltered {
				if _, ok := candidates[entity.ID]; !ok {
					continue
				}
			}
			if !entityMatchesScan(entity, options) || scanKey(shard, entity.ID) <= cursor.After {
				continue
			}
			items = append(items, entity)
			if len(items) > normalizedScanLimit(options.Limit) {
				stats.earlyStop = true
				return true
			}
		}
		return false
	}
	loadPage := func(spec EntityPageSpec, key string) (loaded loadedEntityScanPage, loadErr error) {
		reusePhysicalObject := objectRefCounts[key] > 1
		if object, ok := objectBytes[key]; reusePhysicalObject && ok {
			loaded.data = object.data
			loaded.meta = object.meta
			loaded.cached = object.cached
			loaded.available = true
			stats.physicalObjectReuses++
		} else {
			loaded.data, loaded.meta, loaded.available, loaded.cached, loadErr = s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
			if loadErr != nil || !loaded.available {
				return loaded, loadErr
			}
			if reusePhysicalObject {
				objectBytes[key] = entityScanObjectBytes{data: loaded.data, meta: loaded.meta, cached: loaded.cached}
			}
			if loaded.cached {
				stats.rawCacheHits++
			} else {
				stats.objectLoads++
			}
		}
		if shouldScanParquetEntityCandidates(options, cursor) {
			stats.candidateFilterRequests++
			scan, ok := candidateScans[key]
			if !reusePhysicalObject {
				ok = false
			}
			if !ok {
				candidateCtx, candidateSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.candidate_filter",
					attribute.Int("graphdb.scan.object_bytes", len(loaded.data)),
				)
				started := time.Now()
				scan, loadErr = scanParquetEntityObjectCandidates(candidateCtx, loaded.data, options)
				duration := time.Since(started)
				candidateSpan.SetAttributes(
					attribute.Int("graphdb.scan.candidate_ids_matched", len(scan.IDs)),
					attribute.Int("graphdb.scan.row_groups_read", scan.RowGroupsRead),
					attribute.Int("graphdb.scan.row_groups_skipped", scan.RowGroupsSkipped),
				)
				endStorageSpan(candidateSpan, loadErr)
				if loadErr != nil {
					return loaded, loadErr
				}
				if reusePhysicalObject {
					candidateScans[key] = scan
				}
				stats.candidateObjectScans++
				stats.candidateRowGroupsRead += scan.RowGroupsRead
				stats.candidateRowGroupsSkip += scan.RowGroupsSkipped
				stats.candidateIDsMatched += len(scan.IDs)
				stats.candidateScanDuration += duration
			} else {
				stats.candidateScanReuses++
			}
			loaded.candidates = filterParquetEntityCandidates(scan, spec.Shard, cursor)
			stats.candidateIDsSelected += len(loaded.candidates)
			if len(loaded.candidates) == 0 {
				loaded.skip = true
				return loaded, nil
			}
		}
		decodeCtx, decodeSpan := startStorageSpan(ctx, "graphdb.storage.scan.entities.decode_page",
			attribute.String("graphdb.scan.shard", spec.Shard),
			attribute.Int("graphdb.scan.object_bytes", len(loaded.data)),
		)
		started := time.Now()
		loaded.page, loadErr = decodeParquetEntityPage(decodeCtx, loaded.data, tenantID, spec.Shard, 0)
		duration := time.Since(started)
		stats.parquetDecodes++
		stats.parquetDecodeDuration += duration
		decodeSpan.SetAttributes(
			attribute.Int("graphdb.scan.decoded_entities", len(loaded.page.Entities)),
			attribute.Bool("graphdb.scan.page_readable", loadErr == nil && entityPageReadable(loaded.page, tenantID, version, spec)),
		)
		endStorageSpan(decodeSpan, loadErr)
		return loaded, loadErr
	}

	for i, shard := range shards {
		stats.shardsVisited++
		spec, ok := specs[shard]
		if !ok {
			return EntityScanResult{}, false, nil
		}
		if specFormat(spec.Format) == IndexFormatParquet {
			key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
			stats.markObject(key)
			if i+1 < len(shards) {
				if next, ok := specs[shards[i+1]]; ok && specFormat(next.Format) == IndexFormatParquet {
					nextKey := firstIndexObjectKey(next.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, next.Shard))
					if nextKey != key {
						s.prefetchParquetEntityPageObject(ctx, tenantID, version, next)
					}
				}
			}
			if entry, revalidate, hit := s.borrowCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash); hit {
				stats.decodedCacheHits++
				if !revalidate {
					if consumePage(entry.page, shard, nil, false) {
						return entityScanPage(tenantID, version, items, options, true, compiled.contentHash), true, nil
					}
					continue
				}
				stats.cacheRevalidations++
				pageReadable := false
				done := false
				ok, err := s.withParquetEntityPageObject(ctx, tenantID, version, spec, func(page EntityPageData, _ string, validated bool) error {
					if !validated {
						return nil
					}
					pageReadable = true
					done = consumePage(page, shard, nil, false)
					return nil
				})
				if err != nil || !ok {
					return EntityScanResult{}, ok, err
				}
				if !pageReadable {
					return EntityScanResult{}, false, nil
				}
				if done {
					return entityScanPage(tenantID, version, items, options, true, compiled.contentHash), true, nil
				}
				continue
			}
			stats.decodedCacheMisses++
			loaded, err := loadPage(spec, key)
			if !loaded.available || (err != nil && !loaded.cached) {
				return EntityScanResult{}, loaded.available, err
			}
			if loaded.skip {
				continue
			}
			if (err != nil || !entityPageReadable(loaded.page, tenantID, version, spec)) && loaded.cached {
				s.dropCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
				delete(candidateScans, key)
				delete(objectBytes, key)
				loaded, err = loadPage(spec, key)
				if err != nil || !loaded.available {
					return EntityScanResult{}, loaded.available, err
				}
				if loaded.skip {
					continue
				}
			}
			if err != nil {
				return EntityScanResult{}, false, err
			}
			if !entityPageReadable(loaded.page, tenantID, version, spec) {
				return EntityScanResult{}, false, nil
			}
			s.putCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash, loaded.data, loaded.meta)
			s.putCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash, loaded.page, loaded.meta.ETag)
			if consumePage(loaded.page, shard, loaded.candidates, shouldScanParquetEntityCandidates(options, cursor)) {
				return entityScanPage(tenantID, version, items, options, true, compiled.contentHash), true, nil
			}
			continue
		}
		return EntityScanResult{}, false, nil
	}
	return entityScanPage(tenantID, version, items, options, false, compiled.contentHash), true, nil
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
