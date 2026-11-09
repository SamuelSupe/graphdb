package storage

import (
	"context"
	"errors"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (l *PersistedIndexLookup) getEntityByIDFromParquetPage(ctx context.Context, id string, fields []string, spec EntityPageSpec) (graph.Entity, bool, error) {
	page, ok, err := l.loadParquetEntityPage(ctx, spec)
	if err != nil {
		if ctx.Err() != nil {
			return graph.Entity{}, false, err
		}
		return graph.Entity{}, false, nil
	}
	if !ok {
		return graph.Entity{}, false, nil
	}
	entity, ok := l.entityFromCachedPage(spec.Shard, id, page)
	if !ok {
		return graph.Entity{}, false, nil
	}
	return trimEntityFields(entity, fields), true, nil
}

func (l *PersistedIndexLookup) getEntityFromParquetPage(ctx context.Context, record EntityRecord, fields []string, spec EntityPageSpec) (graph.Entity, bool, error) {
	if record.Page != spec.Shard {
		return graph.Entity{}, false, nil
	}
	matched, err := l.entityRecordMatchesPage(ctx, record)
	if err != nil {
		if ctx.Err() != nil {
			return graph.Entity{}, false, err
		}
		return graph.Entity{}, false, nil
	}
	if !matched || record.Entity.ID != record.ID {
		return graph.Entity{}, false, nil
	}
	return trimEntityFields(record.Entity, fields), true, nil
}

func (l *PersistedIndexLookup) loadParquetEntityPage(ctx context.Context, spec EntityPageSpec) (EntityPageData, bool, error) {
	page, err := l.loadEntityPage(ctx, spec.Shard)
	if errors.Is(err, ErrNotFound) {
		return EntityPageData{}, false, nil
	}
	if err != nil {
		return EntityPageData{}, false, err
	}
	return page, true, nil
}

func (s *TenantStore) loadParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) (EntityPageData, string, bool, error) {
	var result EntityPageData
	var resultETag string
	ok, err := s.withParquetEntityPageObject(ctx, tenantID, version, spec, func(page EntityPageData, etag string, _ bool) error {
		result = copyEntityPage(page)
		resultETag = etag
		return nil
	})
	return result, resultETag, ok, err
}

// Lookup pages stay immutable. Copy only selected entities at the public lookup
// boundary instead of copying every entity before applying IDs and projections.
func (s *TenantStore) borrowValidatedParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) (EntityPageData, string, bool, error) {
	var result EntityPageData
	var resultETag string
	valid := false
	ok, err := s.withParquetEntityPageObject(ctx, tenantID, version, spec, func(page EntityPageData, etag string, validated bool) error {
		if validated {
			result = page
			resultETag = etag
			valid = true
		}
		return nil
	})
	return result, resultETag, ok && valid, err
}

func (s *TenantStore) withParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec, visit func(EntityPageData, string, bool) error) (bool, error) {
	if hasIndexRanges(spec.Objects) {
		page, ok, err := s.loadEntityRangePage(ctx, tenantID, version, spec)
		if err != nil || !ok {
			return ok, err
		}
		return true, visit(page, "", true)
	}
	traceStats := entityPageVisitTraceStatsFromContext(ctx)
	key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
	if entry, revalidate, ok := s.borrowCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash); ok {
		if traceStats != nil {
			traceStats.decodedCacheHits++
		}
		page, etag := entry.page, entry.etag
		cacheValid := true
		if revalidate {
			if traceStats != nil {
				traceStats.cacheRevalidations++
			}
			meta, err := objectMeta(ctx, s.Objects, key)
			if errors.Is(err, ErrNotFound) {
				if traceStats != nil {
					traceStats.cacheInvalidations++
				}
				s.dropCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash)
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if etag != "" && meta.ETag != etag {
				if traceStats != nil {
					traceStats.cacheInvalidations++
				}
				s.dropCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash)
				cacheValid = false
			} else {
				s.entityPageCache.markValidated(entityPageCacheKey(tenantID, version, key, spec.ContentHash, spec.SchemaHash), meta.ETag)
				etag = meta.ETag
			}
		}
		if cacheValid {
			// Pages enter this cache only after tenant/version/content/schema checks.
			// The immutable cache key plus ETag revalidation preserves that result,
			// so recomputing the full logical page hash on every hit is redundant.
			return true, visit(page, etag, true)
		}
	}
	if traceStats != nil {
		traceStats.decodedCacheMisses++
	}
	if exclusiveFileStore(s.Objects) != nil {
		loaded, err := s.loadLocalEntityPage(ctx, tenantID, version, spec, EntityScanOptions{}, scanCursor{})
		if err != nil || !loaded.available {
			return loaded.available, err
		}
		readable := entityPageReadable(loaded.page, tenantID, version, spec)
		if readable {
			s.putCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash, loaded.page, "")
		}
		return true, visit(loaded.page, "", readable)
	}
	data, meta, ok, cached, err := s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
	if err != nil || !ok {
		return ok, err
	}
	if traceStats != nil {
		if cached {
			traceStats.rawCacheHits++
		} else {
			traceStats.objectLoads++
		}
	}
	decodeStart := time.Now()
	page, err := decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, 0)
	if traceStats != nil {
		traceStats.parquetDecodes++
		traceStats.parquetDecodeDuration += time.Since(decodeStart)
	}
	readable := err == nil && entityPageReadable(page, tenantID, version, spec)
	if !readable && cached {
		s.dropCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
		var retryCached bool
		data, meta, ok, retryCached, err = s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
		if err != nil || !ok {
			return ok, err
		}
		if traceStats != nil {
			if retryCached {
				traceStats.rawCacheHits++
			} else {
				traceStats.objectLoads++
			}
		}
		decodeStart = time.Now()
		page, err = decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, 0)
		if traceStats != nil {
			traceStats.parquetDecodes++
			traceStats.parquetDecodeDuration += time.Since(decodeStart)
		}
		readable = err == nil && entityPageReadable(page, tenantID, version, spec)
	}
	if err != nil {
		return false, err
	}
	if readable {
		s.putCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash, data, meta)
		s.putCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash, page, meta.ETag)
	}
	return true, visit(page, meta.ETag, readable)
}

func (s *TenantStore) loadParquetEntityPageObjectBytes(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) ([]byte, ObjectMeta, bool, bool, error) {
	key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
	if data, meta, cached, err := s.cachedIndexObject(ctx, "entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash); err != nil {
		return nil, ObjectMeta{}, false, false, err
	} else if cached {
		return data, meta, true, true, nil
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, ObjectMeta{}, false, false, nil
	}
	if err != nil {
		return nil, ObjectMeta{}, false, false, err
	}
	return data, meta, true, false, nil
}

func (s *TenantStore) inspectParquetObject(ctx context.Context, data []byte, tenantID string, object IndexObject, item *IndexInspectionObject) {
	switch object.Codec {
	case parquetSecondaryIndexCodec:
		index, err := decodeParquetSecondaryIndex(
			ctx,
			data,
			tenantID,
			object.inspectKind,
			object.inspectField,
			0,
			object.inspectUnique,
		)
		if err != nil {
			item.InspectionError = err.Error()
			return
		}
		item.ObjectKind = "secondary-index"
		item.Codec = parquetSecondaryIndexCodec
		entryCount, _ := secondaryIndexCounts(index)
		item.RowCount = entryCount
		item.ContentHash = secondaryIndexContentHash(index)
	case parquetEdgeShardCodec:
		shard, err := decodeParquetEdgeShard(ctx, data, tenantID, object.inspectRelationType, object.inspectShard, 0)
		if err != nil {
			item.InspectionError = err.Error()
			return
		}
		item.ObjectKind = "edge-shard"
		item.Codec = parquetEdgeShardCodec
		item.RowCount = len(shard.Edges)
		item.ContentHash = edgeShardContentHash(shard)
	case parquetEntityPageCodec, "":
		page, err := decodeParquetEntityPage(ctx, data, tenantID, object.inspectShard, 0)
		if err != nil {
			item.InspectionError = err.Error()
			return
		}
		item.ObjectKind = "entity-page"
		item.Codec = parquetEntityPageCodec
		item.RowCount = len(page.Entities)
		item.ContentHash = entityPageContentHash(page)
	default:
		item.InspectionError = "unsupported parquet codec " + object.Codec
		return
	}
	item.HashMatches = object.ContentHash != "" && item.ContentHash == object.ContentHash
}
