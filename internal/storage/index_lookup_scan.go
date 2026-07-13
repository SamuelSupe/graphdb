package storage

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

type entityPageVisitTraceKey struct{}

type entityPageVisitTraceStats struct {
	pageGroupsTotal          int
	pageGroupsVisited        int
	pageSpecsVisited         int
	pagesReadable            int
	physicalEntitiesExamined int
	kindMatched              int
	candidatesDelivered      int
	cursorSkipped            int
	decodedCacheHits         int
	decodedCacheMisses       int
	cacheRevalidations       int
	cacheInvalidations       int
	rawCacheHits             int
	objectLoads              int
	parquetDecodes           int
	parquetDecodeDuration    time.Duration
	earlyStop                bool
}

func withEntityPageVisitTraceStats(ctx context.Context, stats *entityPageVisitTraceStats) context.Context {
	return context.WithValue(ctx, entityPageVisitTraceKey{}, stats)
}

func entityPageVisitTraceStatsFromContext(ctx context.Context) *entityPageVisitTraceStats {
	stats, _ := ctx.Value(entityPageVisitTraceKey{}).(*entityPageVisitTraceStats)
	return stats
}

func (l *PersistedIndexLookup) ListEntities(ctx context.Context, kind string, fields []string) ([]graph.Entity, bool, error) {
	entities := make([]graph.Entity, 0)
	ok, err := l.VisitEntities(ctx, kind, fields, "", func(entity graph.Entity) (bool, error) {
		entities = append(entities, entity)
		return true, nil
	})
	return entities, ok, err
}

func (l *PersistedIndexLookup) VisitEntities(ctx context.Context, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (available bool, err error) {
	tenantID := ""
	version := int64(0)
	catalogPages := 0
	if l != nil {
		tenantID = l.TenantID
		version = l.Version
		catalogPages = len(l.Catalog.EntityPages)
	}
	ctx, span := startStorageSpan(ctx, "graphdb.storage.index_lookup.visit_entities",
		tenantTraceAttr(tenantID),
		attribute.Int64("graphdb.index_lookup.version", version),
		attribute.String("graphdb.index_lookup.kind", kind),
		attribute.Int("graphdb.index_lookup.fields", len(fields)),
		attribute.Bool("graphdb.index_lookup.cursor_present", afterID != ""),
		attribute.Int("graphdb.index_lookup.catalog_pages", catalogPages),
	)
	stats := &entityPageVisitTraceStats{}
	ctx = withEntityPageVisitTraceStats(ctx, stats)
	defer func() {
		span.SetAttributes(
			attribute.Bool("graphdb.index_lookup.available", available),
			attribute.Int("graphdb.index_lookup.page_groups_total", stats.pageGroupsTotal),
			attribute.Int("graphdb.index_lookup.page_groups_visited", stats.pageGroupsVisited),
			attribute.Int("graphdb.index_lookup.page_specs_visited", stats.pageSpecsVisited),
			attribute.Int("graphdb.index_lookup.pages_readable", stats.pagesReadable),
			attribute.Int("graphdb.index_lookup.physical_entities_examined", stats.physicalEntitiesExamined),
			attribute.Int("graphdb.index_lookup.kind_matched", stats.kindMatched),
			attribute.Int("graphdb.index_lookup.candidates_delivered", stats.candidatesDelivered),
			attribute.Int("graphdb.index_lookup.cursor_skipped", stats.cursorSkipped),
			attribute.Int("graphdb.index_lookup.decoded_cache_hits", stats.decodedCacheHits),
			attribute.Int("graphdb.index_lookup.decoded_cache_misses", stats.decodedCacheMisses),
			attribute.Int("graphdb.index_lookup.cache_revalidations", stats.cacheRevalidations),
			attribute.Int("graphdb.index_lookup.cache_invalidations", stats.cacheInvalidations),
			attribute.Int("graphdb.index_lookup.raw_cache_hits", stats.rawCacheHits),
			attribute.Int("graphdb.index_lookup.object_loads", stats.objectLoads),
			attribute.Int("graphdb.index_lookup.parquet_decodes", stats.parquetDecodes),
			attribute.Int64("graphdb.index_lookup.parquet_decode_ms", stats.parquetDecodeDuration.Milliseconds()),
			attribute.Bool("graphdb.index_lookup.early_stop", stats.earlyStop),
		)
		endStorageSpan(span, err)
	}()
	if l == nil || l.Catalog.Version != l.Version || visit == nil {
		return false, nil
	}
	groups := make(map[string][]EntityPageSpec)
	shards := make([]string, 0, len(l.Catalog.EntityPages))
	for _, spec := range l.Catalog.EntityPages {
		shard := currentEntityScanShard(spec.Shard)
		if _, ok := groups[shard]; !ok {
			shards = append(shards, shard)
		}
		groups[shard] = append(groups[shard], spec)
	}
	stats.pageGroupsTotal = len(groups)
	sort.Strings(shards)
	afterShard := ""
	if afterID != "" {
		afterShard = entityShardID(afterID)
	}
	for _, shard := range shards {
		if afterShard != "" && shard < afterShard {
			continue
		}
		if err := objectContextErr(ctx); err != nil {
			return false, err
		}
		stats.pageGroupsVisited++
		pageAfterID := ""
		if shard == afterShard {
			pageAfterID = afterID
		}
		ok, keepGoing, err := l.visitEntityPageGroup(ctx, groups[shard], kind, fields, pageAfterID, func(entity graph.Entity) (bool, error) {
			stats.candidatesDelivered++
			return visit(entity)
		})
		if err != nil || !ok {
			return ok, err
		}
		if !keepGoing {
			stats.earlyStop = true
			return true, nil
		}
	}
	return true, nil
}

func currentEntityScanShard(catalogShard string) string {
	if catalogShard == "default" {
		return catalogShard
	}
	value, err := strconv.ParseUint(catalogShard, 16, 8)
	if err != nil {
		return catalogShard
	}
	return fmt.Sprintf("%02x", value%indexShardBuckets)
}

func (l *PersistedIndexLookup) visitEntityPageGroup(ctx context.Context, specs []EntityPageSpec, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, bool, error) {
	if len(specs) == 1 {
		return l.visitEntitiesFromPage(ctx, specs[0], kind, fields, afterID, visit)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Shard < specs[j].Shard })
	entities := make([]graph.Entity, 0)
	for _, spec := range specs {
		ok, _, err := l.visitEntitiesFromPage(ctx, spec, kind, fields, "", func(entity graph.Entity) (bool, error) {
			entities = append(entities, entity)
			return true, nil
		})
		if err != nil || !ok {
			return ok, false, err
		}
	}
	sort.Slice(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })
	for _, entity := range entities {
		if afterID != "" && entity.ID < afterID {
			continue
		}
		keepGoing, err := visit(entity)
		if err != nil || !keepGoing {
			return true, keepGoing, err
		}
	}
	return true, true, nil
}

func (l *PersistedIndexLookup) visitEntitiesFromPage(ctx context.Context, spec EntityPageSpec, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, bool, error) {
	stats := entityPageVisitTraceStatsFromContext(ctx)
	if stats != nil {
		stats.pageSpecsVisited++
	}
	if specFormat(spec.Format) != IndexFormatParquet {
		return false, false, nil
	}
	readable := false
	keepGoing := true
	ok, err := l.Store.withParquetEntityPageObject(ctx, l.TenantID, l.Version, spec, func(page EntityPageData, _ string, validated bool) error {
		if !validated {
			return nil
		}
		readable = true
		if stats != nil {
			stats.pagesReadable++
		}
		for _, entity := range page.Entities {
			if err := objectContextErr(ctx); err != nil {
				return err
			}
			if stats != nil {
				stats.physicalEntitiesExamined++
			}
			if kind != "" && entity.Kind != kind {
				continue
			}
			if stats != nil {
				stats.kindMatched++
			}
			if afterID != "" && entity.ID < afterID {
				if stats != nil {
					stats.cursorSkipped++
				}
				continue
			}
			var err error
			keepGoing, err = visit(trimEntityFields(entity, fields))
			if err != nil || !keepGoing {
				return err
			}
		}
		return nil
	})
	if err != nil || !ok || !readable {
		return ok && readable, false, err
	}
	return true, keepGoing, nil
}

func (l *PersistedIndexLookup) listEntitiesFromPage(ctx context.Context, spec EntityPageSpec, kind string, fields []string) ([]graph.Entity, bool, error) {
	if specFormat(spec.Format) == IndexFormatParquet {
		return l.listParquetEntitiesFromPage(ctx, spec, kind, fields)
	}
	return nil, false, nil
}
