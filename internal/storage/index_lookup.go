package storage

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type PersistedIndexLookup struct {
	Store    *TenantStore
	TenantID string
	Version  int64
	Catalog  IndexCatalog

	pageMu    sync.Mutex
	pageCache map[string]EntityPageData
	pageIndex map[string]map[string]graph.Entity
	pageMeta  map[string]ObjectMeta

	recordMu    sync.Mutex
	recordCache map[string]EntityRecord

	edgeMu    sync.Mutex
	edgeCache map[string]map[string][]graph.Edge
}

func (l *PersistedIndexLookup) MatchFieldIndex(ctx context.Context, kind string, field string, values []any) ([]string, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	spec, ok := l.catalogFieldSpec(kind, field)
	if !ok {
		return nil, false, nil
	}
	if specFormat(spec.Format) == IndexFormatParquet {
		return l.matchParquetFieldIndex(ctx, spec, values)
	}
	return nil, false, nil
}

func (l *PersistedIndexLookup) OutEdges(ctx context.Context, from string, allowed map[string]struct{}) ([]graph.Edge, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	edges := make([]graph.Edge, 0)
	for _, shardID := range indexShardIDCandidates(from) {
		relationTypes := l.relationTypesForShard(shardID, allowed)
		for i, relationType := range relationTypes {
			if i == 0 {
				continue
			}
			if spec, ok := l.catalogEdgeShardSpec(relationType, shardID); ok && specFormat(spec.Format) == IndexFormatParquet {
				l.Store.prefetchParquetEdgeShardObject(ctx, l.TenantID, l.Version, spec)
			}
		}
		for _, relationType := range relationTypes {
			spec, ok := l.catalogEdgeShardSpec(relationType, shardID)
			if !ok {
				return nil, false, nil
			}
			if specFormat(spec.Format) == IndexFormatParquet {
				shardEdges, ok, err := l.outEdgesFromParquetShard(ctx, spec, from, allowed)
				if err != nil || !ok {
					return nil, ok, err
				}
				edges = append(edges, shardEdges...)
				continue
			}
			return nil, false, nil
		}
	}
	edges = uniqueEdgesByID(edges)
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return edges, true, nil
}

func uniqueEdgesByID(edges []graph.Edge) []graph.Edge {
	if len(edges) < 2 {
		return edges
	}
	seen := map[string]struct{}{}
	out := edges[:0]
	for _, edge := range edges {
		if _, ok := seen[edge.ID]; ok {
			continue
		}
		seen[edge.ID] = struct{}{}
		out = append(out, edge)
	}
	return out
}

func (l *PersistedIndexLookup) GetEntity(ctx context.Context, id string, fields []string) (graph.Entity, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return graph.Entity{}, false, nil
	}
	for _, shard := range indexShardIDCandidates(id) {
		if spec, ok := l.catalogEntityPageSpec(shard); ok {
			entity, found, err := l.getEntityByIDFromParquetPage(ctx, id, fields, spec)
			if err != nil || found {
				return entity, found, err
			}
		}
	}
	record, err := l.loadEntityRecord(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return graph.Entity{}, false, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return graph.Entity{}, false, err
		}
		return graph.Entity{}, false, nil
	}
	if !entityRecordMatchesCatalog(record, l.TenantID, id, l.Catalog, l.Version) {
		return graph.Entity{}, false, nil
	}
	if spec, ok := l.catalogEntityPageSpec(record.Page); ok {
		switch specFormat(spec.Format) {
		case IndexFormatParquet:
			return l.getEntityFromParquetPage(ctx, record, fields, spec)
		}
	}
	return graph.Entity{}, false, nil
}

func (l *PersistedIndexLookup) loadEntityRecord(ctx context.Context, id string) (EntityRecord, error) {
	l.recordMu.Lock()
	if record, ok := l.recordCache[id]; ok {
		l.recordMu.Unlock()
		return record, nil
	}
	l.recordMu.Unlock()

	record, err := l.Store.loadEntityRecord(ctx, l.TenantID, id)
	if err != nil {
		return EntityRecord{}, err
	}
	if record.Deleted {
		return EntityRecord{}, ErrNotFound
	}

	l.recordMu.Lock()
	if l.recordCache == nil {
		l.recordCache = map[string]EntityRecord{}
	}
	l.recordCache[id] = record
	l.recordMu.Unlock()
	return record, nil
}

func (l *PersistedIndexLookup) catalogFieldSpec(kind string, field string) (IndexSpec, bool) {
	for _, index := range l.Catalog.Indexes {
		if index.Kind == kind && index.Field == field && index.Status == "ready" {
			return index, true
		}
	}
	return IndexSpec{}, false
}

func (l *PersistedIndexLookup) catalogEdgeShardSpec(relationType string, shard string) (EdgeShard, bool) {
	for _, edgeShard := range l.Catalog.EdgeShards {
		if edgeShard.RelationType == relationType && edgeShard.Shard == shard {
			return edgeShard, true
		}
	}
	return EdgeShard{}, false
}

func catalogHasEntityPage(catalog IndexCatalog, shard string) bool {
	for _, page := range catalog.EntityPages {
		if page.Shard == shard {
			return true
		}
	}
	return false
}

func fieldIndexMatchesCatalog(index SecondaryIndex, spec IndexSpec, version int64) bool {
	if index.Version > version || index.Kind != spec.Kind || index.Field != spec.Field {
		return false
	}
	if !index.cacheVerified && (spec.ContentHash == "" || secondaryIndexContentHash(index) != spec.ContentHash) {
		return false
	}
	entryCount, distinctValues := secondaryIndexCounts(index)
	if entryCount != spec.EntryCount || distinctValues != spec.DistinctValues {
		return false
	}
	return indexTopValuesMatchCatalog(index, spec.TopValues)
}

func edgeShardMatchesCatalog(shard EdgeShardData, spec EdgeShard, version int64) bool {
	if shard.Version > version || shard.RelationType != spec.RelationType || shard.Shard != spec.Shard || len(shard.Edges) != spec.EdgeCount {
		return false
	}
	return shard.cacheVerified || (spec.ContentHash != "" && edgeShardContentHash(shard) == spec.ContentHash)
}

func entityRecordMatchesCatalog(record EntityRecord, tenantID string, id string, catalog IndexCatalog, version int64) bool {
	return !record.Deleted &&
		record.Version <= version &&
		indexTenantMatches(record.TenantID, tenantID) &&
		record.ID == id &&
		record.Entity.ID == id &&
		indexShardIDMatches(id, record.Page) &&
		catalogHasEntityPage(catalog, record.Page)
}

func (l *PersistedIndexLookup) entityRecordMatchesPage(ctx context.Context, record EntityRecord) (bool, error) {
	if ok, done, err := l.entityRecordMatchesPageHash(ctx, record); done || err != nil {
		return ok, err
	}
	page, err := l.loadEntityPage(ctx, record.Page)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return false, err
		}
		return false, nil
	}
	if page.Shard != record.Page || page.Version > l.Version || !indexTenantMatches(page.TenantID, l.TenantID) {
		return false, nil
	}
	if spec, ok := l.catalogEntityPageSpec(record.Page); !ok || spec.ContentHash == "" || entityPageContentHash(page) != spec.ContentHash {
		return false, nil
	}
	entity, ok := l.entityFromCachedPage(record.Page, record.ID, page)
	if !ok {
		return false, nil
	}
	return reflect.DeepEqual(entity, record.Entity), nil
}

func (l *PersistedIndexLookup) entityRecordMatchesPageHash(ctx context.Context, record EntityRecord) (bool, bool, error) {
	if record.PageHash == "" || record.PageETag == "" || record.ContentHash == "" {
		return false, false, nil
	}
	if entityRecordContentHash(record) != record.ContentHash {
		return false, true, nil
	}
	spec, ok := l.catalogEntityPageSpec(record.Page)
	if !ok || spec.ContentHash == "" {
		return false, true, nil
	}
	if record.PageHash != spec.ContentHash {
		return false, true, nil
	}
	meta, err := l.loadEntityPageMeta(ctx, record.Page)
	if errors.Is(err, ErrNotFound) {
		return false, true, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return false, true, err
		}
		return false, true, nil
	}
	return meta.ETag == record.PageETag, true, nil
}

func (l *PersistedIndexLookup) loadEntityPageMeta(ctx context.Context, shard string) (ObjectMeta, error) {
	l.pageMu.Lock()
	if meta, ok := l.pageMeta[shard]; ok {
		l.pageMu.Unlock()
		return meta, nil
	}
	l.pageMu.Unlock()

	spec, ok := l.catalogEntityPageSpec(shard)
	if !ok || specFormat(spec.Format) != IndexFormatParquet {
		return ObjectMeta{}, ErrNotFound
	}
	key := firstIndexObjectKey(spec.Objects, "page", l.Store.parquetEntityPageVersionKey(l.TenantID, l.Version, shard))
	meta, err := objectMeta(ctx, l.Store.Objects, key)
	if err != nil {
		return meta, err
	}

	l.pageMu.Lock()
	if l.pageMeta == nil {
		l.pageMeta = map[string]ObjectMeta{}
	}
	l.pageMeta[shard] = meta
	l.pageMu.Unlock()
	return meta, nil
}

func indexTenantMatches(objectTenantID string, tenantID string) bool {
	return objectTenantID == "" || objectTenantID == tenantID
}

func (l *PersistedIndexLookup) catalogEntityPageSpec(shard string) (EntityPageSpec, bool) {
	for _, page := range l.Catalog.EntityPages {
		if page.Shard == shard {
			return page, true
		}
	}
	return EntityPageSpec{}, false
}

func (l *PersistedIndexLookup) loadEntityPage(ctx context.Context, shard string) (EntityPageData, error) {
	l.pageMu.Lock()
	if page, ok := l.pageCache[shard]; ok {
		l.pageMu.Unlock()
		return page, nil
	}
	l.pageMu.Unlock()

	if spec, ok := l.catalogEntityPageSpec(shard); ok && specFormat(spec.Format) == IndexFormatParquet {
		loaded, _, ok, err := l.Store.loadValidatedParquetEntityPageObject(ctx, l.TenantID, l.Version, spec)
		if err != nil || !ok {
			if err != nil {
				return EntityPageData{}, err
			}
			return EntityPageData{}, ErrNotFound
		}
		page := loaded
		l.pageMu.Lock()
		if l.pageCache == nil {
			l.pageCache = map[string]EntityPageData{}
		}
		if l.pageIndex == nil {
			l.pageIndex = map[string]map[string]graph.Entity{}
		}
		l.pageCache[shard] = page
		l.pageIndex[shard] = entityPageIndex(page)
		l.pageMu.Unlock()
		return page, nil
	}
	return EntityPageData{}, ErrNotFound
}

func (l *PersistedIndexLookup) entityFromCachedPage(shard string, id string, page EntityPageData) (graph.Entity, bool) {
	l.pageMu.Lock()
	if l.pageIndex == nil {
		l.pageIndex = map[string]map[string]graph.Entity{}
	}
	entities := l.pageIndex[shard]
	if entities == nil {
		entities = entityPageIndex(page)
		l.pageIndex[shard] = entities
	}
	entity, ok := entities[id]
	l.pageMu.Unlock()
	return entity, ok
}

func entityPageIndex(page EntityPageData) map[string]graph.Entity {
	entities := make(map[string]graph.Entity, len(page.Entities))
	for _, entity := range page.Entities {
		entities[entity.ID] = entity
	}
	return entities
}

func indexTopValuesMatchCatalog(index SecondaryIndex, expected []IndexValueStat) bool {
	actual := secondaryIndexTopValues(index, len(expected))
	if len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func secondaryIndexTopValues(index SecondaryIndex, limit int) []IndexValueStat {
	if limit <= 0 {
		return nil
	}
	values := make([]IndexValueStat, 0, len(index.Values))
	for value, ids := range index.Values {
		if len(ids) > 0 {
			values = append(values, IndexValueStat{Value: value, Count: len(ids)})
		}
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Count == values[j].Count {
			return values[i].Value < values[j].Value
		}
		return values[i].Count > values[j].Count
	})
	if len(values) > limit {
		values = values[:limit]
	}
	return values
}

func (l *PersistedIndexLookup) relationTypesForShard(shardID string, allowed map[string]struct{}) []string {
	seen := map[string]struct{}{}
	for _, shard := range l.Catalog.EdgeShards {
		if shard.Shard != shardID || !relationAllowedForLookup(shard.RelationType, allowed) {
			continue
		}
		seen[shard.RelationType] = struct{}{}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func relationAllowedForLookup(relationType string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[relationType]
	return ok
}

func trimEntityFields(entity graph.Entity, fields []string) graph.Entity {
	entity = graph.CopyEntity(entity)
	if len(fields) == 0 {
		return entity
	}
	keep := map[string]struct{}{}
	for _, field := range fields {
		if fieldName, ok := entityFieldName(field); ok {
			keep[fieldName] = struct{}{}
		}
	}
	if len(keep) == 0 {
		entity.Fields = graph.Fields{}
		entity.FieldSources = nil
		return entity
	}
	next := graph.Fields{}
	for field, value := range entity.Fields {
		if _, ok := keep[field]; ok {
			next[field] = value
		}
	}
	entity.Fields = next
	entity.FieldSources = trimFieldSources(entity.FieldSources, keep)
	return entity
}

func trimFieldSources(sources map[string]graph.FieldSource, keep map[string]struct{}) map[string]graph.FieldSource {
	if len(sources) == 0 || len(keep) == 0 {
		return nil
	}
	next := map[string]graph.FieldSource{}
	for field := range keep {
		if source, ok := sources[field]; ok {
			next[field] = source
		}
	}
	if len(next) == 0 {
		return nil
	}
	return next
}

func entityFieldName(field string) (string, bool) {
	switch field {
	case "", "id", "kind", "source", "external_id", "confidence", "source_priority", "created_at", "updated_at":
		return "", false
	}
	if strings.HasPrefix(field, "identity.") {
		return "", false
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		return name, name != ""
	}
	return field, true
}
