package storage

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"graphdb/internal/graph"
)

func (l *PersistedIndexLookup) ListEntities(ctx context.Context, kind string, fields []string) ([]graph.Entity, bool, error) {
	entities := make([]graph.Entity, 0)
	ok, err := l.VisitEntities(ctx, kind, fields, "", func(entity graph.Entity) (bool, error) {
		entities = append(entities, entity)
		return true, nil
	})
	return entities, ok, err
}

func (l *PersistedIndexLookup) VisitEntities(ctx context.Context, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, error) {
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
		pageAfterID := ""
		if shard == afterShard {
			pageAfterID = afterID
		}
		ok, keepGoing, err := l.visitEntityPageGroup(ctx, groups[shard], kind, fields, pageAfterID, visit)
		if err != nil || !ok {
			return ok, err
		}
		if !keepGoing {
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
		for _, entity := range page.Entities {
			if err := objectContextErr(ctx); err != nil {
				return err
			}
			if kind != "" && entity.Kind != kind {
				continue
			}
			if afterID != "" && entity.ID < afterID {
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
