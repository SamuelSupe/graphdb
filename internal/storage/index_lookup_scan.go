package storage

import (
	"context"
	"sort"

	"graphdb/internal/graph"
)

func (l *PersistedIndexLookup) ListEntities(ctx context.Context, kind string, fields []string) ([]graph.Entity, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	specs := append([]EntityPageSpec(nil), l.Catalog.EntityPages...)
	sort.Slice(specs, func(i, j int) bool { return specs[i].Shard < specs[j].Shard })
	entities := make([]graph.Entity, 0)
	for _, spec := range specs {
		if err := objectContextErr(ctx); err != nil {
			return nil, false, err
		}
		page, ok, err := l.listEntitiesFromPage(ctx, spec, kind, fields)
		if err != nil || !ok {
			return nil, ok, err
		}
		entities = append(entities, page...)
	}
	sort.Slice(entities, func(i, j int) bool {
		return scanKey(entityShardID(entities[i].ID), entities[i].ID) < scanKey(entityShardID(entities[j].ID), entities[j].ID)
	})
	return entities, true, nil
}

func (l *PersistedIndexLookup) listEntitiesFromPage(ctx context.Context, spec EntityPageSpec, kind string, fields []string) ([]graph.Entity, bool, error) {
	if specFormat(spec.Format) == IndexFormatParquet {
		return l.listParquetEntitiesFromPage(ctx, spec, kind, fields)
	}
	return nil, false, nil
}
