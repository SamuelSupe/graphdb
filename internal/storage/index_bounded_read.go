package storage

import (
	"context"
	"fmt"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func (s *TenantStore) loadEntityRangePage(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) (EntityPageData, bool, error) {
	if err := ctx.Err(); err != nil {
		return EntityPageData{}, false, err
	}
	if err := validateRangeObjects(spec.Objects, spec.EntityCount, spec.ContentHash); err != nil {
		return EntityPageData{}, false, err
	}
	page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, Shard: spec.Shard, UpdatedAt: spec.UpdatedAt, hashCanonical: true, logicalContentHash: spec.ContentHash}
	for _, object := range spec.Objects {
		routing, _ := objectIndexRange(object)
		part, _, ok, err := s.borrowValidatedParquetEntityPageObject(ctx, tenantID, version, entityRangeSpec(spec, object))
		if err != nil || !ok {
			return EntityPageData{}, false, err
		}
		for _, entity := range part.Entities {
			if !routing.contains(entity.ID) {
				return EntityPageData{}, false, fmt.Errorf("entity is outside its index range")
			}
		}
		page.Entities = append(page.Entities, part.Entities...)
		page.Version = max(page.Version, part.Version)
	}
	return page, true, ctx.Err()
}

func (s *TenantStore) loadEdgeRangePage(ctx context.Context, tenantID string, version int64, spec EdgeShard) (EdgeShardData, bool, error) {
	if err := ctx.Err(); err != nil {
		return EdgeShardData{}, false, err
	}
	if err := validateRangeObjects(spec.Objects, spec.EdgeCount, spec.ContentHash); err != nil {
		return EdgeShardData{}, false, err
	}
	shard := EdgeShardData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, RelationType: spec.RelationType, Shard: spec.Shard, UpdatedAt: spec.UpdatedAt, hashCanonical: true, logicalContentHash: spec.ContentHash, cacheVerified: true}
	for _, object := range spec.Objects {
		routing, _ := objectIndexRange(object)
		partSpec := edgeRangeSpec(spec, object)
		part, ok, err := s.loadParquetEdgeShardObject(ctx, tenantID, version, partSpec)
		if err != nil || !ok {
			return EdgeShardData{}, false, err
		}
		if !edgeShardObjectMatchesCatalog(part, partSpec, version) || !indexTenantMatches(part.TenantID, tenantID) {
			return EdgeShardData{}, false, fmt.Errorf("invalid index edge range")
		}
		for _, edge := range part.Edges {
			if !routing.contains(edge.ID) {
				return EdgeShardData{}, false, fmt.Errorf("edge is outside its index range")
			}
		}
		shard.Edges = append(shard.Edges, part.Edges...)
		shard.Version = max(shard.Version, part.Version)
	}
	return shard, true, ctx.Err()
}

func (l *PersistedIndexLookup) getEntityFromRangePage(ctx context.Context, id string, fields []string, spec EntityPageSpec) (graph.Entity, bool, error) {
	if err := ctx.Err(); err != nil {
		return graph.Entity{}, false, err
	}
	n := rangeObjectIndex(spec.Objects, id)
	page, ok, err := l.Store.borrowEntityRange(ctx, l.TenantID, l.Version, spec, n)
	if err != nil || !ok {
		return graph.Entity{}, false, err
	}
	i := sort.Search(len(page.Entities), func(i int) bool { return page.Entities[i].ID >= id })
	if i == len(page.Entities) || page.Entities[i].ID != id {
		return graph.Entity{}, false, nil
	}
	return trimEntityFields(page.Entities[i], fields), true, nil
}

func (s *TenantStore) borrowEntityRange(ctx context.Context, tenantID string, version int64, spec EntityPageSpec, n int) (EntityPageData, bool, error) {
	page, _, ok, err := s.borrowValidatedParquetEntityPageObject(ctx, tenantID, version, entityRangeSpec(spec, spec.Objects[n]))
	if err != nil || !ok {
		return EntityPageData{}, false, err
	}
	routing, _ := objectIndexRange(spec.Objects[n])
	for _, entity := range page.Entities {
		if !routing.contains(entity.ID) {
			return EntityPageData{}, false, fmt.Errorf("entity is outside its index range")
		}
	}
	return page, true, nil
}

func (l *PersistedIndexLookup) visitEntityRanges(ctx context.Context, spec EntityPageSpec, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, bool, error) {
	prefetch := rangeScanPrefetch{store: l.Store, ctx: ctx}
	defer prefetch.close()
	for n := rangeObjectIndex(spec.Objects, afterID); n < len(spec.Objects); n++ {
		if err := prefetch.entity(l.TenantID, l.Version, spec, n); err != nil {
			return false, false, err
		}
		object := spec.Objects[n]
		routing, _ := objectIndexRange(object)
		keepGoing := true
		consume := func(page EntityPageData) error {
			for _, entity := range page.Entities {
				if !routing.contains(entity.ID) {
					return fmt.Errorf("entity is outside its index range")
				}
			}
			start := sort.Search(len(page.Entities), func(i int) bool { return page.Entities[i].ID >= afterID })
			for _, entity := range page.Entities[start:] {
				if err := ctx.Err(); err != nil {
					return err
				}
				if kind != "" && entity.Kind != kind {
					if err := query.ConsumeIndexScanCost(ctx, 1); err != nil {
						return err
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
		}
		if session := entityPageVisitSessionFromContext(ctx); session != nil {
			session.stats.pageSpecsVisited++
			session.resumeID = ""
			if afterID != "" && routing.contains(afterID) {
				session.resumeID = afterID
			}
			ok, skipped, err := session.visitPage(ctx, entityRangeSpec(spec, object), consume)
			if err != nil || !ok {
				return false, false, err
			}
			if skipped {
				// Metadata pruning saves decoding, not the logical scan allowance.
				if err := query.ConsumeIndexScanCost(ctx, session.skippedRows); err != nil {
					return false, false, err
				}
				continue
			}
		} else {
			page, ok, err := l.Store.borrowEntityRange(ctx, l.TenantID, l.Version, spec, n)
			if err != nil || !ok {
				return false, false, err
			}
			if err := consume(page); err != nil {
				return false, false, err
			}
		}
		if !keepGoing {
			return true, false, nil
		}
	}
	return true, true, nil
}
