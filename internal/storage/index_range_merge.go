package storage

import (
	"context"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

const indexPageMergeRows = 768
const indexPageMergeBytes = 6 << 20

func rangePageSmall(o IndexObject) bool {
	r, _ := decodeIndexRange(o.Role)
	return o.RowCount < 384 || r.Bytes < indexPageMaxBytes/4
}

func rangePageMergeable(o IndexObject) bool {
	r, _ := decodeIndexRange(o.Role)
	return o.RowCount != 1 || r.Bytes <= indexPageMaxBytes
}
func rangePageShrank(old, next IndexObject) bool {
	a, _ := decodeIndexRange(old.Role)
	b, _ := decodeIndexRange(next.Role)
	return next.RowCount < old.RowCount || b.Bytes < a.Bytes
}

// The callback encodes the actual merged payload. There is no count-only size
// estimate and no chain: each source page participates in at most one pair.
func mergeRangeObjects(ctx context.Context, objects []IndexObject, shrank map[string]bool, merge func(context.Context, IndexObject, IndexObject) (IndexObject, bool, error)) ([]IndexObject, error) {
	var result []IndexObject
	attempted := map[string]bool{}
	for i := 0; i < len(objects); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a := objects[i]
		if i+1 < len(objects) {
			b := objects[i+1]
			if !attempted[a.ContentHash] && !attempted[b.ContentHash] && rangePageMergeable(a) && rangePageMergeable(b) && a.RowCount+b.RowCount <= indexPageMergeRows && (shrank[a.ContentHash] && rangePageSmall(a) || shrank[b.ContentHash] && rangePageSmall(b)) {
				attempted[a.ContentHash], attempted[b.ContentHash] = true, true
				merged, ok, err := merge(ctx, a, b)
				if err != nil {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
					return nil, err
				}
				if ok {
					result = append(result, merged)
					i++
					continue
				}
			}
		}
		result = append(result, a)
	}
	return result, nil
}

func (s *TenantStore) tryRangeMerge(ctx context.Context) (context.Context, func(), bool) {
	if s.localFileStore() == nil {
		return ctx, func() {}, true
	}
	// Replica catalogs must not diverge with local encoder occupancy: cursors
	// bind their catalog hash across replicas. Replicated application waits
	// within its existing deadline rather than making an optional local choice.
	if s.ReplicationMode || IsReplicatedContext(ctx) {
		return s.rangeEncodeContext(ctx), func() {}, true
	}
	select {
	case s.maintenance.encodes <- struct{}{}:
		return context.WithValue(ctx, rangeEncodeBudgetKey{}, false), func() { <-s.maintenance.encodes }, true
	default:
		return ctx, func() {}, false
	}
}

func (s *TenantStore) mergeEntityRanges(ctx context.Context, tenant string, version int64, spec EntityPageSpec, objects []IndexObject, pages []EntityPageData, shrank map[string]bool) ([]IndexObject, []EntityPageData, error) {
	pending := make(map[string]EntityPageData)
	for _, p := range pages {
		pending[p.logicalContentHash] = p
	}
	removed := map[string]bool{}
	var mergedPages []EntityPageData
	load := func(ctx context.Context, o IndexObject) (EntityPageData, error) {
		if p, ok := pending[o.ContentHash]; ok {
			return p, nil
		}
		p, _, ok, err := s.borrowValidatedParquetEntityPageObject(ctx, tenant, version, entityRangeSpec(spec, o))
		if err == nil && !ok {
			err = fmt.Errorf("merge source entity page unavailable")
		}
		return p, err
	}
	next, err := mergeRangeObjects(ctx, objects, shrank, func(ctx context.Context, a, b IndexObject) (IndexObject, bool, error) {
		ctx, release, ok := s.tryRangeMerge(ctx)
		if !ok {
			return IndexObject{}, false, nil
		}
		defer release()
		left, err := load(ctx, a)
		if err != nil {
			return IndexObject{}, false, err
		}
		right, err := load(ctx, b)
		if err != nil {
			return IndexObject{}, false, err
		}
		left.Entities = append(append([]graph.Entity(nil), left.Entities...), right.Entities...)
		left.pageRole = a.Role
		left.Version = version
		parts, err := prepareBoundedEntityPages(ctx, tenant, []EntityPageData{left})
		if err != nil {
			return IndexObject{}, false, err
		}
		if len(parts) != 1 {
			return IndexObject{}, false, nil
		}
		p := parts[0]
		r, _ := decodeIndexRange(p.pageRole)
		if r.Bytes > indexPageMergeBytes {
			return IndexObject{}, false, nil
		}
		o := IndexObject{Role: p.pageRole, Key: s.parquetEntityPageVersionKey(tenant, version, boundedPageObjectID(spec.Shard, p.logicalContentHash)), RowCount: len(p.Entities), ContentHash: p.logicalContentHash, SchemaHash: parquetEntityPageSchemaHash()}
		removed[a.ContentHash], removed[b.ContentHash] = true, true
		mergedPages = append(mergedPages, p)
		return o, true, nil
	})
	if err != nil {
		return nil, nil, err
	}
	retained := make([]EntityPageData, 0, len(pages)+len(mergedPages))
	for _, p := range pages {
		if !removed[p.logicalContentHash] {
			retained = append(retained, p)
		}
	}
	return next, append(retained, mergedPages...), nil
}

func (s *TenantStore) mergeEdgeRanges(ctx context.Context, tenant string, version int64, spec EdgeShard, objects []IndexObject, shards []EdgeShardData, shrank map[string]bool, reverse bool) ([]IndexObject, []EdgeShardData, error) {
	pending := make(map[string]EdgeShardData)
	for _, p := range shards {
		pending[p.logicalContentHash] = p
	}
	removed := map[string]bool{}
	var mergedShards []EdgeShardData
	load := func(ctx context.Context, o IndexObject) (EdgeShardData, error) {
		if p, ok := pending[o.ContentHash]; ok {
			return p, nil
		}
		partSpec := edgeRangeSpec(spec, o)
		p, ok, err := s.loadParquetEdgeShardObject(ctx, tenant, version, partSpec)
		if err == nil && (!ok || !edgeShardMatchesCatalog(p, partSpec, version) || !indexTenantMatches(p.TenantID, tenant)) {
			err = fmt.Errorf("merge source edge page unavailable")
		}
		return p, err
	}
	next, err := mergeRangeObjects(ctx, objects, shrank, func(ctx context.Context, a, b IndexObject) (IndexObject, bool, error) {
		ctx, release, ok := s.tryRangeMerge(ctx)
		if !ok {
			return IndexObject{}, false, nil
		}
		defer release()
		left, err := load(ctx, a)
		if err != nil {
			return IndexObject{}, false, err
		}
		right, err := load(ctx, b)
		if err != nil {
			return IndexObject{}, false, err
		}
		left.Edges = append(append([]graph.Edge(nil), left.Edges...), right.Edges...)
		left.pageRole = a.Role
		left.Version = version
		left.reverse = reverse
		parts, err := prepareBoundedEdgeShards(ctx, tenant, []EdgeShardData{left})
		if err != nil {
			return IndexObject{}, false, err
		}
		if len(parts) != 1 {
			return IndexObject{}, false, nil
		}
		p := parts[0]
		r, _ := decodeIndexRange(p.pageRole)
		if r.Bytes > indexPageMergeBytes {
			return IndexObject{}, false, nil
		}
		key := s.parquetEdgeShardVersionKey(tenant, version, spec.RelationType, boundedPageObjectID(spec.Shard, p.logicalContentHash))
		if reverse {
			key = s.reverseEdgeShardVersionKey(tenant, version, spec.RelationType, boundedPageObjectID(spec.Shard, p.logicalContentHash))
		}
		o := IndexObject{Role: p.pageRole, Key: key, RowCount: len(p.Edges), ContentHash: p.logicalContentHash, SchemaHash: parquetEdgeShardSchemaHash()}
		removed[a.ContentHash], removed[b.ContentHash] = true, true
		mergedShards = append(mergedShards, p)
		return o, true, nil
	})
	if err != nil {
		return nil, nil, err
	}
	retained := make([]EdgeShardData, 0, len(shards)+len(mergedShards))
	for _, p := range shards {
		if !removed[p.logicalContentHash] {
			retained = append(retained, p)
		}
	}
	return next, append(retained, mergedShards...), nil
}
