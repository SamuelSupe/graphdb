package storage

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// Keep old scalar values and edge endpoints, not graphs or source histories.
// A small byte cap also bounds an unusually large indexed scalar value.
func (s *TenantStore) mergeDeferredIndexUpdate(pending, next *commitIndexUpdate) {
	defer func() {
		pending.version = next.version
		if pending.baseVersion < 0 {
			pending.deferredEntities, pending.deferredEdges = nil, nil
			pending.deferredBytes = 0
		}
	}()
	if pending.baseVersion < 0 || pending.version != next.baseVersion || next.before == nil || next.rebuild || !canIncrementIndexes(next.mutations) ||
		len(pending.deferredEntities)+len(pending.deferredEdges)+len(next.report.AffectedEntityIDs)+len(next.report.AffectedEdgeIDs) > maxPendingIndexChanges {
		pending.baseVersion = -1
		return
	}
	limit := min(s.backgroundIndexByteLimit()/64, int64(maxPendingIndexChanges)*1024)
	if pending.deferredEntities == nil {
		pending.deferredEntities = make(map[string]graph.Entity)
		pending.deferredEdges = make(map[string]graph.Edge)
	}
	for _, id := range next.report.AffectedEntityIDs {
		if _, seen := pending.deferredEntities[id]; !seen {
			entity, _ := next.before.Entities.Get(id)
			image := graph.Entity{ID: entity.ID, Kind: entity.Kind, Fields: make(graph.Fields)}
			bytes := int64(1024 + len(id) + len(entity.Kind))
			for field, value := range entity.Fields {
				if key, scalar := graph.ScalarIndexKey(value); scalar {
					bytes += int64(64 + len(field) + len(key))
					image.Fields[field] = value
				}
			}
			pending.deferredBytes += bytes
			if pending.deferredBytes > limit {
				pending.baseVersion = -1
				return
			}
			pending.deferredEntities[id] = image
		}
	}
	for _, id := range next.report.AffectedEdgeIDs {
		if _, seen := pending.deferredEdges[id]; !seen {
			edge, _ := next.before.Edges.Get(id)
			pending.deferredBytes += int64(1024 + len(id) + len(edge.Type) + len(edge.From) + len(edge.To))
			if pending.deferredBytes > limit {
				pending.baseVersion = -1
				return
			}
			pending.deferredEdges[id] = graph.Edge{ID: edge.ID, Type: edge.Type, From: edge.From, To: edge.To}
		}
	}
}

func (s *TenantStore) prepareDeferredIndexBase(ctx context.Context, tenantID string, work *commitIndexUpdate) error {
	defer func() { work.deferredEntities, work.deferredEdges = nil, nil; work.deferredBytes = 0 }()
	if work.baseVersion < 0 {
		return nil
	}
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if err != nil || catalog.Version != work.baseVersion {
		work.baseVersion = -1
		return ctx.Err()
	}
	// Incremental builders only read old entity values and edge endpoints from
	// before; they scan complete partitions from the newly captured graph.
	before := &graph.Graph{Version: catalog.Version, Entities: graph.NewShardedMap[graph.Entity](), Edges: graph.NewShardedMap[graph.Edge]()}
	for id, entity := range work.deferredEntities {
		if entity.ID != "" {
			before.Entities.Set(id, entity)
		}
	}
	for id, edge := range work.deferredEdges {
		if edge.ID != "" {
			before.Edges.Set(id, edge)
		}
	}
	work.before = before
	work.report = graph.ApplyReport{AffectedEntityIDs: sortedStringKeys(work.deferredEntities), AffectedEdgeIDs: sortedStringKeys(work.deferredEdges)}
	return nil
}
