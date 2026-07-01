package graph

import (
	"fmt"
	"time"
)

func (g *Graph) applyMerge(request MergeRequest, version int64, now time.Time) error {
	target, ok := g.Entities[request.TargetID]
	if !ok {
		return fmt.Errorf("merge target entity %q not found", request.TargetID)
	}
	for _, sourceID := range request.SourceIDs {
		if sourceID == request.TargetID {
			continue
		}
		source, ok := g.Entities[sourceID]
		if !ok {
			return fmt.Errorf("merge source entity %q not found", sourceID)
		}
		if source.Kind != target.Kind {
			return fmt.Errorf("cannot merge entity %q kind %q into %q kind %q", sourceID, source.Kind, target.ID, target.Kind)
		}
		fields, err := g.EffectiveFields(target.Kind)
		if err != nil {
			return err
		}
		target = mergeEntityWithSpecs(target, source, fields)
		target.MergedFrom = appendUnique(target.MergedFrom, sourceID)
		for edgeID, edge := range g.Edges {
			if edge.From == sourceID {
				edge.From = request.TargetID
				g.Edges[edgeID] = edge
			}
			if edge.To == sourceID {
				edge.To = request.TargetID
				g.Edges[edgeID] = edge
			}
		}
		delete(g.Entities, sourceID)
	}
	g.Edges, _ = mergeEdgeSet(g.Edges, version, now)
	target.ID = request.TargetID
	target.Version = version
	target.UpdatedAt = now
	g.Entities[request.TargetID] = target
	g.rebuildIndexes()
	return g.validateAllEdges()
}

func (g *Graph) applySplit(request SplitRequest, version int64, now time.Time) error {
	if request.SourceID == "" {
		return fmt.Errorf("split source_id is required")
	}
	if _, ok := g.Entities[request.SourceID]; !ok {
		return fmt.Errorf("split source entity %q not found", request.SourceID)
	}
	delete(g.Entities, request.SourceID)
	for edgeID, edge := range g.Edges {
		if edge.From == request.SourceID || edge.To == request.SourceID {
			delete(g.Edges, edgeID)
		}
	}
	for _, entity := range request.Entities {
		normalized, err := normalizeEntity(entity)
		if err != nil {
			return err
		}
		sanitizeIncomingEntitySources(&normalized)
		if err := g.applyEntitySchema(&normalized); err != nil {
			return err
		}
		if normalized.ID == request.SourceID {
			return fmt.Errorf("split replacement cannot reuse source id %q", request.SourceID)
		}
		normalized.SplitFrom = request.SourceID
		normalized.Version = version
		normalized.CreatedAt = now
		normalized.UpdatedAt = now
		stampFieldSources(&normalized, version, now)
		if err := g.validateUniqueEntity(normalized); err != nil {
			return err
		}
		if err := g.validateIdentityAvailable(normalized); err != nil {
			return err
		}
		clearEntityWriteMetadata(&normalized)
		g.Entities[normalized.ID] = normalized
	}
	return nil
}

func (g *Graph) validateIdentityAvailable(entity Entity) error {
	signatures := g.identitySignatures(entity)
	if len(signatures) == 0 {
		return nil
	}
	for id, existing := range g.Entities {
		if id == entity.ID || existing.Kind != entity.Kind {
			continue
		}
		for _, existingSignature := range g.identitySignatures(existing) {
			for _, signature := range signatures {
				if signature.Value == existingSignature.Value {
					return fmt.Errorf("entity %q duplicates identity %q owned by %q", entity.ID, signature.Value, id)
				}
			}
		}
	}
	return nil
}
