package graph

import (
	"fmt"
	"time"
)

func (g *Graph) applySplit(
	request SplitRequest,
	version int64,
	now time.Time,
	uniqueValidator *uniqueEntityValidator,
	fieldSpecsByKind map[string]map[string]FieldSpec,
	affected *uniqueStringCollector,
	affectedEdges *uniqueStringCollector,
) error {
	if request.SourceID == "" {
		return fmt.Errorf("split source_id is required")
	}
	source, ok := g.Entities[request.SourceID]
	if !ok {
		return fmt.Errorf("split source entity %q not found", request.SourceID)
	}
	g.removeEntityFromIndexes(request.SourceID, source)
	delete(g.Entities, request.SourceID)
	affected.add(request.SourceID)
	for _, edgeID := range g.incidentEdgeIDs(request.SourceID) {
		edge, ok := g.Edges[edgeID]
		if !ok {
			continue
		}
		g.removeEdgeFromIndexes(edgeID, edge)
		delete(g.Edges, edgeID)
		affectedEdges.add(edgeID)
	}
	for _, entity := range request.Entities {
		normalized, err := normalizeEntity(entity)
		if err != nil {
			return err
		}
		sanitizeIncomingEntitySources(&normalized)
		fields, err := g.effectiveFieldsCached(
			normalized.Kind,
			fieldSpecsByKind,
		)
		if err != nil {
			return err
		}
		if err := g.applyEntitySchemaWithSpecs(
			&normalized,
			fields,
		); err != nil {
			return err
		}
		if normalized.ID == request.SourceID {
			return fmt.Errorf(
				"split replacement cannot reuse source id %q",
				request.SourceID,
			)
		}
		if _, exists := g.Entities[normalized.ID]; exists {
			return fmt.Errorf(
				"split replacement entity %q already exists",
				normalized.ID,
			)
		}
		normalized.SplitFrom = request.SourceID
		normalized.Version = version
		normalized.CreatedAt = now
		normalized.UpdatedAt = now
		stampFieldSources(&normalized, version, now)
		if err := uniqueValidator.validate(normalized); err != nil {
			return err
		}
		if err := g.validateIdentityIndexAvailable(normalized); err != nil {
			return err
		}
		clearEntityWriteMetadata(&normalized)
		g.Entities[normalized.ID] = normalized
		g.addEntityToIndexes(normalized.ID, normalized)
		uniqueValidator.add(normalized)
		affected.add(normalized.ID)
	}
	return nil
}
