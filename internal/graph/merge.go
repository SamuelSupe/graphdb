package graph

import (
	"fmt"
	"time"
)

func (g *Graph) applyMerge(
	request MergeRequest,
	version int64,
	now time.Time,
	uniqueValidator *uniqueEntityValidator,
	fieldSpecsByKind map[string]map[string]FieldSpec,
	tracker *mutationFingerprintTracker,
	affected *uniqueStringCollector,
	reportAffectedEdges *uniqueStringCollector,
) error {
	target, ok := g.Entities[request.TargetID]
	if !ok {
		return fmt.Errorf("merge target entity %q not found", request.TargetID)
	}
	affected.add(request.TargetID)
	fields, err := g.effectiveFieldsCached(
		target.Kind,
		fieldSpecsByKind,
	)
	if err != nil {
		return err
	}
	g.removeEntityFromIndexes(target.ID, target)
	affectedMergeEdges := map[string]struct{}{}
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
		target = mergeEntityWithSpecs(target, source, fields)
		target.MergedFrom = appendUnique(target.MergedFrom, sourceID)
		if err := g.redirectMergeEdges(
			sourceID,
			request.TargetID,
			version,
			now,
			tracker,
			reportAffectedEdges,
			affectedMergeEdges,
		); err != nil {
			return err
		}
		g.removeEntityFromIndexes(sourceID, source)
		delete(g.Entities, sourceID)
		affected.add(sourceID)
	}
	target.ID = request.TargetID
	target.Version = version
	target.UpdatedAt = now
	g.Entities[request.TargetID] = target
	if err := validateEntityFieldsWithSpecs(target, fields); err != nil {
		return err
	}
	if err := uniqueValidator.validate(target); err != nil {
		return err
	}
	if err := g.validateIdentityIndexAvailable(target); err != nil {
		return err
	}
	g.addEntityToIndexes(request.TargetID, target)
	uniqueValidator.add(target)
	return g.validateAffectedMergeEdges(affectedMergeEdges)
}
