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
	target, ok := g.Entities.Get(request.TargetID)
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
	sources := make(map[string]bool, len(request.SourceIDs))
	for _, sourceID := range request.SourceIDs {
		if sourceID == request.TargetID {
			continue
		}
		source, ok := g.Entities.Get(sourceID)
		if !ok {
			return fmt.Errorf("merge source entity %q not found", sourceID)
		}
		if source.Kind != target.Kind {
			return fmt.Errorf("cannot merge entity %q kind %q into %q kind %q", sourceID, source.Kind, target.ID, target.Kind)
		}
		target = mergeEntityWithSpecs(target, source, fields)
		target.MergedFrom = appendUnique(target.MergedFrom, sourceID)
		sources[sourceID] = true
		g.removeEntityFromIndexes(sourceID, source)
		g.Entities.Delete(sourceID)
		affected.add(sourceID)
	}
	target.ID = request.TargetID
	target.Version = version
	target.UpdatedAt = now
	g.Entities.Set(request.TargetID, target)
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
	affectedMergeEdges := map[string]struct{}{}
	if err := g.redirectMergeEdges(request.SourceIDs, sources, request.TargetID, version, now, tracker, reportAffectedEdges, affectedMergeEdges); err != nil {
		return err
	}
	return g.validateAffectedMergeEdges(affectedMergeEdges)
}
