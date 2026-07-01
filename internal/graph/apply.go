package graph

import (
	"fmt"
	"time"
)

func (g *Graph) ApplyCommit(commit Commit) error {
	_, err := g.ApplyCommitWithOptions(commit, ApplyOptions{})
	return err
}

func (g *Graph) ApplyCommitWithOptions(commit Commit, options ApplyOptions) (ApplyReport, error) {
	clone, report, err := g.ApplyCommitCopyWithOptions(commit, options)
	if err != nil {
		return ApplyReport{}, err
	}
	*g = *clone
	return report, nil
}

func (g *Graph) ApplyCommitCopyWithOptions(commit Commit, options ApplyOptions) (*Graph, ApplyReport, error) {
	if commit.Version <= g.Version {
		return nil, ApplyReport{}, fmt.Errorf("commit version %d must be greater than graph version %d", commit.Version, g.Version)
	}
	policyReport := ApplyReport{}
	if options.SourcePolicy != nil {
		var err error
		commit.Mutations, policyReport, err = ApplySourcePolicy(commit.Mutations, *options.SourcePolicy)
		if err != nil {
			return nil, ApplyReport{}, err
		}
	}
	clone := g.Clone()
	clone.Version = commit.Version
	report, err := clone.applyMutations(commit, options)
	if err != nil {
		return nil, ApplyReport{}, err
	}
	report.Suppressed = append(policyReport.Suppressed, report.Suppressed...)
	return clone, report, nil
}

func (g *Graph) applyMutations(commit Commit, _ ApplyOptions) (ApplyReport, error) {
	report := ApplyReport{}
	now := commit.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	indexesDirty := false
	for _, edgeID := range commit.Mutations.DeleteEdges {
		if edgeID == "" {
			return ApplyReport{}, fmt.Errorf("delete edge id is required")
		}
		resolved, err := g.resolveEdgeReference(edgeID)
		if err != nil {
			return ApplyReport{}, err
		}
		if resolved != "" {
			edge := g.Edges[resolved]
			g.removeEdgeFromIndexes(resolved, edge)
			delete(g.Edges, resolved)
		} else {
			delete(g.Edges, edgeID)
		}
	}
	for _, request := range commit.Mutations.DeleteEdgeRequests {
		edgeID, err := g.resolveEdgeDeleteRequest(request)
		if err != nil {
			return ApplyReport{}, err
		}
		if edgeID == "" {
			continue
		}
		edge, ok := g.Edges[edgeID]
		if !ok {
			continue
		}
		backfillEdgeSources(&edge, edge.Version, edge.UpdatedAt)
		existingOwner := *edge.ExistenceSource
		incomingOwner := sourceForDelete(request, commit.Version, now)
		if sourceCanDeleteEdge(existingOwner, incomingOwner) {
			g.removeEdgeFromIndexes(edgeID, edge)
			delete(g.Edges, edgeID)
			continue
		}
		report.Suppressed = append(report.Suppressed, existenceConflict(edgeID, request, existingOwner, incomingOwner))
	}
	for _, request := range commit.Mutations.DeleteEntityRequests {
		entityID, err := g.resolveEntityDeleteRequest(request)
		if err != nil {
			return ApplyReport{}, err
		}
		if entityID == "" {
			continue
		}
		entity, ok := g.Entities[entityID]
		if !ok {
			continue
		}
		backfillFieldSources(&entity)
		existingOwner := *entity.ExistenceSource
		incomingOwner := sourceForEntityDelete(request, commit.Version, now)
		if sourceCanDeleteEntity(existingOwner, incomingOwner) {
			g.deleteEntityForce(entityID)
			report.AffectedEntityIDs = appendUnique(report.AffectedEntityIDs, entityID)
			continue
		}
		report.Suppressed = append(report.Suppressed, entityExistenceConflict(entityID, request, existingOwner, incomingOwner))
	}
	for _, entityID := range commit.Mutations.DeleteEntities {
		if entityID == "" {
			return ApplyReport{}, fmt.Errorf("delete entity id is required")
		}
		resolved := g.ResolveEntityReference(entityID)
		if resolved == "" {
			continue
		}
		g.deleteEntityForce(resolved)
		report.AffectedEntityIDs = appendUnique(report.AffectedEntityIDs, resolved)
	}
	for _, relationName := range commit.Mutations.DeleteRelationTypes {
		if relationName == "" {
			return ApplyReport{}, fmt.Errorf("delete relation type name is required")
		}
		delete(g.RelationTypes, relationName)
		for edgeID, edge := range g.Edges {
			if edge.Type == relationName {
				g.removeEdgeFromIndexes(edgeID, edge)
				delete(g.Edges, edgeID)
			}
		}
	}
	for _, ciTypeName := range commit.Mutations.DeleteCITypes {
		if ciTypeName == "" {
			return ApplyReport{}, fmt.Errorf("delete ci type name is required")
		}
		for _, entity := range g.Entities {
			if entity.Kind == ciTypeName {
				return ApplyReport{}, fmt.Errorf("cannot delete ci type %q while entities of that kind exist", ciTypeName)
			}
		}
		delete(g.CITypes, ciTypeName)
		indexesDirty = true
	}
	for _, ciType := range commit.Mutations.UpsertCITypes {
		normalized, err := normalizeCIType(ciType)
		if err != nil {
			return ApplyReport{}, err
		}
		g.CITypes[normalized.Name] = normalized
		indexesDirty = true
	}
	if indexesDirty {
		if err := g.validateCITypes(); err != nil {
			return ApplyReport{}, err
		}
		if err := g.validateEntitiesAgainstCITypes(); err != nil {
			return ApplyReport{}, err
		}
	}
	for _, relationType := range commit.Mutations.UpsertRelationTypes {
		normalized, err := normalizeRelationType(relationType)
		if err != nil {
			return ApplyReport{}, err
		}
		g.RelationTypes[normalized.Name] = normalized
		if err := g.validateAllEdges(); err != nil {
			return ApplyReport{}, err
		}
	}
	if indexesDirty {
		g.rebuildIndexes()
	}
	for _, entity := range commit.Mutations.UpsertEntities {
		normalized, err := normalizeEntity(entity)
		if err != nil {
			return ApplyReport{}, err
		}
		sanitizeIncomingEntitySources(&normalized)
		if err := g.applyEntitySchema(&normalized); err != nil {
			return ApplyReport{}, err
		}
		incomingID := normalized.ID
		targetID, err := g.resolveEntityID(normalized)
		if err != nil {
			return ApplyReport{}, err
		}
		report.CanonicalEntities = append(report.CanonicalEntities, entityCanonicalization(normalized, incomingID, targetID))
		normalized.ID = targetID
		report.Suppressed = append(report.Suppressed, entityFieldConflictsForTarget(normalized, targetID)...)
		normalized.FieldConflicts = nil
		if previous, ok := g.Entities[normalized.ID]; ok {
			if previous.Kind != normalized.Kind {
				return ApplyReport{}, fmt.Errorf("entity %q kind change from %q to %q is not allowed", normalized.ID, previous.Kind, normalized.Kind)
			}
			normalized.ID = incomingID
			var mergeReport ApplyReport
			fields, err := g.EffectiveFields(previous.Kind)
			if err != nil {
				return ApplyReport{}, err
			}
			normalized, mergeReport = mergeEntityForUpsert(previous, normalized, targetID, fields, commit.Version, now)
			report.Suppressed = append(report.Suppressed, mergeReport.Suppressed...)
			if !previous.CreatedAt.IsZero() {
				normalized.CreatedAt = previous.CreatedAt
			}
		} else if normalized.CreatedAt.IsZero() {
			normalized.CreatedAt = now
		}
		if _, ok := g.Entities[normalized.ID]; !ok {
			stampFieldSources(&normalized, commit.Version, now)
		}
		normalized.UpdatedAt = now
		normalized.Version = commit.Version
		if err := g.validateUniqueEntity(normalized); err != nil {
			return ApplyReport{}, err
		}
		if previous, ok := g.Entities[normalized.ID]; ok {
			g.removeEntityFromIndexes(normalized.ID, previous)
		}
		clearEntityWriteMetadata(&normalized)
		g.Entities[normalized.ID] = normalized
		g.addEntityToIndexes(normalized.ID, normalized)
		report.AffectedEntityIDs = appendUnique(report.AffectedEntityIDs, normalized.ID)
	}
	for _, request := range commit.Mutations.MarkSourceStale {
		staleReport, err := g.applySourceStale(request, commit.Version, now)
		if err != nil {
			return ApplyReport{}, err
		}
		report.Suppressed = append(report.Suppressed, staleReport.Suppressed...)
		report.AffectedEntityIDs = appendUnique(report.AffectedEntityIDs, staleReport.AffectedEntityIDs...)
	}
	for _, merge := range commit.Mutations.MergeEntities {
		if err := g.applyMerge(merge, commit.Version, now); err != nil {
			return ApplyReport{}, err
		}
	}
	for _, split := range commit.Mutations.SplitEntities {
		if err := g.applySplit(split, commit.Version, now); err != nil {
			return ApplyReport{}, err
		}
		g.rebuildIndexes()
	}
	for _, edge := range commit.Mutations.UpsertEdges {
		normalized, err := normalizeEdge(edge)
		if err != nil {
			return ApplyReport{}, err
		}
		sanitizeIncomingEdgeSources(&normalized)
		incomingID := firstNonEmpty(normalized.ID, normalized.ExternalID)
		normalized = canonicalizeEdge(normalized, commit.Version, now)
		report.CanonicalEdges = append(report.CanonicalEdges, EdgeCanonicalization{
			CanonicalID: normalized.ID,
			IncomingID:  incomingID,
			Type:        normalized.Type,
			From:        normalized.From,
			To:          normalized.To,
		})
		if err := g.validateEdge(normalized); err != nil {
			return ApplyReport{}, err
		}
		if previous, ok := g.Edges[normalized.ID]; ok {
			g.removeEdgeFromIndexes(normalized.ID, previous)
			var mergeReport ApplyReport
			normalized, mergeReport = mergeEdgeForUpsert(previous, normalized, normalized.ID, incomingID, commit.Version, now)
			report.Suppressed = append(report.Suppressed, mergeReport.Suppressed...)
			if !previous.CreatedAt.IsZero() {
				normalized.CreatedAt = previous.CreatedAt
			}
		} else {
			if normalized.CreatedAt.IsZero() {
				normalized.CreatedAt = now
			}
			stampEdgeSources(&normalized, commit.Version, now)
		}
		normalized.UpdatedAt = now
		normalized.Version = commit.Version
		g.Edges[normalized.ID] = normalized
		g.addEdgeToIndexes(normalized.ID, normalized)
		if err := g.validateCardinality(normalized); err != nil {
			return ApplyReport{}, err
		}
	}
	return report, nil
}
