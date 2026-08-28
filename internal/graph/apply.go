package graph

import (
	"errors"
	"fmt"
	"time"
)

var ErrBatchApplyRequiresIsolation = errors.New("batch apply requires per-commit isolation")

func (g *Graph) ApplyCommit(commit Commit) error {
	_, err := g.ApplyCommitWithOptions(commit, ApplyOptions{})
	return err
}

func (g *Graph) ApplyCommitWithOptions(commit Commit, options ApplyOptions) (ApplyReport, error) {
	clone, report, err := g.ApplyCommitCopyWithOptions(commit, options)
	if err != nil {
		return ApplyReport{}, err
	}
	g.replaceState(clone)
	return report, nil
}

func (g *Graph) replaceState(next *Graph) {
	fingerprint, fingerprintReady := next.contentFingerprintState()
	logicalHashCache := next.cloneLogicalHashCache()
	g.contentFingerprintMu.Lock()
	defer g.contentFingerprintMu.Unlock()
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	g.Version = next.Version
	g.CITypes = next.CITypes
	g.Entities = next.Entities
	g.RelationTypes = next.RelationTypes
	g.Edges = next.Edges
	g.out = next.out
	g.in = next.in
	g.edgeAliasIndex = next.edgeAliasIndex
	g.edgeTypeIndex = next.edgeTypeIndex
	g.entityAliasIndex = next.entityAliasIndex
	g.kindCounts = next.kindCounts
	g.fieldIndex = next.fieldIndex
	g.identityIndex = next.identityIndex
	g.cow = next.cow
	g.contentFingerprint = fingerprint
	g.contentFingerprintReady = fingerprintReady
	g.logicalHashCache = logicalHashCache
	g.invalidateEntityOrder()
}

func (g *Graph) ApplyCommitCopyWithOptions(commit Commit, options ApplyOptions) (*Graph, ApplyReport, error) {
	if commit.Version <= g.Version {
		return nil, ApplyReport{}, fmt.Errorf("commit version %d must be greater than graph version %d", commit.Version, g.Version)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		return nil, ApplyReport{}, err
	}
	return g.applyCommitToCopy(g.Clone(), commit, options)
}

// ApplyCommitStorageCopyWithOptions returns a mutation copy optimized for the
// storage commit path. The result must be treated as immutable after publish;
// unlike Clone, unchanged entity and edge values may be shared.
func (g *Graph) ApplyCommitStorageCopyWithOptions(commit Commit, options ApplyOptions) (*Graph, ApplyReport, error) {
	if commit.Version <= g.Version {
		return nil, ApplyReport{}, fmt.Errorf("commit version %d must be greater than graph version %d", commit.Version, g.Version)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		return nil, ApplyReport{}, err
	}
	return g.applyCommitToCopy(g.cloneForStorageMutation(commit.Mutations), commit, options)
}

// ApplyCommitBatchStorageCopyWithOptions applies an ordered batch to one
// private storage COW graph. A no-op or invalid commit asks the caller to
// discard the private graph and retry with per-commit isolation.
func (g *Graph) ApplyCommitBatchStorageCopyWithOptions(commits []Commit, options []ApplyOptions) (*Graph, []ApplyReport, error) {
	if len(commits) == 0 {
		return g, nil, nil
	}
	if len(options) != 0 && len(options) != len(commits) {
		return nil, nil, fmt.Errorf("batch apply options length %d does not match commits length %d", len(options), len(commits))
	}
	if err := g.ensureContentFingerprint(); err != nil {
		return nil, nil, err
	}
	impact := storageMutationImpact{}
	previousVersion := g.Version
	for _, commit := range commits {
		if commit.Version <= previousVersion {
			return nil, nil, fmt.Errorf("commit version %d must be greater than graph version %d", commit.Version, previousVersion)
		}
		commitImpact := storageMutationImpactFor(commit.Mutations)
		impact.ciTypes = impact.ciTypes || commitImpact.ciTypes
		impact.entities = impact.entities || commitImpact.entities
		impact.relationTypes = impact.relationTypes || commitImpact.relationTypes
		impact.edges = impact.edges || commitImpact.edges
		previousVersion = commit.Version
	}
	next := g.cloneForStorageImpact(impact)
	reports := make([]ApplyReport, 0, len(commits))
	for index, commit := range commits {
		option := ApplyOptions{}
		if len(options) != 0 {
			option = options[index]
		}
		if option.SourcePolicy != nil {
			var policyReport ApplyReport
			var err error
			commit.Mutations, policyReport, err = ApplySourcePolicy(commit.Mutations, *option.SourcePolicy)
			if err != nil {
				return nil, reports, fmt.Errorf("%w: commit %d: %v", ErrBatchApplyRequiresIsolation, index, err)
			}
			next.Version = commit.Version
			report, err := next.applyMutations(commit, ApplyOptions{})
			if err != nil {
				return nil, reports, fmt.Errorf("%w: commit %d: %v", ErrBatchApplyRequiresIsolation, index, err)
			}
			report.Suppressed = append(policyReport.Suppressed, report.Suppressed...)
			if !report.Changed {
				return nil, reports, fmt.Errorf("%w: commit %d is a logical no-op", ErrBatchApplyRequiresIsolation, index)
			}
			reports = append(reports, report)
			continue
		}
		next.Version = commit.Version
		report, err := next.applyMutations(commit, ApplyOptions{})
		if err != nil {
			return nil, reports, fmt.Errorf("%w: commit %d: %v", ErrBatchApplyRequiresIsolation, index, err)
		}
		if !report.Changed {
			return nil, reports, fmt.Errorf("%w: commit %d is a logical no-op", ErrBatchApplyRequiresIsolation, index)
		}
		reports = append(reports, report)
	}
	return next, reports, nil
}

// ApplyCommitInPlaceForStorage replays a commit into a private graph being
// loaded from storage. The caller must discard the graph when an error occurs.
func (g *Graph) ApplyCommitInPlaceForStorage(commit Commit) error {
	if commit.Version <= g.Version {
		return fmt.Errorf("commit version %d must be greater than graph version %d", commit.Version, g.Version)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		return err
	}
	g.Version = commit.Version
	_, err := g.applyMutations(commit, ApplyOptions{})
	return err
}

func (g *Graph) applyCommitToCopy(clone *Graph, commit Commit, options ApplyOptions) (*Graph, ApplyReport, error) {
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
	clone.Version = commit.Version
	report, err := clone.applyMutations(commit, options)
	if err != nil {
		return nil, ApplyReport{}, err
	}
	report.Suppressed = append(policyReport.Suppressed, report.Suppressed...)
	return clone, report, nil
}

func (g *Graph) applyMutations(commit Commit, _ ApplyOptions) (ApplyReport, error) {
	g.invalidateEntityOrder()
	report := ApplyReport{}
	tracker := newMutationFingerprintTracker(g)
	affected := newUniqueStringCollector(&report.AffectedEntityIDs)
	affectedEdges := newUniqueStringCollector(&report.AffectedEdgeIDs)
	now := commit.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for _, edgeID := range commit.Mutations.DeleteEdges {
		if edgeID == "" {
			return ApplyReport{}, fmt.Errorf("delete edge id is required")
		}
		resolved, err := g.resolveEdgeReference(edgeID)
		if err != nil {
			return ApplyReport{}, err
		}
		if resolved != "" {
			tracker.touchEdge(resolved)
			affectedEdges.add(resolved)
			edge := g.Edges[resolved]
			g.removeEdgeFromIndexes(resolved, edge)
			delete(g.Edges, resolved)
		} else {
			tracker.touchEdge(edgeID)
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
		tracker.touchEdge(edgeID)
		edge = copyEdge(edge)
		backfillEdgeSources(&edge, edge.Version, edge.UpdatedAt)
		existingOwner := *edge.ExistenceSource
		incomingOwner := sourceForDelete(request, commit.Version, now)
		if sourceCanDeleteEdge(existingOwner, incomingOwner) {
			affectedEdges.add(edgeID)
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
		entity = copyEntity(entity)
		backfillFieldSources(&entity)
		existingOwner := *entity.ExistenceSource
		incomingOwner := sourceForEntityDelete(request, commit.Version, now)
		if sourceCanDeleteEntity(existingOwner, incomingOwner) {
			tracker.touchEntityWithEdges(entityID)
			affectedEdges.add(g.incidentEdgeIDs(entityID)...)
			g.deleteEntityForce(entityID)
			affected.add(entityID)
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
		tracker.touchEntityWithEdges(resolved)
		affectedEdges.add(g.incidentEdgeIDs(resolved)...)
		g.deleteEntityForce(resolved)
		affected.add(resolved)
	}
	if err := g.applyRelationTypeDeletes(
		commit.Mutations.DeleteRelationTypes,
		tracker,
		affectedEdges,
	); err != nil {
		return ApplyReport{}, err
	}
	if err := g.applyCITypeMutations(
		commit.Mutations.DeleteCITypes,
		commit.Mutations.UpsertCITypes,
		tracker,
	); err != nil {
		return ApplyReport{}, err
	}
	if err := g.applyRelationTypeUpserts(
		commit.Mutations.UpsertRelationTypes,
		tracker,
	); err != nil {
		return ApplyReport{}, err
	}
	uniqueValidator, err := newUniqueEntityValidator(
		g,
		entityMutationKinds(g, commit.Mutations),
	)
	if err != nil {
		return ApplyReport{}, err
	}
	fieldSpecsByKind := make(map[string]map[string]FieldSpec)
	for _, entity := range commit.Mutations.UpsertEntities {
		prepared, err := g.prepareEntityUpsert(
			entity,
			fieldSpecsByKind,
			commit.Version,
			now,
		)
		if err != nil {
			return ApplyReport{}, err
		}
		normalized := prepared.entity
		tracker.touchEntity(prepared.targetID)
		report.CanonicalEntities = append(report.CanonicalEntities, prepared.canonical)
		report.Suppressed = append(report.Suppressed, prepared.suppressed...)
		if !prepared.existed {
			stampFieldSources(&normalized, commit.Version, now)
		}
		normalized.UpdatedAt = now
		normalized.Version = commit.Version
		if err := uniqueValidator.validate(normalized); err != nil {
			return ApplyReport{}, err
		}
		if err := g.validateIdentityIndexAvailable(normalized); err != nil {
			return ApplyReport{}, err
		}
		if prepared.existed {
			g.removeEntityFromIndexes(normalized.ID, prepared.previous)
		}
		clearEntityWriteMetadata(&normalized)
		g.Entities[normalized.ID] = normalized
		g.addEntityToIndexes(normalized.ID, normalized)
		uniqueValidator.add(normalized)
		affected.add(normalized.ID)
	}
	for _, request := range commit.Mutations.MarkSourceStale {
		staleReport, err := g.applySourceStale(request, commit.Version, now, tracker)
		if err != nil {
			return ApplyReport{}, err
		}
		report.Suppressed = append(report.Suppressed, staleReport.Suppressed...)
		affected.add(staleReport.AffectedEntityIDs...)
		affectedEdges.add(staleReport.AffectedEdgeIDs...)
	}
	for _, merge := range commit.Mutations.MergeEntities {
		tracker.touchMerge(merge)
		if err := g.applyMerge(
			merge,
			commit.Version,
			now,
			uniqueValidator,
			fieldSpecsByKind,
			tracker,
			affected,
			affectedEdges,
		); err != nil {
			return ApplyReport{}, err
		}
	}
	for _, split := range commit.Mutations.SplitEntities {
		tracker.touchSplit(split)
		if err := g.applySplit(
			split,
			commit.Version,
			now,
			uniqueValidator,
			fieldSpecsByKind,
			affected,
			affectedEdges,
		); err != nil {
			return ApplyReport{}, err
		}
	}
	for _, edge := range commit.Mutations.UpsertEdges {
		normalized, err := normalizeEdge(edge)
		if err != nil {
			return ApplyReport{}, err
		}
		sanitizeIncomingEdgeSources(&normalized)
		incomingID := firstNonEmpty(normalized.ID, normalized.ExternalID)
		normalized = canonicalizeEdge(normalized, commit.Version, now)
		tracker.touchEdge(normalized.ID)
		affectedEdges.add(normalized.ID)
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
	if err := tracker.finish(&report); err != nil {
		return ApplyReport{}, err
	}
	return report, nil
}
