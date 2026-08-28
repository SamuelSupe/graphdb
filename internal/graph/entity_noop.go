package graph

import (
	"fmt"
	"time"
)

// PreviewStorageEntityNoop validates a pure entity-upsert commit and reports
// whether applying it would leave the logical graph unchanged. A false result
// means the caller must use the normal mutation path.
func (g *Graph) PreviewStorageEntityNoop(commit Commit) (ApplyReport, bool, error) {
	if !onlyEntityUpserts(commit.Mutations) {
		return ApplyReport{}, false, nil
	}
	if commit.Version <= g.Version {
		return ApplyReport{}, false, fmt.Errorf(
			"commit version %d must be greater than graph version %d",
			commit.Version,
			g.Version,
		)
	}
	if err := g.ensureContentFingerprint(); err != nil {
		return ApplyReport{}, false, err
	}
	now := commit.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	uniqueValidator, err := newUniqueEntityValidator(
		g,
		entityMutationKinds(g, commit.Mutations),
	)
	if err != nil {
		return ApplyReport{}, false, err
	}
	fieldSpecsByKind := make(map[string]map[string]FieldSpec)
	report := ApplyReport{}
	affected := newUniqueStringCollector(&report.AffectedEntityIDs)
	for _, entity := range commit.Mutations.UpsertEntities {
		prepared, err := g.prepareEntityUpsert(
			entity,
			fieldSpecsByKind,
			commit.Version,
			now,
		)
		if err != nil {
			return ApplyReport{}, false, err
		}
		if !prepared.existed {
			return ApplyReport{}, false, nil
		}
		normalized := prepared.entity
		normalized.UpdatedAt = now
		normalized.Version = commit.Version
		if err := uniqueValidator.validate(normalized); err != nil {
			return ApplyReport{}, false, err
		}
		if err := g.validateIdentityIndexAvailable(normalized); err != nil {
			return ApplyReport{}, false, err
		}
		clearEntityWriteMetadata(&normalized)
		before, err := contentFingerprintEntry(
			"entity",
			prepared.targetID,
			logicalEntityForHash(prepared.previous),
		)
		if err != nil {
			return ApplyReport{}, false, err
		}
		after, err := contentFingerprintEntry(
			"entity",
			prepared.targetID,
			logicalEntityForHash(normalized),
		)
		if err != nil {
			return ApplyReport{}, false, err
		}
		if before != after {
			return ApplyReport{}, false, nil
		}
		uniqueValidator.add(normalized)
		affected.add(prepared.targetID)
		report.CanonicalEntities = append(
			report.CanonicalEntities,
			prepared.canonical,
		)
		report.Suppressed = append(report.Suppressed, prepared.suppressed...)
	}
	report.ContentFingerprint, err = g.ContentFingerprint()
	if err != nil {
		return ApplyReport{}, false, err
	}
	return report, true, nil
}

func onlyEntityUpserts(mutations Mutations) bool {
	return len(mutations.UpsertEntities) > 0 &&
		len(mutations.UpsertCITypes) == 0 &&
		len(mutations.DeleteCITypes) == 0 &&
		len(mutations.UpsertRelationTypes) == 0 &&
		len(mutations.DeleteRelationTypes) == 0 &&
		len(mutations.DeleteEntities) == 0 &&
		len(mutations.DeleteEntityRequests) == 0 &&
		len(mutations.MarkSourceStale) == 0 &&
		len(mutations.UpsertEdges) == 0 &&
		len(mutations.DeleteEdges) == 0 &&
		len(mutations.DeleteEdgeRequests) == 0 &&
		len(mutations.MergeEntities) == 0 &&
		len(mutations.SplitEntities) == 0
}
