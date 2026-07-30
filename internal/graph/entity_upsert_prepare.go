package graph

import (
	"fmt"
	"time"
)

type preparedEntityUpsert struct {
	entity     Entity
	previous   Entity
	existed    bool
	targetID   string
	canonical  EntityCanonicalization
	suppressed []FieldConflict
}

func (g *Graph) prepareEntityUpsert(
	entity Entity,
	fieldSpecsByKind map[string]map[string]FieldSpec,
	version int64,
	updatedAt time.Time,
) (preparedEntityUpsert, error) {
	normalized, err := normalizeEntity(entity)
	if err != nil {
		return preparedEntityUpsert{}, err
	}
	sanitizeIncomingEntitySources(&normalized)
	fields, err := g.effectiveFieldsCached(normalized.Kind, fieldSpecsByKind)
	if err != nil {
		return preparedEntityUpsert{}, err
	}
	if err := g.applyEntitySchemaWithSpecs(&normalized, fields); err != nil {
		return preparedEntityUpsert{}, err
	}
	incomingID := normalized.ID
	targetID, err := g.resolveEntityID(normalized)
	if err != nil {
		return preparedEntityUpsert{}, err
	}
	if err := g.validateResolvedEntityTarget(incomingID, targetID); err != nil {
		return preparedEntityUpsert{}, err
	}
	prepared := preparedEntityUpsert{
		targetID:  targetID,
		canonical: entityCanonicalization(normalized, incomingID, targetID),
	}
	normalized.ID = targetID
	prepared.suppressed = append(
		prepared.suppressed,
		entityFieldConflictsForTarget(normalized, targetID)...,
	)
	normalized.FieldConflicts = nil
	if previous, ok := g.Entities[targetID]; ok {
		if previous.Kind != normalized.Kind {
			return preparedEntityUpsert{}, fmt.Errorf(
				"entity %q kind change from %q to %q is not allowed",
				targetID,
				previous.Kind,
				normalized.Kind,
			)
		}
		normalized.ID = incomingID
		var mergeReport ApplyReport
		normalized, mergeReport = mergeEntityForUpsert(
			previous,
			normalized,
			targetID,
			fields,
			version,
			updatedAt,
		)
		prepared.suppressed = append(
			prepared.suppressed,
			mergeReport.Suppressed...,
		)
		if !previous.CreatedAt.IsZero() {
			normalized.CreatedAt = previous.CreatedAt
		}
		prepared.previous = previous
		prepared.existed = true
	} else if normalized.CreatedAt.IsZero() {
		normalized.CreatedAt = updatedAt
	}
	prepared.entity = normalized
	return prepared, nil
}
