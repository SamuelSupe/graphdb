package graph

import (
	"reflect"
	"time"
)

func stampFieldSources(entity *Entity, version int64, updatedAt time.Time) {
	owner := writeOwnerFromEntity(*entity, version, updatedAt)
	entity.ExistenceSource = &owner
	if entity.FieldSources == nil {
		entity.FieldSources = map[string]FieldSource{}
	}
	for field := range entity.Fields {
		if source, ok := entity.FieldSources[field]; ok {
			entity.FieldSources[field] = stampFieldSource(source, owner)
			continue
		}
		entity.FieldSources[field] = owner
	}
}

func mergeEntityForUpsert(existing Entity, incoming Entity, targetID string, fieldSpecs map[string]FieldSpec, version int64, updatedAt time.Time) (Entity, ApplyReport) {
	merged := copyEntity(existing)
	backfillFieldSources(&merged)
	incomingOwner := writeOwnerFromEntity(incoming, version, updatedAt)
	incomingID := firstNonEmpty(incoming.ExternalID, incoming.ID)
	report := ApplyReport{}
	for field, value := range incoming.Fields {
		current, exists := merged.Fields[field]
		fieldOwner := incomingFieldOwner(incoming, field, incomingOwner, version, updatedAt)
		if shouldAppendUnique(fieldSpecs[field], incoming, field) {
			if !exists || isEmptyFieldValue(current) {
				merged.Fields[field] = copyAny(value)
				setFieldSource(&merged, field, fieldOwner)
				continue
			}
			mergedValue, changed, ok := appendUniqueArrayField(current, value)
			if ok {
				if changed {
					merged.Fields[field] = mergedValue
					setFieldSource(&merged, field, betterFieldSource(fieldSourceOrEntityOwner(merged, field), fieldOwner))
				}
				continue
			}
		}
		if !exists || isEmptyFieldValue(current) {
			merged.Fields[field] = copyAny(value)
			setFieldSource(&merged, field, fieldOwner)
			continue
		}
		existingOwner := fieldSourceOrEntityOwner(merged, field)
		if isEmptyFieldValue(value) {
			report.Suppressed = append(report.Suppressed, FieldConflict{
				ResourceType:     "entity",
				EntityID:         targetID,
				IncomingID:       incomingID,
				Field:            field,
				ExistingSource:   existingOwner.Source,
				ExistingPriority: existingOwner.Priority,
				IncomingSource:   fieldOwner.Source,
				IncomingPriority: fieldOwner.Priority,
				ExistingValue:    current,
				IncomingValue:    value,
				Message:          "incoming empty field value was ignored because upsert does not clear existing values",
			})
			continue
		}
		if incomingCanOverwrite(existingOwner, fieldOwner) {
			merged.Fields[field] = copyAny(value)
			setFieldSource(&merged, field, fieldOwner)
			continue
		}
		if reflect.DeepEqual(current, value) {
			continue
		}
		report.Suppressed = append(report.Suppressed, FieldConflict{
			ResourceType:     "entity",
			EntityID:         targetID,
			IncomingID:       incomingID,
			Field:            field,
			ExistingSource:   existingOwner.Source,
			ExistingPriority: existingOwner.Priority,
			IncomingSource:   fieldOwner.Source,
			IncomingPriority: fieldOwner.Priority,
			ExistingValue:    current,
			IncomingValue:    value,
			Message:          "incoming field value was ignored because source priority is lower",
		})
	}
	for key, value := range incoming.Identity {
		if _, ok := merged.Identity[key]; !ok || entityRankCanOverwrite(incoming, existing) {
			merged.Identity[key] = copyAny(value)
		}
	}
	merged.Sources = mergeSources(merged.Sources, incoming.Sources)
	merged.Confidence = maxFloat(existing.Confidence, incoming.Confidence)
	merged.SourceRank = maxInt(existing.SourceRank, incoming.SourceRank)
	merged.Source = firstNonEmpty(existing.Source, incoming.Source)
	merged.ExternalID = firstNonEmpty(existing.ExternalID, incoming.ExternalID)
	if incoming.ID != "" && incoming.ID != existing.ID {
		merged.MergedFrom = appendUnique(merged.MergedFrom, incoming.ID)
	}
	merged.MergedFrom = appendUnique(merged.MergedFrom, incoming.MergedFrom...)
	merged.ID = targetID
	clearEntityWriteMetadata(&merged)
	return merged, report
}

func shouldAppendUnique(spec FieldSpec, incoming Entity, field string) bool {
	return effectiveMergeStrategy(spec) == FieldMergeAppendUnique && incoming.FieldWriteModes[field] != FieldMergeReplace
}

func effectiveMergeStrategy(spec FieldSpec) string {
	if spec.MergeStrategy == "" {
		return FieldMergeReplace
	}
	return spec.MergeStrategy
}

func appendUniqueArrayField(existing any, incoming any) ([]any, bool, bool) {
	existingValues, ok := existing.([]any)
	if !ok {
		return nil, false, false
	}
	incomingValues, ok := incoming.([]any)
	if !ok {
		return nil, false, false
	}
	merged := copyAnySlice(existingValues)
	changed := false
	for _, value := range incomingValues {
		if arrayContainsValue(merged, value) {
			continue
		}
		merged = append(merged, copyAny(value))
		changed = true
	}
	return merged, changed, true
}

func arrayContainsValue(values []any, value any) bool {
	for _, existing := range values {
		if fieldValuesEqual(existing, value) {
			return true
		}
	}
	return false
}

func backfillFieldSources(entity *Entity) {
	owner := ownerFromEntity(*entity, entity.Version, entity.UpdatedAt)
	if entity.ExistenceSource == nil {
		entity.ExistenceSource = &owner
	}
	if entity.FieldSources == nil {
		entity.FieldSources = map[string]FieldSource{}
	}
	for field := range entity.Fields {
		if _, ok := entity.FieldSources[field]; !ok {
			entity.FieldSources[field] = owner
		}
	}
}

func fieldSourceOrEntityOwner(entity Entity, field string) FieldSource {
	if source, ok := entity.FieldSources[field]; ok {
		return source
	}
	if entity.ExistenceSource != nil {
		return *entity.ExistenceSource
	}
	return ownerFromEntity(entity, entity.Version, entity.UpdatedAt)
}

func incomingFieldOwner(entity Entity, field string, fallback FieldSource, version int64, updatedAt time.Time) FieldSource {
	source, ok := entity.FieldSources[field]
	if !ok {
		return fallback
	}
	return stampFieldSource(source, fallbackWithVersion(fallback, version, updatedAt))
}

func stampFieldSource(source FieldSource, fallback FieldSource) FieldSource {
	if source.Source == "" {
		source.Source = fallback.Source
	}
	if source.Version == 0 {
		source.Version = fallback.Version
	}
	if source.UpdatedAt.IsZero() {
		source.UpdatedAt = fallback.UpdatedAt
	}
	return source
}

func fallbackWithVersion(source FieldSource, version int64, updatedAt time.Time) FieldSource {
	source.Version = version
	source.UpdatedAt = updatedAt
	return source
}

func setFieldSource(entity *Entity, field string, source FieldSource) {
	if entity.FieldSources == nil {
		entity.FieldSources = map[string]FieldSource{}
	}
	entity.FieldSources[field] = source
}

func ownerFromEntity(entity Entity, version int64, updatedAt time.Time) FieldSource {
	best := FieldSource{
		Source:     entity.Source,
		Priority:   entity.SourceRank,
		Confidence: entity.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
	for _, source := range entity.Sources {
		if source.Priority > best.Priority || (source.Priority == best.Priority && source.Confidence > best.Confidence) {
			best.Source = source.Source
			best.Priority = source.Priority
			best.Confidence = source.Confidence
		}
	}
	return best
}

func writeOwnerFromEntity(entity Entity, version int64, updatedAt time.Time) FieldSource {
	return FieldSource{
		Source:     entity.Source,
		Priority:   entity.SourceRank,
		Confidence: entity.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
}

func incomingCanOverwrite(existing FieldSource, incoming FieldSource) bool {
	if incoming.Priority != existing.Priority {
		return incoming.Priority > existing.Priority
	}
	return incoming.Confidence >= existing.Confidence
}

func betterFieldSource(existing FieldSource, incoming FieldSource) FieldSource {
	if incoming.Priority != existing.Priority {
		if incoming.Priority > existing.Priority {
			return incoming
		}
		return existing
	}
	if incoming.Confidence > existing.Confidence {
		return incoming
	}
	return existing
}

func isEmptyFieldValue(value any) bool {
	if value == nil {
		return true
	}
	if text, ok := value.(string); ok {
		return text == ""
	}
	return false
}
