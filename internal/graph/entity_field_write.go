package graph

import (
	"fmt"
	"sort"
	"strings"
)

func PrepareEntityFieldWrites(mutations Mutations) (Mutations, error) {
	mutations.UpsertEntities = append([]Entity(nil), mutations.UpsertEntities...)
	for i := range mutations.UpsertEntities {
		if err := prepareEntityFieldWrites(&mutations.UpsertEntities[i]); err != nil {
			return Mutations{}, err
		}
	}
	mutations.SplitEntities = append([]SplitRequest(nil), mutations.SplitEntities...)
	for i := range mutations.SplitEntities {
		mutations.SplitEntities[i].Entities = append([]Entity(nil), mutations.SplitEntities[i].Entities...)
		for j := range mutations.SplitEntities[i].Entities {
			if err := prepareEntityFieldWrites(&mutations.SplitEntities[i].Entities[j]); err != nil {
				return Mutations{}, err
			}
		}
	}
	return mutations, nil
}

func prepareEntityFieldWrites(entity *Entity) error {
	fields, modes, conflicts, err := normalizeEntityFieldWrites(*entity)
	if err != nil {
		return err
	}
	entity.Fields = fields
	entity.FieldWriteModes = modes
	entity.FieldConflicts = conflicts
	return nil
}

func normalizeEntityFieldWrites(entity Entity) (Fields, map[string]string, []FieldConflict, error) {
	fields, err := normalizeFields(entity.Fields)
	if err != nil {
		return nil, nil, nil, err
	}
	modes, err := normalizeFieldWriteModes(entity.FieldWriteModes)
	if err != nil {
		return nil, nil, nil, err
	}
	out := make(Fields, len(fields))
	marked := map[string]bool{}
	conflicts := make([]FieldConflict, 0)
	for _, rawField := range sortedFieldKeys(fields) {
		field, override, err := parseFieldOverrideMarker(rawField)
		if err != nil {
			return nil, nil, nil, err
		}
		value := fields[rawField]
		current, exists := out[field]
		if !exists {
			out[field] = value
			if override {
				if modes == nil {
					modes = map[string]string{}
				}
				modes[field] = FieldMergeReplace
				marked[field] = true
			}
			continue
		}
		if override || marked[field] {
			winner, loser := current, value
			loserField := rawField
			if override {
				winner, loser = value, current
				out[field] = value
				if modes == nil {
					modes = map[string]string{}
				}
				modes[field] = FieldMergeReplace
				marked[field] = true
				loserField = field
			}
			if !fieldValuesEqual(winner, loser) {
				conflicts = append(conflicts, samePayloadFieldConflict(entity, field, loserField, winner, loser))
			}
			continue
		}
	}
	modes = trimFieldWriteModesToFields(modes, out)
	return out, modes, conflicts, nil
}

func normalizeFieldWriteModes(values map[string]string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(values))
	for field, mode := range values {
		field = strings.TrimSpace(field)
		mode = strings.TrimSpace(mode)
		if field == "" {
			return nil, fmt.Errorf("field write mode field name is required")
		}
		if strings.HasSuffix(field, "!") {
			return nil, fmt.Errorf("field write mode field %q must not end with !", field)
		}
		if mode == "" {
			continue
		}
		if mode != FieldMergeReplace {
			return nil, fmt.Errorf("field write mode %q for field %q is unsupported", mode, field)
		}
		out[field] = mode
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func parseFieldOverrideMarker(rawField string) (string, bool, error) {
	if !strings.HasSuffix(rawField, "!") {
		if strings.TrimSpace(rawField) == "" {
			return "", false, fmt.Errorf("field name is required")
		}
		return rawField, false, nil
	}
	field := strings.TrimSuffix(rawField, "!")
	if strings.TrimSpace(field) == "" || strings.HasSuffix(field, "!") {
		return "", false, fmt.Errorf("field override marker %q is invalid", rawField)
	}
	return field, true, nil
}

func samePayloadFieldConflict(entity Entity, field string, ignoredField string, winner any, loser any) FieldConflict {
	return FieldConflict{
		ResourceType:     "entity",
		EntityID:         entity.ID,
		IncomingID:       firstNonEmptyString(entity.ExternalID, entity.ID),
		Field:            field,
		AliasField:       ignoredField,
		ExistingSource:   entity.Source,
		ExistingPriority: entity.SourceRank,
		IncomingSource:   entity.Source,
		IncomingPriority: entity.SourceRank,
		ExistingValue:    copyAny(winner),
		IncomingValue:    copyAny(loser),
		Message:          "incoming field value was ignored because override marker field is present",
	}
}

func sortedFieldKeys(fields Fields) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func trimFieldWriteModesToFields(modes map[string]string, fields Fields) map[string]string {
	if len(modes) == 0 {
		return nil
	}
	out := make(map[string]string, len(modes))
	for field, mode := range modes {
		if _, ok := fields[field]; ok && mode != "" {
			out[field] = mode
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func entityFieldConflictsForTarget(entity Entity, targetID string) []FieldConflict {
	if len(entity.FieldConflicts) == 0 {
		return nil
	}
	out := make([]FieldConflict, len(entity.FieldConflicts))
	for i, conflict := range entity.FieldConflicts {
		conflict.EntityID = targetID
		if conflict.IncomingID == "" {
			conflict.IncomingID = firstNonEmptyString(entity.ExternalID, entity.ID)
		}
		out[i] = conflict
	}
	return out
}

func clearEntityWriteMetadata(entity *Entity) {
	entity.FieldWriteModes = nil
	entity.FieldConflicts = nil
}
