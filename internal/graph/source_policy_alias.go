package graph

import (
	"reflect"
	"sort"
	"strings"
)

func applyFieldAliasesToEntity(entity *Entity, policy SourcePolicy) []FieldConflict {
	if entity.Source == "" || len(entity.Fields) == 0 || len(policy.FieldAliases) == 0 {
		return nil
	}
	aliases := aliasesForEntity(policy, entity.Source, entity.Kind)
	if len(aliases) == 0 {
		return nil
	}
	groups := map[string][]fieldAliasValue{}
	for alias, canonical := range aliases {
		value, ok := entity.Fields[alias]
		if ok {
			groups[canonical] = append(groups[canonical], fieldAliasValue{Alias: alias, Value: value, Mode: entity.FieldWriteModes[alias]})
		}
	}
	if len(groups) == 0 {
		return nil
	}
	fields := make(Fields, len(entity.Fields))
	for key, value := range entity.Fields {
		fields[key] = value
	}
	fieldSources := copyFieldSourceMap(entity.FieldSources)
	fieldModes := copyFieldWriteModes(entity.FieldWriteModes)
	conflicts := make([]FieldConflict, 0)
	for _, canonical := range sortedAliasGroupKeys(groups) {
		values := groups[canonical]
		sort.Slice(values, func(i, j int) bool {
			return values[i].Alias < values[j].Alias
		})
		if canonicalValue, ok := fields[canonical]; ok {
			if marked, ok := firstMarkedAlias(values); ok {
				delete(fields, marked.Alias)
				moveAliasFieldSource(fieldSources, marked.Alias, canonical)
				moveAliasFieldMode(fieldModes, marked.Alias, canonical)
				if !reflect.DeepEqual(canonicalValue, marked.Value) {
					conflicts = append(conflicts, aliasFieldConflict(*entity, canonical, canonical, marked.Value, canonicalValue, "incoming canonical field value was ignored because override alias field is present"))
				}
				fields[canonical] = marked.Value
				for _, incoming := range values {
					if incoming.Alias == marked.Alias {
						continue
					}
					delete(fields, incoming.Alias)
					delete(fieldSources, incoming.Alias)
					delete(fieldModes, incoming.Alias)
					if !reflect.DeepEqual(marked.Value, incoming.Value) {
						conflicts = append(conflicts, aliasFieldConflict(*entity, canonical, incoming.Alias, marked.Value, incoming.Value, "incoming alias field value was ignored because another override alias mapped to the same canonical field"))
					}
				}
				continue
			}
			for _, incoming := range values {
				delete(fields, incoming.Alias)
				delete(fieldSources, incoming.Alias)
				delete(fieldModes, incoming.Alias)
				if !reflect.DeepEqual(canonicalValue, incoming.Value) {
					conflicts = append(conflicts, aliasFieldConflict(*entity, canonical, incoming.Alias, canonicalValue, incoming.Value, "incoming alias field value was ignored because canonical field is present"))
				}
			}
			continue
		}
		chosen := values[0]
		fields[canonical] = chosen.Value
		moveAliasFieldSource(fieldSources, chosen.Alias, canonical)
		moveAliasFieldMode(fieldModes, chosen.Alias, canonical)
		delete(fields, chosen.Alias)
		for _, incoming := range values[1:] {
			delete(fields, incoming.Alias)
			delete(fieldSources, incoming.Alias)
			delete(fieldModes, incoming.Alias)
			if !reflect.DeepEqual(chosen.Value, incoming.Value) {
				conflicts = append(conflicts, aliasFieldConflict(*entity, canonical, incoming.Alias, chosen.Value, incoming.Value, "incoming alias field value was ignored because another alias mapped to the same canonical field"))
			}
		}
	}
	entity.Fields = fields
	remapFieldConflicts(entity.FieldConflicts, aliases)
	if len(fieldSources) == 0 {
		entity.FieldSources = nil
	} else {
		entity.FieldSources = fieldSources
	}
	entity.FieldWriteModes = trimFieldWriteModesToFields(fieldModes, fields)
	return conflicts
}

type fieldAliasValue struct {
	Alias string
	Value any
	Mode  string
}

func aliasesForEntity(policy SourcePolicy, source string, kind string) map[string]string {
	source = strings.TrimSpace(source)
	kind = strings.TrimSpace(kind)
	out := map[string]string{}
	for _, rule := range policy.FieldAliases {
		if rule.Source == source && rule.Kind == "" {
			for alias, canonical := range rule.Aliases {
				out[alias] = canonical
			}
		}
	}
	if kind == "" {
		return out
	}
	for _, rule := range policy.FieldAliases {
		if rule.Source == source && rule.Kind == kind {
			for alias, canonical := range rule.Aliases {
				out[alias] = canonical
			}
		}
	}
	return out
}

func aliasFieldConflict(entity Entity, canonical string, alias string, existing any, incoming any, message string) FieldConflict {
	return FieldConflict{
		ResourceType:     "entity",
		EntityID:         entity.ID,
		IncomingID:       firstNonEmptyString(entity.ExternalID, entity.ID),
		Field:            canonical,
		AliasField:       alias,
		ExistingSource:   entity.Source,
		ExistingPriority: entity.SourceRank,
		IncomingSource:   entity.Source,
		IncomingPriority: entity.SourceRank,
		ExistingValue:    existing,
		IncomingValue:    incoming,
		Message:          message,
	}
}

func copyFieldSourceMap(values map[string]FieldSource) map[string]FieldSource {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]FieldSource, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func moveAliasFieldSource(values map[string]FieldSource, alias string, canonical string) {
	if len(values) == 0 {
		return
	}
	if _, ok := values[canonical]; ok {
		delete(values, alias)
		return
	}
	if source, ok := values[alias]; ok {
		values[canonical] = source
		delete(values, alias)
	}
}

func moveAliasFieldMode(values map[string]string, alias string, canonical string) {
	if len(values) == 0 {
		return
	}
	if mode, ok := values[alias]; ok {
		values[canonical] = mode
		delete(values, alias)
	}
}

func firstMarkedAlias(values []fieldAliasValue) (fieldAliasValue, bool) {
	for _, value := range values {
		if value.Mode == FieldMergeReplace {
			return value, true
		}
	}
	return fieldAliasValue{}, false
}

func remapFieldConflicts(conflicts []FieldConflict, aliases map[string]string) {
	for i := range conflicts {
		if canonical, ok := aliases[conflicts[i].Field]; ok {
			conflicts[i].Field = canonical
		}
	}
}

func sortedAliasGroupKeys(groups map[string][]fieldAliasValue) []string {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
