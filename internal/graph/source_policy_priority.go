package graph

import "strings"

func applyFieldPrioritiesToEntity(entity *Entity, policy SourcePolicy) {
	if entity.Source == "" || len(entity.Fields) == 0 || len(policy.FieldPriorities) == 0 {
		return
	}
	for field := range entity.Fields {
		priority, ok := fieldPriorityForEntity(policy, entity.Source, entity.Kind, field)
		if !ok {
			continue
		}
		if entity.FieldSources == nil {
			entity.FieldSources = map[string]FieldSource{}
		}
		entity.FieldSources[field] = FieldSource{
			Source:     entity.Source,
			Priority:   priority,
			Confidence: entity.Confidence,
		}
	}
}

func fieldPriorityForEntity(policy SourcePolicy, source string, kind string, field string) (int, bool) {
	source = strings.TrimSpace(source)
	kind = strings.TrimSpace(kind)
	field = strings.TrimSpace(field)
	if source == "" || field == "" {
		return 0, false
	}
	if kind != "" {
		for _, rule := range policy.FieldPriorities {
			if rule.Source == source && rule.Kind == kind {
				if priority, ok := rule.Fields[field]; ok {
					return priority, true
				}
			}
		}
	}
	for _, rule := range policy.FieldPriorities {
		if rule.Source == source && rule.Kind == "" {
			if priority, ok := rule.Fields[field]; ok {
				return priority, true
			}
		}
	}
	return 0, false
}
