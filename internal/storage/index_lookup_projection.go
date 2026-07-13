package storage

import (
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func trimEntityFields(entity graph.Entity, fields []string) graph.Entity {
	if len(fields) == 0 {
		return graph.CopyEntity(entity)
	}
	keep := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if fieldName, ok := entityFieldName(field); ok {
			keep[fieldName] = struct{}{}
		}
	}
	if len(keep) == 0 {
		entity.Fields = graph.Fields{}
		entity.FieldSources = nil
		return graph.CopyEntity(entity)
	}

	selected := make(graph.Fields, len(keep))
	for field := range keep {
		if value, ok := entity.Fields[field]; ok {
			selected[field] = value
		}
	}
	entity.Fields = selected
	entity.FieldSources = trimFieldSources(entity.FieldSources, keep)
	return graph.CopyEntity(entity)
}

func trimFieldSources(sources map[string]graph.FieldSource, keep map[string]struct{}) map[string]graph.FieldSource {
	if len(sources) == 0 || len(keep) == 0 {
		return nil
	}
	next := make(map[string]graph.FieldSource, min(len(sources), len(keep)))
	for field := range keep {
		if source, ok := sources[field]; ok {
			next[field] = source
		}
	}
	if len(next) == 0 {
		return nil
	}
	return next
}

func entityFieldName(field string) (string, bool) {
	switch field {
	case "", "id", "kind", "source", "external_id", "confidence", "source_priority", "created_at", "updated_at":
		return "", false
	}
	if strings.HasPrefix(field, "identity.") {
		return "", false
	}
	if strings.HasPrefix(field, "fields.") {
		name := strings.TrimPrefix(field, "fields.")
		return name, name != ""
	}
	return field, true
}
