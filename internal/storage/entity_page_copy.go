package storage

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func copyEntityPage(page EntityPageData) EntityPageData {
	page.Entities = copyGraphEntities(page.Entities)
	return page
}

func copyGraphEntities(entities []graph.Entity) []graph.Entity {
	if len(entities) == 0 {
		return nil
	}
	out := make([]graph.Entity, len(entities))
	for i, entity := range entities {
		out[i] = copyEntityShape(entity)
	}
	return out
}

func copyEntityShape(entity graph.Entity) graph.Entity {
	entity.Fields = copyFieldsShape(entity.Fields)
	entity.FieldSources = copyFieldSourcesShape(entity.FieldSources)
	if entity.ExistenceSource != nil {
		source := *entity.ExistenceSource
		entity.ExistenceSource = &source
	}
	entity.Identity = copyMapAnyShape(entity.Identity)
	entity.Sources = append([]graph.EntitySource(nil), entity.Sources...)
	entity.MergedFrom = append([]string(nil), entity.MergedFrom...)
	return entity
}

func copyFieldsShape(fields graph.Fields) graph.Fields {
	if fields == nil {
		return nil
	}
	out := make(graph.Fields, len(fields))
	for key, value := range fields {
		out[key] = copyAnyShape(value)
	}
	return out
}

func copyFieldSourcesShape(sources map[string]graph.FieldSource) map[string]graph.FieldSource {
	if sources == nil {
		return nil
	}
	out := make(map[string]graph.FieldSource, len(sources))
	for key, value := range sources {
		out[key] = value
	}
	return out
}

func copyMapAnyShape(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = copyAnyShape(value)
	}
	return out
}

func copyAnyShape(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case graph.Fields:
		return copyFieldsShape(typed)
	case map[string]any:
		return copyMapAnyShape(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = copyAnyShape(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	case []int:
		return append([]int(nil), typed...)
	case []int64:
		return append([]int64(nil), typed...)
	case []float64:
		return append([]float64(nil), typed...)
	case []bool:
		return append([]bool(nil), typed...)
	default:
		return value
	}
}
