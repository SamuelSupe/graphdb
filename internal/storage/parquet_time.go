package storage

import (
	"maps"
	"slices"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// Typed timestamps must have the same UTC representation in JSON content
// hashes and Parquet columns. Arbitrary field values retain their wire values.
// Copy nested collections only when converting them; callers may share them.
func entityTimesUTC(entity graph.Entity) graph.Entity {
	entity.CreatedAt = entity.CreatedAt.UTC()
	entity.UpdatedAt = entity.UpdatedAt.UTC()
	entity.FieldSources = fieldSourceTimesUTC(entity.FieldSources)
	entity.ExistenceSource = existenceSourceTimeUTC(entity.ExistenceSource)
	copied := false
	for i, source := range entity.Sources {
		if source.ObservedAt.Location() == time.UTC && source.StaleAt.Location() == time.UTC {
			continue
		}
		if !copied {
			entity.Sources = slices.Clone(entity.Sources)
			copied = true
		}
		entity.Sources[i].ObservedAt = source.ObservedAt.UTC()
		entity.Sources[i].StaleAt = source.StaleAt.UTC()
	}
	return entity
}

func edgeTimesUTC(edge graph.Edge) graph.Edge {
	edge.CreatedAt = edge.CreatedAt.UTC()
	edge.UpdatedAt = edge.UpdatedAt.UTC()
	edge.FieldSources = fieldSourceTimesUTC(edge.FieldSources)
	edge.ExistenceSource = existenceSourceTimeUTC(edge.ExistenceSource)
	copied := false
	for i, source := range edge.Sources {
		if source.ObservedAt.Location() == time.UTC {
			continue
		}
		if !copied {
			edge.Sources = slices.Clone(edge.Sources)
			copied = true
		}
		edge.Sources[i].ObservedAt = source.ObservedAt.UTC()
	}
	return edge
}

func fieldSourceTimesUTC(sources map[string]graph.FieldSource) map[string]graph.FieldSource {
	copied := false
	for key, source := range sources {
		if source.UpdatedAt.Location() == time.UTC {
			continue
		}
		if !copied {
			sources = maps.Clone(sources)
			copied = true
		}
		source.UpdatedAt = source.UpdatedAt.UTC()
		sources[key] = source
	}
	return sources
}

func existenceSourceTimeUTC(source *graph.FieldSource) *graph.FieldSource {
	if source == nil || source.UpdatedAt.Location() == time.UTC {
		return source
	}
	normalized := *source
	normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	return &normalized
}

func entitySliceTimesUTC(entities []graph.Entity) []graph.Entity {
	entities = slices.Clone(entities)
	for i := range entities {
		entities[i] = entityTimesUTC(entities[i])
	}
	return entities
}

func edgeSliceTimesUTC(edges []graph.Edge) []graph.Edge {
	edges = slices.Clone(edges)
	for i := range edges {
		edges[i] = edgeTimesUTC(edges[i])
	}
	return edges
}

func mutationTimesUTC(mutations graph.Mutations) graph.Mutations {
	mutations.UpsertEntities = entitySliceTimesUTC(mutations.UpsertEntities)
	mutations.UpsertEdges = edgeSliceTimesUTC(mutations.UpsertEdges)
	mutations.SplitEntities = slices.Clone(mutations.SplitEntities)
	for i := range mutations.SplitEntities {
		mutations.SplitEntities[i].Entities = entitySliceTimesUTC(mutations.SplitEntities[i].Entities)
	}
	return mutations
}

func ingestRequestTimesUTC(request IngestRequest) IngestRequest {
	request.Items = slices.Clone(request.Items)
	for i := range request.Items {
		item := &request.Items[i]
		if item.Entity != nil {
			entity := entityTimesUTC(*item.Entity)
			item.Entity = &entity
		}
		if item.Edge != nil {
			edge := edgeTimesUTC(*item.Edge)
			item.Edge = &edge
		}
	}
	return request
}
