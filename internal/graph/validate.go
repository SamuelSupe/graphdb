package graph

import (
	"fmt"
	"strings"
)

func normalizeEntity(entity Entity) (Entity, error) {
	entity.ID = strings.TrimSpace(entity.ID)
	entity.Kind = strings.TrimSpace(entity.Kind)
	entity.Source = strings.TrimSpace(entity.Source)
	entity.ExternalID = strings.TrimSpace(entity.ExternalID)
	if entity.Kind == "" {
		return Entity{}, fmt.Errorf("entity %q requires kind", entity.ID)
	}
	entity.Sources = normalizeSources(entity)
	if entity.ID == "" && len(entity.Sources) == 0 {
		return Entity{}, fmt.Errorf("entity id or source/external_id is required")
	}
	if entity.Fields == nil {
		entity.Fields = Fields{}
	}
	if err := prepareEntityFieldWrites(&entity); err != nil {
		return Entity{}, fmt.Errorf("entity %q fields: %w", entity.ID, err)
	}
	identity, err := normalizeFields(entity.Identity)
	if err != nil {
		return Entity{}, fmt.Errorf("entity %q identity_keys: %w", entity.ID, err)
	}
	entity.Identity = identity
	if entity.Confidence < 0 || entity.Confidence > 1 {
		return Entity{}, fmt.Errorf("entity %q confidence must be between 0 and 1", entity.ID)
	}
	entity.FieldSources = normalizeFieldSources(entity.FieldSources)
	return entity, nil
}

func normalizeEdge(edge Edge) (Edge, error) {
	edge.ID = strings.TrimSpace(edge.ID)
	edge.Type = strings.TrimSpace(edge.Type)
	edge.From = strings.TrimSpace(edge.From)
	edge.To = strings.TrimSpace(edge.To)
	edge.Source = strings.TrimSpace(edge.Source)
	edge.ExternalID = strings.TrimSpace(edge.ExternalID)
	if edge.Type == "" {
		return Edge{}, fmt.Errorf("edge requires type")
	}
	if edge.From == "" || edge.To == "" {
		return Edge{}, fmt.Errorf("edge %q requires from and to", edge.ID)
	}
	if edge.Fields == nil {
		edge.Fields = Fields{}
	}
	fields, err := normalizeFields(edge.Fields)
	if err != nil {
		return Edge{}, fmt.Errorf("edge %q fields: %w", edge.ID, err)
	}
	edge.Fields = fields
	if edge.Confidence < 0 || edge.Confidence > 1 {
		return Edge{}, fmt.Errorf("edge %q confidence must be between 0 and 1", edge.ID)
	}
	edge.FieldSources = normalizeFieldSources(edge.FieldSources)
	edge.Sources = normalizeEdgeSourceList(edge.Sources, edge.UpdatedAt)
	return edge, nil
}

func normalizeSources(entity Entity) []EntitySource {
	sources := append([]EntitySource(nil), entity.Sources...)
	if entity.Source != "" || entity.ExternalID != "" {
		sources = append(sources, EntitySource{
			Source:     entity.Source,
			ExternalID: entity.ExternalID,
			Confidence: entity.Confidence,
			Priority:   entity.SourceRank,
		})
	}
	out := make([]EntitySource, 0, len(sources))
	seen := map[string]struct{}{}
	for _, source := range sources {
		source.Source = strings.TrimSpace(source.Source)
		source.ExternalID = strings.TrimSpace(source.ExternalID)
		if source.Source == "" || source.ExternalID == "" {
			continue
		}
		if source.Confidence < 0 || source.Confidence > 1 {
			source.Confidence = 0
		}
		key := source.Source + "\x00" + source.ExternalID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, source)
	}
	return out
}

func sanitizeIncomingEntitySources(entity *Entity) {
	if entity.Source == "" || entity.ExternalID == "" {
		if entity.Source == "" && entity.ExternalID == "" && len(entity.Sources) == 1 {
			entity.Source = entity.Sources[0].Source
			entity.ExternalID = entity.Sources[0].ExternalID
			entity.Confidence = entity.Sources[0].Confidence
			entity.SourceRank = entity.Sources[0].Priority
			return
		}
		entity.Sources = nil
		return
	}
	entity.Sources = []EntitySource{{
		Source:     entity.Source,
		ExternalID: entity.ExternalID,
		Confidence: entity.Confidence,
		Priority:   entity.SourceRank,
	}}
}

func sanitizeIncomingEdgeSources(edge *Edge) {
	edge.Sources = nil
}

func normalizeFieldSources(sources map[string]FieldSource) map[string]FieldSource {
	if len(sources) == 0 {
		return nil
	}
	out := make(map[string]FieldSource, len(sources))
	for field, source := range sources {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		source.Source = strings.TrimSpace(source.Source)
		if source.Confidence < 0 || source.Confidence > 1 {
			source.Confidence = 0
		}
		out[field] = source
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
