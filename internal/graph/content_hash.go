package graph

import (
	"sort"
)

type logicalSnapshot struct {
	CITypes       []CIType        `json:"ci_types,omitempty"`
	Entities      []logicalEntity `json:"entities,omitempty"`
	RelationTypes []RelationType  `json:"relation_types,omitempty"`
	Edges         []logicalEdge   `json:"edges,omitempty"`
}

type logicalEntity struct {
	ID              string                  `json:"id"`
	Kind            string                  `json:"kind"`
	Fields          Fields                  `json:"fields,omitempty"`
	FieldSources    map[string]logicalOwner `json:"field_sources,omitempty"`
	ExistenceSource *logicalOwner           `json:"existence_source,omitempty"`
	Source          string                  `json:"source,omitempty"`
	ExternalID      string                  `json:"external_id,omitempty"`
	Identity        map[string]any          `json:"identity_keys,omitempty"`
	Confidence      float64                 `json:"confidence,omitempty"`
	SourceRank      int                     `json:"source_priority,omitempty"`
	Sources         []logicalEntitySource   `json:"sources,omitempty"`
	MergedFrom      []string                `json:"merged_from,omitempty"`
	SplitFrom       string                  `json:"split_from,omitempty"`
}

type logicalEdge struct {
	ID              string                  `json:"id"`
	Type            string                  `json:"type"`
	From            string                  `json:"from"`
	To              string                  `json:"to"`
	Fields          Fields                  `json:"fields,omitempty"`
	FieldSources    map[string]logicalOwner `json:"field_sources,omitempty"`
	Source          string                  `json:"source,omitempty"`
	ExternalID      string                  `json:"external_id,omitempty"`
	Confidence      float64                 `json:"confidence,omitempty"`
	SourceRank      int                     `json:"source_priority,omitempty"`
	Sources         []logicalEdgeSource     `json:"sources,omitempty"`
	ExistenceSource *logicalOwner           `json:"existence_source,omitempty"`
}

type logicalOwner struct {
	Source     string  `json:"source,omitempty"`
	Priority   int     `json:"priority"`
	Confidence float64 `json:"confidence,omitempty"`
}

type logicalEntitySource struct {
	Source     string  `json:"source"`
	ExternalID string  `json:"external_id"`
	Confidence float64 `json:"confidence,omitempty"`
	Priority   int     `json:"priority,omitempty"`
	Stale      bool    `json:"stale,omitempty"`
}

type logicalEdgeSource struct {
	Source     string  `json:"source"`
	ExternalID string  `json:"external_id,omitempty"`
	EdgeID     string  `json:"edge_id,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
	Priority   int     `json:"priority,omitempty"`
}

func (g *Graph) logicalSnapshot() logicalSnapshot {
	out := logicalSnapshot{
		CITypes:       make([]CIType, 0, len(g.CITypes)),
		RelationTypes: make([]RelationType, 0, len(g.RelationTypes)),
		Entities:      make([]logicalEntity, 0, len(g.Entities)),
		Edges:         make([]logicalEdge, 0, len(g.Edges)),
	}
	for _, ciType := range g.CITypes {
		out.CITypes = append(out.CITypes, copyCIType(ciType))
	}
	for _, relationType := range g.RelationTypes {
		out.RelationTypes = append(out.RelationTypes, copyRelationType(relationType))
	}
	for _, entity := range g.Entities {
		out.Entities = append(out.Entities, logicalEntityFromEntity(entity))
	}
	for _, edge := range g.Edges {
		out.Edges = append(out.Edges, logicalEdgeFromEdge(edge))
	}
	sort.Slice(out.CITypes, func(i, j int) bool {
		return out.CITypes[i].Name < out.CITypes[j].Name
	})
	sort.Slice(out.RelationTypes, func(i, j int) bool {
		return out.RelationTypes[i].Name < out.RelationTypes[j].Name
	})
	sort.Slice(out.Entities, func(i, j int) bool {
		return out.Entities[i].ID < out.Entities[j].ID
	})
	sort.Slice(out.Edges, func(i, j int) bool {
		return out.Edges[i].ID < out.Edges[j].ID
	})
	return out
}

func logicalEntityFromEntity(entity Entity) logicalEntity {
	mergedFrom := append([]string(nil), entity.MergedFrom...)
	sort.Strings(mergedFrom)
	return logicalEntity{
		ID:              entity.ID,
		Kind:            entity.Kind,
		Fields:          copyFields(entity.Fields),
		FieldSources:    logicalFieldSources(entity.FieldSources),
		ExistenceSource: logicalOwnerPtr(entity.ExistenceSource),
		Source:          entity.Source,
		ExternalID:      entity.ExternalID,
		Identity:        copyIdentity(entity.Identity),
		Confidence:      entity.Confidence,
		SourceRank:      entity.SourceRank,
		Sources:         logicalEntitySources(entity.Sources),
		MergedFrom:      mergedFrom,
		SplitFrom:       entity.SplitFrom,
	}
}

func logicalEdgeFromEdge(edge Edge) logicalEdge {
	return logicalEdge{
		ID:              edge.ID,
		Type:            edge.Type,
		From:            edge.From,
		To:              edge.To,
		Fields:          copyFields(edge.Fields),
		FieldSources:    logicalFieldSources(edge.FieldSources),
		Source:          edge.Source,
		ExternalID:      edge.ExternalID,
		Confidence:      edge.Confidence,
		SourceRank:      edge.SourceRank,
		Sources:         logicalEdgeSources(edge.Sources),
		ExistenceSource: logicalOwnerPtr(edge.ExistenceSource),
	}
}

func logicalFieldSources(sources map[string]FieldSource) map[string]logicalOwner {
	if sources == nil {
		return nil
	}
	out := make(map[string]logicalOwner, len(sources))
	for field, source := range sources {
		out[field] = logicalOwnerFromFieldSource(source)
	}
	return out
}

func logicalOwnerPtr(source *FieldSource) *logicalOwner {
	if source == nil {
		return nil
	}
	owner := logicalOwnerFromFieldSource(*source)
	return &owner
}

func logicalOwnerFromFieldSource(source FieldSource) logicalOwner {
	return logicalOwner{Source: source.Source, Priority: source.Priority, Confidence: source.Confidence}
}

func logicalEntitySources(sources []EntitySource) []logicalEntitySource {
	out := make([]logicalEntitySource, 0, len(sources))
	for _, source := range sources {
		out = append(out, logicalEntitySource{
			Source:     source.Source,
			ExternalID: source.ExternalID,
			Confidence: source.Confidence,
			Priority:   source.Priority,
			Stale:      source.Stale,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source == out[j].Source {
			return out[i].ExternalID < out[j].ExternalID
		}
		return out[i].Source < out[j].Source
	})
	return out
}

func logicalEdgeSources(sources []EdgeSource) []logicalEdgeSource {
	out := make([]logicalEdgeSource, 0, len(sources))
	for _, source := range sources {
		out = append(out, logicalEdgeSource{
			Source:     source.Source,
			ExternalID: source.ExternalID,
			EdgeID:     source.EdgeID,
			Confidence: source.Confidence,
			Priority:   source.Priority,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].ExternalID != out[j].ExternalID {
			return out[i].ExternalID < out[j].ExternalID
		}
		return out[i].EdgeID < out[j].EdgeID
	})
	return out
}
