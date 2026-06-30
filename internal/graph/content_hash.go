package graph

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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

func (g *Graph) ContentMD5() (string, error) {
	data, err := json.Marshal(g.logicalSnapshot())
	if err != nil {
		return "", err
	}
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:]), nil
}

func (g *Graph) logicalSnapshot() logicalSnapshot {
	snapshot := g.Snapshot()
	out := logicalSnapshot{
		CITypes:       snapshot.CITypes,
		RelationTypes: snapshot.RelationTypes,
		Entities:      make([]logicalEntity, 0, len(snapshot.Entities)),
		Edges:         make([]logicalEdge, 0, len(snapshot.Edges)),
	}
	for _, entity := range snapshot.Entities {
		out.Entities = append(out.Entities, logicalEntityFromEntity(entity))
	}
	for _, edge := range snapshot.Edges {
		out.Edges = append(out.Edges, logicalEdgeFromEdge(edge))
	}
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
