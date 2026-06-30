package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

func CanonicalEdgeID(edge Edge) string {
	return CanonicalEdgeIDParts(edge.Type, edge.From, edge.To)
}

func CanonicalEdgeIDParts(relationType string, from string, to string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(relationType) + "\x00" + strings.TrimSpace(from) + "\x00" + strings.TrimSpace(to)))
	return "edge:" + hex.EncodeToString(sum[:])[:32]
}

func canonicalizeEdge(edge Edge, version int64, updatedAt time.Time) Edge {
	incomingID := edge.ID
	canonicalID := CanonicalEdgeID(edge)
	edge.ID = canonicalID
	if incomingID == canonicalID {
		incomingID = ""
	}
	edge.Sources = normalizeEdgeSources(edge, incomingID, updatedAt)
	backfillEdgeSources(&edge, version, updatedAt)
	return edge
}

func normalizeEdgeSources(edge Edge, incomingID string, observedAt time.Time) []EdgeSource {
	sources := append([]EdgeSource(nil), edge.Sources...)
	if edge.Source != "" || edge.ExternalID != "" || incomingID != "" {
		if incomingID != "" || !edgeSourcesContainPrimary(sources, edge.Source, edge.ExternalID) {
			sources = append(sources, EdgeSource{
				Source:     edge.Source,
				ExternalID: edge.ExternalID,
				EdgeID:     incomingID,
				Confidence: edge.Confidence,
				Priority:   edge.SourceRank,
				ObservedAt: observedAt,
			})
		}
	}
	return normalizeEdgeSourceList(sources, observedAt)
}

func edgeSourcesContainPrimary(sources []EdgeSource, source string, externalID string) bool {
	source = strings.TrimSpace(source)
	externalID = strings.TrimSpace(externalID)
	if source == "" && externalID == "" {
		return false
	}
	for _, item := range sources {
		if strings.TrimSpace(item.Source) == source && strings.TrimSpace(item.ExternalID) == externalID {
			return true
		}
	}
	return false
}

func normalizeEdgeSourceList(sources []EdgeSource, observedAt time.Time) []EdgeSource {
	out := make([]EdgeSource, 0, len(sources))
	seen := map[string]struct{}{}
	for _, source := range sources {
		source.Source = strings.TrimSpace(source.Source)
		source.ExternalID = strings.TrimSpace(source.ExternalID)
		source.EdgeID = strings.TrimSpace(source.EdgeID)
		if source.Source == "" && source.ExternalID == "" && source.EdgeID == "" {
			continue
		}
		if source.Confidence < 0 || source.Confidence > 1 {
			source.Confidence = 0
		}
		if source.ObservedAt.IsZero() {
			source.ObservedAt = observedAt
		}
		key := source.Source + "\x00" + source.ExternalID + "\x00" + source.EdgeID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, source)
	}
	return out
}

func mergeEdgeSources(left, right []EdgeSource) []EdgeSource {
	out := append([]EdgeSource(nil), left...)
	seen := map[string]int{}
	for i, source := range out {
		seen[source.Source+"\x00"+source.ExternalID+"\x00"+source.EdgeID] = i
	}
	for _, source := range right {
		key := source.Source + "\x00" + source.ExternalID + "\x00" + source.EdgeID
		if idx, ok := seen[key]; ok {
			if sourceRankBetter(source.Priority, source.Confidence, out[idx].Priority, out[idx].Confidence) {
				out[idx] = source
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, source)
	}
	return out
}

func backfillEdgeSources(edge *Edge, version int64, updatedAt time.Time) {
	owner := ownerFromEdge(*edge, version, updatedAt)
	if edge.ExistenceSource == nil {
		edge.ExistenceSource = &owner
	}
	if edge.FieldSources == nil {
		edge.FieldSources = map[string]FieldSource{}
	}
	for field := range edge.Fields {
		if _, ok := edge.FieldSources[field]; !ok {
			edge.FieldSources[field] = owner
		}
	}
}

func stampEdgeSources(edge *Edge, version int64, updatedAt time.Time) {
	owner := writeOwnerFromEdge(*edge, version, updatedAt)
	edge.ExistenceSource = &owner
	if len(edge.Fields) == 0 {
		edge.FieldSources = nil
		return
	}
	edge.FieldSources = map[string]FieldSource{}
	for field := range edge.Fields {
		edge.FieldSources[field] = owner
	}
}

func ownerFromEdge(edge Edge, version int64, updatedAt time.Time) FieldSource {
	best := FieldSource{
		Source:     edge.Source,
		Priority:   edge.SourceRank,
		Confidence: edge.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
	for _, source := range edge.Sources {
		if source.Priority > best.Priority || (source.Priority == best.Priority && source.Confidence > best.Confidence) {
			best.Source = source.Source
			best.Priority = source.Priority
			best.Confidence = source.Confidence
		}
	}
	return best
}

func writeOwnerFromEdge(edge Edge, version int64, updatedAt time.Time) FieldSource {
	return FieldSource{
		Source:     edge.Source,
		Priority:   edge.SourceRank,
		Confidence: edge.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
}

func edgeFieldSourceOrOwner(edge Edge, field string) FieldSource {
	if source, ok := edge.FieldSources[field]; ok {
		return source
	}
	if edge.ExistenceSource != nil {
		return *edge.ExistenceSource
	}
	return ownerFromEdge(edge, edge.Version, edge.UpdatedAt)
}

func setEdgeFieldSource(edge *Edge, field string, source FieldSource) {
	if edge.FieldSources == nil {
		edge.FieldSources = map[string]FieldSource{}
	}
	edge.FieldSources[field] = source
}

func edgeSourceAliasMatches(edge Edge, id string) bool {
	return EdgeSourceAliasMatches(edge, id)
}

func EdgeSourceAliasMatches(edge Edge, id string) bool {
	if id == "" {
		return false
	}
	if edge.ID == id || edge.ExternalID == id {
		return true
	}
	for _, source := range edge.Sources {
		if source.EdgeID == id || source.ExternalID == id {
			return true
		}
	}
	return false
}
