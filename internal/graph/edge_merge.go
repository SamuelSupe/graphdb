package graph

import (
	"reflect"
	"sort"
	"time"
)

func mergeEdgeForUpsert(existing Edge, incoming Edge, canonicalID string, incomingID string, version int64, updatedAt time.Time) (Edge, ApplyReport) {
	merged := copyEdge(existing)
	backfillEdgeSources(&merged, merged.Version, merged.UpdatedAt)
	incomingOwner := writeOwnerFromEdge(incoming, version, updatedAt)
	report := ApplyReport{}
	for field, value := range incoming.Fields {
		current, exists := merged.Fields[field]
		if !exists || isEmptyFieldValue(current) {
			merged.Fields[field] = copyAny(value)
			setEdgeFieldSource(&merged, field, incomingOwner)
			continue
		}
		existingOwner := edgeFieldSourceOrOwner(merged, field)
		if isEmptyFieldValue(value) {
			report.Suppressed = append(report.Suppressed, edgeConflict(canonicalID, incomingID, field, existingOwner, incomingOwner, current, value, "incoming empty edge field value was ignored because upsert does not clear existing values"))
			continue
		}
		if incomingCanOverwrite(existingOwner, incomingOwner) {
			merged.Fields[field] = copyAny(value)
			setEdgeFieldSource(&merged, field, incomingOwner)
			continue
		}
		if reflect.DeepEqual(current, value) {
			continue
		}
		report.Suppressed = append(report.Suppressed, edgeConflict(canonicalID, incomingID, field, existingOwner, incomingOwner, current, value, "incoming edge field value was ignored because source priority is lower"))
	}
	merged.Sources = mergeEdgeSources(merged.Sources, incoming.Sources)
	merged.Confidence = maxFloat(existing.Confidence, incoming.Confidence)
	merged.SourceRank = maxInt(existing.SourceRank, incoming.SourceRank)
	merged.Source = firstNonEmpty(existing.Source, incoming.Source)
	merged.ExternalID = firstNonEmpty(existing.ExternalID, incoming.ExternalID)
	if merged.ExistenceSource == nil || incomingCanOverwrite(*merged.ExistenceSource, incomingOwner) {
		merged.ExistenceSource = &incomingOwner
	}
	merged.ID = canonicalID
	merged.Type = incoming.Type
	merged.From = incoming.From
	merged.To = incoming.To
	return merged, report
}

func edgeConflict(edgeID string, incomingID string, field string, existing FieldSource, incoming FieldSource, existingValue any, incomingValue any, message string) FieldConflict {
	return FieldConflict{
		ResourceType:     "edge",
		EdgeID:           edgeID,
		CanonicalID:      edgeID,
		IncomingID:       incomingID,
		Field:            field,
		ExistingSource:   existing.Source,
		ExistingPriority: existing.Priority,
		IncomingSource:   incoming.Source,
		IncomingPriority: incoming.Priority,
		ExistingValue:    existingValue,
		IncomingValue:    incomingValue,
		Message:          message,
	}
}

func sourceForDelete(request EdgeDeleteRequest, version int64, updatedAt time.Time) FieldSource {
	return FieldSource{
		Source:     request.Source,
		Priority:   request.SourceRank,
		Confidence: request.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
}

func sourceCanDeleteEdge(existing FieldSource, incoming FieldSource) bool {
	return incoming.Priority >= existing.Priority
}

func existenceConflict(edgeID string, request EdgeDeleteRequest, existing FieldSource, incoming FieldSource) FieldConflict {
	incomingID := request.ID
	if incomingID == "" {
		incomingID = CanonicalEdgeIDParts(request.Type, request.From, request.To)
	}
	return edgeConflict(edgeID, incomingID, "__existence__", existing, incoming, true, false, "incoming edge delete was ignored because source priority is lower")
}

func mergeEdgeSet(edges map[string]Edge, version int64, updatedAt time.Time) (map[string]Edge, ApplyReport) {
	items := make([]Edge, 0, len(edges))
	for _, edge := range edges {
		items = append(items, edge)
	}
	sortEdgesForMerge(items)
	return mergeEdgeList(items, version, updatedAt)
}

func sortEdgesForMerge(edges []Edge) {
	sort.SliceStable(edges, func(i, j int) bool {
		left := edges[i]
		right := edges[j]
		if left.Version != right.Version {
			return left.Version < right.Version
		}
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.Before(right.UpdatedAt)
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		leftKey := left.Type + "\x00" + left.From + "\x00" + left.To
		rightKey := right.Type + "\x00" + right.From + "\x00" + right.To
		return leftKey < rightKey
	})
}

func mergeEdgeList(edges []Edge, version int64, updatedAt time.Time) (map[string]Edge, ApplyReport) {
	next := map[string]Edge{}
	report := ApplyReport{}
	for _, edge := range edges {
		incomingID := firstNonEmpty(edge.ID, edge.ExternalID)
		edge = canonicalizeEdge(edge, firstNonZero(edge.Version, version), firstNonZeroTime(edge.UpdatedAt, updatedAt))
		if existing, ok := next[edge.ID]; ok {
			var mergeReport ApplyReport
			edge, mergeReport = mergeEdgeForUpsert(existing, edge, edge.ID, incomingID, firstNonZero(edge.Version, version), firstNonZeroTime(edge.UpdatedAt, updatedAt))
			report.Suppressed = append(report.Suppressed, mergeReport.Suppressed...)
			if !existing.CreatedAt.IsZero() {
				edge.CreatedAt = existing.CreatedAt
			}
		}
		next[edge.ID] = edge
	}
	return next, report
}

func firstNonZero(left int64, right int64) int64 {
	if left != 0 {
		return left
	}
	return right
}

func firstNonZeroTime(left time.Time, right time.Time) time.Time {
	if !left.IsZero() {
		return left
	}
	return right
}
