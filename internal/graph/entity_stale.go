package graph

import (
	"fmt"
	"strings"
	"time"
)

func (g *Graph) applySourceStale(request SourceStaleRequest, version int64, now time.Time) (ApplyReport, error) {
	request.Source = strings.TrimSpace(request.Source)
	request.Kind = strings.TrimSpace(request.Kind)
	request.Action = strings.TrimSpace(request.Action)
	if request.Source == "" {
		return ApplyReport{}, fmt.Errorf("mark source stale requires source")
	}
	if request.Action == "" {
		request.Action = "mark_stale"
	}
	observed := observedExternalIDSet(request.ObservedExternalIDs)
	report := ApplyReport{}
	affected := newUniqueStringCollector(&report.AffectedEntityIDs)
	ids := sortedEntityIDs(g.Entities)
	for _, entityID := range ids {
		entity := copyEntity(g.Entities[entityID])
		if request.Kind != "" && entity.Kind != request.Kind {
			continue
		}
		if !entitySourceMissing(entity, request.Source, observed) {
			continue
		}
		switch request.Action {
		case "mark_stale":
			markEntitySourceStale(&entity, request.Source, version, now)
			entity.Version = version
			entity.UpdatedAt = now
			g.removeEntityFromIndexes(entityID, g.Entities[entityID])
			g.Entities[entityID] = entity
			g.addEntityToIndexes(entityID, entity)
			affected.add(entityID)
		case "delete":
			backfillFieldSources(&entity)
			existingOwner := *entity.ExistenceSource
			incomingOwner := sourceForStaleDelete(request, version, now)
			if sourceCanDeleteEntity(existingOwner, incomingOwner) {
				g.deleteEntityForce(entityID)
				affected.add(entityID)
				continue
			}
			report.Suppressed = append(report.Suppressed, staleDeleteConflict(entityID, request, existingOwner, incomingOwner))
		default:
			return ApplyReport{}, fmt.Errorf("unsupported source stale action %q", request.Action)
		}
	}
	return report, nil
}

func observedExternalIDSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func entitySourceMissing(entity Entity, source string, observed map[string]struct{}) bool {
	for _, item := range entity.Sources {
		if item.Source != source || item.ExternalID == "" {
			continue
		}
		if _, ok := observed[item.ExternalID]; ok {
			return false
		}
		return true
	}
	return false
}

func markEntitySourceStale(entity *Entity, source string, version int64, staleAt time.Time) {
	for i := range entity.Sources {
		if entity.Sources[i].Source != source {
			continue
		}
		entity.Sources[i].Stale = true
		entity.Sources[i].StaleAt = staleAt
	}
}

func sourceForStaleDelete(request SourceStaleRequest, version int64, updatedAt time.Time) FieldSource {
	return FieldSource{
		Source:     request.Source,
		Priority:   request.SourceRank,
		Confidence: request.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
}

func staleDeleteConflict(entityID string, request SourceStaleRequest, existing FieldSource, incoming FieldSource) FieldConflict {
	return FieldConflict{
		ResourceType:     "entity",
		EntityID:         entityID,
		Field:            "__existence__",
		ExistingSource:   existing.Source,
		ExistingPriority: existing.Priority,
		IncomingSource:   incoming.Source,
		IncomingPriority: incoming.Priority,
		ExistingValue:    true,
		IncomingValue:    false,
		Message:          "stale entity delete was ignored because source priority is lower",
	}
}
