package graph

import (
	"fmt"
	"strings"
	"time"
)

func (g *Graph) resolveEntityDeleteRequest(request EntityDeleteRequest) (string, error) {
	request.ID = strings.TrimSpace(request.ID)
	request.Kind = strings.TrimSpace(request.Kind)
	request.Source = strings.TrimSpace(request.Source)
	request.ExternalID = strings.TrimSpace(request.ExternalID)
	if request.ID != "" {
		return g.ResolveEntityReference(request.ID), nil
	}
	if request.Source != "" || request.ExternalID != "" || request.Kind != "" {
		if request.Kind == "" || request.Source == "" || request.ExternalID == "" {
			return "", fmt.Errorf("delete entity request requires kind, source and external_id when using source identity")
		}
		return g.identityIndex[request.Kind][sourceIdentitySignature(EntitySource{Source: request.Source, ExternalID: request.ExternalID}).Value], nil
	}
	return "", fmt.Errorf("delete entity request requires id or kind/source/external_id")
}

func (g *Graph) deleteEntityForce(entityID string) {
	if entityID == "" {
		return
	}
	if existing, ok := g.Entities[entityID]; ok {
		g.removeEntityFromIndexes(entityID, existing)
	}
	delete(g.Entities, entityID)
	for _, edgeID := range g.incidentEdgeIDs(entityID) {
		edge, ok := g.Edges[edgeID]
		if !ok {
			continue
		}
		g.removeEdgeFromIndexes(edgeID, edge)
		delete(g.Edges, edgeID)
	}
}

func sourceForEntityDelete(request EntityDeleteRequest, version int64, updatedAt time.Time) FieldSource {
	return FieldSource{
		Source:     request.Source,
		Priority:   request.SourceRank,
		Confidence: request.Confidence,
		Version:    version,
		UpdatedAt:  updatedAt,
	}
}

func sourceCanDeleteEntity(existing FieldSource, incoming FieldSource) bool {
	return incoming.Priority >= existing.Priority
}

func entityExistenceConflict(entityID string, request EntityDeleteRequest, existing FieldSource, incoming FieldSource) FieldConflict {
	incomingID := firstNonEmpty(request.ID, request.ExternalID)
	return FieldConflict{
		ResourceType:     "entity",
		EntityID:         entityID,
		IncomingID:       incomingID,
		Field:            "__existence__",
		ExistingSource:   existing.Source,
		ExistingPriority: existing.Priority,
		IncomingSource:   incoming.Source,
		IncomingPriority: incoming.Priority,
		ExistingValue:    true,
		IncomingValue:    false,
		Message:          "incoming entity delete was ignored because source priority is lower",
	}
}
