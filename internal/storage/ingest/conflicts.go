package ingest

import (
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func IngestConflicts(request IngestRequest, suppressed []graph.FieldConflict) []IngestConflict {
	out := make([]IngestConflict, 0, len(suppressed))
	var itemIndex *ConflictItemIndex
	if len(suppressed) > 1 {
		index := NewIngestConflictItemIndex(request.Items)
		itemIndex = &index
	}
	for _, conflict := range suppressed {
		index, externalID, _ := FindIngestConflictItem(request.Items, itemIndex, conflict)
		out = append(out, IngestConflict{
			ResourceType:     conflict.ResourceType,
			Index:            index,
			ExternalID:       externalID,
			EntityID:         conflict.EntityID,
			EdgeID:           conflict.EdgeID,
			CanonicalID:      conflict.CanonicalID,
			IncomingID:       conflict.IncomingID,
			Field:            conflict.Field,
			AliasField:       conflict.AliasField,
			ExistingSource:   conflict.ExistingSource,
			ExistingPriority: conflict.ExistingPriority,
			IncomingSource:   conflict.IncomingSource,
			IncomingPriority: conflict.IncomingPriority,
			ExistingValue:    conflict.ExistingValue,
			IncomingValue:    conflict.IncomingValue,
			Message:          conflict.Message,
		})
	}
	return out
}

// ConflictItemIndex keeps the earliest input item for every identifier
// that a conflict can use. A batch can produce one suppression per field, so
// scanning the entire batch for each conflict makes the response path O(n*m).
type ConflictItemIndex struct {
	items []IngestItem

	entityIncoming map[string]int
	entityIDs      map[string]int

	edgeUpsertIncoming  map[string]int
	edgeUpsertCanonical map[string]int
	edgeDeleteIncoming  map[string]int
	edgeDeleteCanonical map[string]int
}

func NewIngestConflictItemIndex(items []IngestItem) ConflictItemIndex {
	index := ConflictItemIndex{
		items: items,
	}
	for itemIndex, item := range items {
		index.addEntityItem(itemIndex, item)
		index.addEdgeItem(itemIndex, item)
	}
	return index
}

func (index *ConflictItemIndex) addEntityItem(itemIndex int, item IngestItem) {
	if item.Entity != nil {
		index.addEntityIdentifiers(itemIndex, item.ExternalID, item.Entity.ID, item.Entity.ExternalID)
		return
	}
	if item.DeleteEntity != nil {
		index.addEntityIdentifiers(itemIndex, item.ExternalID, item.DeleteEntity.ID, item.DeleteEntity.ExternalID)
	}
}

func (index *ConflictItemIndex) addEntityIdentifiers(itemIndex int, itemExternalID string, entityID string, entityExternalID string) {
	index.add(&index.entityIncoming, entityExternalID, itemIndex)
	index.add(&index.entityIncoming, itemExternalID, itemIndex)
	index.add(&index.entityIncoming, entityID, itemIndex)
	index.add(&index.entityIDs, entityID, itemIndex)
}

func (index *ConflictItemIndex) addEdgeItem(itemIndex int, item IngestItem) {
	if item.Edge != nil {
		index.add(&index.edgeUpsertIncoming, firstTrimmed(item.Edge.ID, item.Edge.ExternalID, item.ExternalID), itemIndex)
		if canonicalID, ok := canonicalEdgeIDFromParts(item.Edge.Type, item.Edge.From, item.Edge.To); ok {
			index.add(&index.edgeUpsertCanonical, canonicalID, itemIndex)
		}
	}
	if item.DeleteEdge != nil {
		index.add(&index.edgeDeleteIncoming, firstTrimmed(item.DeleteEdge.ID, item.ExternalID), itemIndex)
		if canonicalID, ok := canonicalEdgeIDFromParts(item.DeleteEdge.Type, item.DeleteEdge.From, item.DeleteEdge.To); ok {
			index.add(&index.edgeDeleteCanonical, canonicalID, itemIndex)
		}
	}
}

func (index *ConflictItemIndex) add(items *map[string]int, identifier string, itemIndex int) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return
	}
	if *items == nil {
		*items = make(map[string]int, len(index.items))
	}
	if _, exists := (*items)[identifier]; !exists {
		(*items)[identifier] = itemIndex
	}
}

func FindIngestConflictItem(items []IngestItem, itemIndex *ConflictItemIndex, conflict graph.FieldConflict) (int, string, bool) {
	if itemIndex != nil {
		if index, ok := itemIndex.find(conflict); ok {
			return index, items[index].ExternalID, true
		}
	}
	return FindIngestConflictItemLinear(items, conflict)
}

func (index *ConflictItemIndex) find(conflict graph.FieldConflict) (int, bool) {
	candidate := -1
	consider := func(items map[string]int, identifier string) {
		if itemIndex, ok := items[identifier]; ok && (candidate < 0 || itemIndex < candidate) {
			candidate = itemIndex
		}
	}
	if conflict.ResourceType == "edge" {
		consider(index.edgeUpsertIncoming, conflict.IncomingID)
		consider(index.edgeDeleteIncoming, conflict.IncomingID)
		consider(index.edgeDeleteCanonical, conflict.IncomingID)
		consider(index.edgeUpsertCanonical, conflict.CanonicalID)
		consider(index.edgeDeleteCanonical, conflict.CanonicalID)
		consider(index.edgeUpsertCanonical, conflict.EdgeID)
		consider(index.edgeDeleteCanonical, conflict.EdgeID)
	} else if conflict.IncomingID != "" {
		consider(index.entityIncoming, conflict.IncomingID)
	} else {
		consider(index.entityIDs, conflict.EntityID)
	}
	if candidate < 0 || !ingestConflictMatchesItem(conflict, index.items[candidate]) {
		return 0, false
	}
	return candidate, true
}

func FindIngestConflictItemLinear(items []IngestItem, conflict graph.FieldConflict) (int, string, bool) {
	for index, item := range items {
		if ingestConflictMatchesItem(conflict, item) {
			return index, item.ExternalID, true
		}
	}
	return 0, "", false
}

func ingestConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	if conflict.ResourceType == "edge" {
		return edgeConflictMatchesItem(conflict, item)
	}
	return entityConflictMatchesItem(conflict, item)
}

func entityConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	if item.Entity != nil {
		entityID := strings.TrimSpace(item.Entity.ID)
		entityExternalID := strings.TrimSpace(item.Entity.ExternalID)
		itemExternalID := strings.TrimSpace(item.ExternalID)
		if conflict.IncomingID != "" {
			return conflict.IncomingID == entityExternalID ||
				conflict.IncomingID == itemExternalID ||
				conflict.IncomingID == entityID
		}
		return conflict.EntityID != "" && conflict.EntityID == entityID
	}
	if item.DeleteEntity == nil {
		return false
	}
	entityID := strings.TrimSpace(item.DeleteEntity.ID)
	entityExternalID := strings.TrimSpace(item.DeleteEntity.ExternalID)
	itemExternalID := strings.TrimSpace(item.ExternalID)
	if conflict.IncomingID != "" {
		return conflict.IncomingID == entityExternalID ||
			conflict.IncomingID == itemExternalID ||
			conflict.IncomingID == entityID
	}
	return conflict.EntityID != "" && conflict.EntityID == entityID
}

func edgeConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	if item.Edge != nil && edgeUpsertConflictMatchesItem(conflict, item) {
		return true
	}
	if item.DeleteEdge != nil && edgeDeleteConflictMatchesItem(conflict, item) {
		return true
	}
	return false
}

func edgeUpsertConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	edge := item.Edge
	incomingID := firstTrimmed(edge.ID, edge.ExternalID, item.ExternalID)
	if conflict.IncomingID != "" && conflict.IncomingID == incomingID {
		return true
	}
	canonicalID, ok := canonicalEdgeIDFromParts(edge.Type, edge.From, edge.To)
	if !ok {
		return false
	}
	return conflict.CanonicalID == canonicalID || conflict.EdgeID == canonicalID
}

func edgeDeleteConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	request := item.DeleteEdge
	incomingID := firstTrimmed(request.ID, item.ExternalID)
	if conflict.IncomingID != "" && conflict.IncomingID == incomingID {
		return true
	}
	canonicalID, ok := canonicalEdgeIDFromParts(request.Type, request.From, request.To)
	if !ok {
		return false
	}
	return conflict.IncomingID == canonicalID || conflict.CanonicalID == canonicalID || conflict.EdgeID == canonicalID
}

func canonicalEdgeIDFromParts(edgeType string, from string, to string) (string, bool) {
	edgeType = strings.TrimSpace(edgeType)
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if edgeType == "" || from == "" || to == "" {
		return "", false
	}
	return graph.CanonicalEdgeIDParts(edgeType, from, to), true
}

func firstTrimmed(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
