package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

func CanonicalEntityIDParts(kind string, source string, externalID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(kind) + "\x00" + strings.TrimSpace(source) + "\x00" + strings.TrimSpace(externalID)))
	return "entity:" + hex.EncodeToString(sum[:])[:32]
}

func canonicalEntityID(entity Entity) string {
	source, externalID, ok := primarySourceIdentity(entity)
	if !ok {
		return ""
	}
	return CanonicalEntityIDParts(entity.Kind, source, externalID)
}

func primarySourceIdentity(entity Entity) (string, string, bool) {
	if entity.Source != "" && entity.ExternalID != "" {
		return entity.Source, entity.ExternalID, true
	}
	for _, source := range entity.Sources {
		if source.Source != "" && source.ExternalID != "" {
			return source.Source, source.ExternalID, true
		}
	}
	return "", "", false
}

func entityCanonicalization(entity Entity, incomingID string, canonicalID string) EntityCanonicalization {
	source, externalID, _ := primarySourceIdentity(entity)
	return EntityCanonicalization{
		CanonicalID: canonicalID,
		IncomingID:  incomingID,
		Kind:        entity.Kind,
		Source:      source,
		ExternalID:  externalID,
	}
}

func (g *Graph) canonicalizeEntitySet(version int64, updatedAt time.Time) map[string]string {
	ids := sortedEntityIDs(g.Entities)
	aliases := map[string]string{}
	owners := map[string]string{}
	for _, id := range ids {
		entity := g.Entities[id]
		targetID := findCanonicalOwner(owners, entity)
		if targetID == "" || targetID == id {
			backfillFieldSources(&entity)
			g.Entities[id] = entity
			registerCanonicalOwners(owners, id, entity)
			continue
		}
		target := g.Entities[targetID]
		fields, _ := g.EffectiveFields(target.Kind)
		merged := mergeEntityWithSpecs(target, entity, fields)
		if !target.CreatedAt.IsZero() {
			merged.CreatedAt = target.CreatedAt
		}
		merged.ID = targetID
		merged.Version = version
		merged.UpdatedAt = updatedAt
		g.Entities[targetID] = merged
		delete(g.Entities, id)
		aliases[id] = targetID
		for _, oldID := range entity.MergedFrom {
			aliases[oldID] = targetID
		}
		registerCanonicalOwners(owners, targetID, merged)
	}
	g.rebuildIndexes()
	return aliases
}

func findCanonicalOwner(owners map[string]string, entity Entity) string {
	for _, signature := range sourceIdentitySignatures(entity.Sources) {
		if signature.Strategy == "reject" {
			continue
		}
		if id := owners[entity.Kind+"\x00"+signature.Value]; id != "" {
			return id
		}
	}
	return ""
}

func registerCanonicalOwners(owners map[string]string, entityID string, entity Entity) {
	for _, signature := range sourceIdentitySignatures(entity.Sources) {
		if signature.Strategy == "reject" {
			continue
		}
		key := entity.Kind + "\x00" + signature.Value
		if owners[key] == "" {
			owners[key] = entityID
		}
	}
}

func sortedEntityIDs(entities map[string]Entity) []string {
	ids := make([]string, 0, len(entities))
	for id := range entities {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
