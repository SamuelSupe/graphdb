package graph

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func (g *Graph) resolveEntityID(entity Entity) (string, error) {
	if entity.ID == "" {
		if id, _, err := g.findEntityByIdentity(entity); err != nil {
			return "", err
		} else if id != "" {
			return id, nil
		}
		if id := canonicalEntityID(entity); id != "" {
			return id, nil
		}
		return "", fmt.Errorf("entity id is required when no identity match exists")
	}
	if id, signature, err := g.findEntityByIdentity(entity); err != nil {
		return "", err
	} else if id != "" && id != entity.ID {
		if signature.Strategy == "reject" {
			return "", fmt.Errorf("entity %q duplicates identity %q owned by %q", entity.ID, signature.Value, id)
		}
		return id, nil
	}
	return entity.ID, nil
}

func (g *Graph) findEntityByIdentity(entity Entity) (string, identitySignature, error) {
	signatures := g.lookupIdentitySignatures(entity)
	for _, signature := range signatures {
		if id := g.identityIndex[entity.Kind][signature.Value]; id != "" {
			return id, signature, nil
		}
	}
	return "", identitySignature{}, nil
}

type identitySignature struct {
	Value    string
	Strategy string
}

func (g *Graph) identitySignatures(entity Entity) []identitySignature {
	signatures := make([]identitySignature, 0)
	signatures = append(signatures, g.ciIdentitySignatures(entity, entityConfidence(entity))...)
	signatures = append(signatures, sourceIdentitySignatures(entity.Sources)...)
	sortIdentitySignatures(signatures)
	return signatures
}

func (g *Graph) lookupIdentitySignatures(entity Entity) []identitySignature {
	signatures := make([]identitySignature, 0)
	signatures = append(signatures, g.ciIdentitySignatures(entity, entity.Confidence)...)
	if entity.Source != "" && entity.ExternalID != "" {
		signatures = append(signatures, sourceIdentitySignature(EntitySource{
			Source:     entity.Source,
			ExternalID: entity.ExternalID,
		}))
	}
	signatures = append(signatures, sourceIdentitySignatures(entity.Sources)...)
	sortIdentitySignatures(signatures)
	return signatures
}

func (g *Graph) ciIdentitySignatures(entity Entity, confidence float64) []identitySignature {
	signatures := make([]identitySignature, 0)
	if ciType, ok := g.CITypes[entity.Kind]; ok {
		for _, key := range ciType.IdentityKeys {
			if key.ConfidenceThreshold > 0 && confidence < key.ConfidenceThreshold {
				continue
			}
			parts := make([]string, 0, len(key.Fields))
			for _, field := range key.Fields {
				value, ok := entity.Identity[field]
				if !ok {
					value, ok = entity.Fields[field]
				}
				scalar, scalarOK := scalarKey(value)
				if !ok || !scalarOK {
					parts = nil
					break
				}
				parts = append(parts, field+"="+scalar)
			}
			if len(parts) > 0 {
				signatures = append(signatures, identitySignature{
					Value:    "rule:" + key.Name + "|" + strings.Join(parts, "|"),
					Strategy: key.Strategy,
				})
			}
		}
	}
	return signatures
}

func sourceIdentitySignatures(sources []EntitySource) []identitySignature {
	signatures := make([]identitySignature, 0, len(sources))
	for _, source := range sources {
		signatures = append(signatures, sourceIdentitySignature(source))
	}
	return signatures
}

func sourceIdentitySignature(source EntitySource) identitySignature {
	return identitySignature{
		Value:    "source:" + source.Source + "|external_id=" + source.ExternalID,
		Strategy: "merge",
	}
}

func sortIdentitySignatures(signatures []identitySignature) {
	sort.Slice(signatures, func(i, j int) bool {
		return signatures[i].Value < signatures[j].Value
	})
}

func mergeEntity(existing, incoming Entity) Entity {
	merged := copyEntity(existing)
	backfillFieldSources(&merged)
	incoming = copyEntity(incoming)
	backfillFieldSources(&incoming)
	for key, value := range incoming.Fields {
		if _, ok := merged.Fields[key]; !ok || entityRankCanOverwrite(incoming, existing) {
			merged.Fields[key] = value
			setFieldSource(&merged, key, fieldSourceOrEntityOwner(incoming, key))
		}
	}
	for key, value := range incoming.Identity {
		if _, ok := merged.Identity[key]; !ok || entityRankCanOverwrite(incoming, existing) {
			merged.Identity[key] = value
		}
	}
	merged.Sources = mergeSources(merged.Sources, incoming.Sources)
	merged.Confidence = maxFloat(existing.Confidence, incoming.Confidence)
	merged.SourceRank = maxInt(existing.SourceRank, incoming.SourceRank)
	merged.Source = firstNonEmpty(existing.Source, incoming.Source)
	merged.ExternalID = firstNonEmpty(existing.ExternalID, incoming.ExternalID)
	if incoming.ID != "" && incoming.ID != existing.ID {
		merged.MergedFrom = appendUnique(merged.MergedFrom, incoming.ID)
	}
	merged.MergedFrom = appendUnique(merged.MergedFrom, incoming.MergedFrom...)
	return merged
}

func entityRankCanOverwrite(incoming Entity, existing Entity) bool {
	incomingPriority, incomingConfidence := bestEntitySourceRank(incoming)
	existingPriority, existingConfidence := bestEntitySourceRank(existing)
	if incomingPriority != existingPriority {
		return incomingPriority > existingPriority
	}
	return incomingConfidence >= existingConfidence
}

func entityConfidence(entity Entity) float64 {
	best := entity.Confidence
	for _, source := range entity.Sources {
		if source.Confidence > best {
			best = source.Confidence
		}
	}
	return best
}

func bestEntitySourceRank(entity Entity) (int, float64) {
	bestPriority := entity.SourceRank
	bestConfidence := entity.Confidence
	for _, source := range entity.Sources {
		if sourceRankBetter(source.Priority, source.Confidence, bestPriority, bestConfidence) {
			bestPriority = source.Priority
			bestConfidence = source.Confidence
		}
	}
	return bestPriority, bestConfidence
}

func mergeSources(left, right []EntitySource) []EntitySource {
	out := append([]EntitySource(nil), left...)
	seen := map[string]int{}
	for i, source := range out {
		seen[source.Source+"\x00"+source.ExternalID] = i
	}
	for _, source := range right {
		key := source.Source + "\x00" + source.ExternalID
		if idx, ok := seen[key]; ok {
			if sourceRankBetter(source.Priority, source.Confidence, out[idx].Priority, out[idx].Confidence) {
				out[idx] = source
			} else if !source.ObservedAt.IsZero() {
				out[idx].ObservedAt = source.ObservedAt
			}
			if !source.Stale {
				out[idx].Stale = false
				out[idx].StaleAt = time.Time{}
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, source)
	}
	return out
}

func sourceRankBetter(incomingPriority int, incomingConfidence float64, existingPriority int, existingConfidence float64) bool {
	if incomingPriority != existingPriority {
		return incomingPriority > existingPriority
	}
	return incomingConfidence > existingConfidence
}

func appendUnique(values []string, more ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values)+len(more))
	for _, value := range append(values, more...) {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstNonEmpty(left, right string) string {
	if left != "" {
		return left
	}
	return right
}

func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
