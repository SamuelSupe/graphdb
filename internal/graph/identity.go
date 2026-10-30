package graph

import (
	"fmt"
	"sort"
	"strconv"
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
	ownerID := ""
	var ownerSignature identitySignature
	for _, signature := range signatures {
		id := g.identityIndex[entity.Kind][signature.Value]
		if id == "" {
			continue
		}
		if ownerID != "" && ownerID != id {
			return "", identitySignature{}, fmt.Errorf(
				"entity kind %q identities %q and %q resolve to conflicting owners %q and %q",
				entity.Kind,
				ownerSignature.Value,
				signature.Value,
				ownerID,
				id,
			)
		}
		if ownerID == "" || signature.Strategy == "reject" {
			ownerID = id
			ownerSignature = signature
		}
	}
	return ownerID, ownerSignature, nil
}

type identitySignature struct {
	Value    string
	Strategy string
}

func (g *Graph) identitySignatures(entity Entity) []identitySignature {
	signatures := make([]identitySignature, 0)
	signatures = append(signatures, g.ciIdentitySignatures(entity, entityConfidence(entity))...)
	signatures = append(signatures, g.sourceIdentitySignatures(entity.Sources)...)
	sortIdentitySignatures(signatures)
	return signatures
}

func (g *Graph) lookupIdentitySignatures(entity Entity) []identitySignature {
	signatures := make([]identitySignature, 0)
	signatures = append(signatures, g.ciIdentitySignatures(entity, entity.Confidence)...)
	if entity.Source != "" && entity.ExternalID != "" {
		signatures = append(signatures, g.sourceIdentitySignature(EntitySource{
			Source:     entity.Source,
			ExternalID: entity.ExternalID,
		}))
	}
	signatures = append(signatures, g.sourceIdentitySignatures(entity.Sources)...)
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
			if value, ok := ciIdentitySignature(entity, key, g.legacyIdentity); ok {
				signatures = append(signatures, identitySignature{Value: value, Strategy: key.Strategy})
			}
		}
	}
	return signatures
}

// CIIdentitySignature uses the same tuple encoding as live entity matching.
func CIIdentitySignature(entity Entity, key IdentityKey) (string, bool) {
	return ciIdentitySignature(entity, key, false)
}

func ciIdentitySignature(entity Entity, key IdentityKey, legacy bool) (string, bool) {
	parts := []string{"rule", key.Name}
	var old []string
	for _, field := range key.Fields {
		value, ok := entity.Identity[field]
		if !ok {
			value, ok = entity.Fields[field]
		}
		scalar, scalarOK := ScalarIndexKey(value)
		if !ok || !scalarOK {
			return "", false
		}
		if legacy {
			old = append(old, field+"="+scalar)
		} else {
			parts = append(parts, field, scalar)
		}
	}
	if len(key.Fields) == 0 {
		return "", false
	}
	if legacy {
		return "rule:" + key.Name + "|" + strings.Join(old, "|"), true
	}
	return identityTuple(parts...), true
}

func identityTuple(parts ...string) string {
	var key strings.Builder
	for _, part := range parts {
		key.WriteString(strconv.Itoa(len(part)))
		key.WriteByte(':')
		key.WriteString(part)
	}
	return key.String()
}

func sourceIdentitySignatures(sources []EntitySource) []identitySignature {
	return (&Graph{}).sourceIdentitySignatures(sources)
}

func (g *Graph) sourceIdentitySignatures(sources []EntitySource) []identitySignature {
	signatures := make([]identitySignature, 0, len(sources))
	for _, source := range sources {
		signatures = append(signatures, g.sourceIdentitySignature(source))
	}
	return signatures
}

func (g *Graph) sourceIdentitySignature(source EntitySource) identitySignature {
	value := identityTuple("source", source.Source, source.ExternalID)
	if g.legacyIdentity {
		value = "source:" + source.Source + "|external_id=" + source.ExternalID
	}
	return identitySignature{Value: value, Strategy: "merge"}
}

func legacyCommitIdentity(commit Commit) bool {
	return commit.LayoutVersion > 0 && commit.IdentityEncoding == 0
}

// WithCommitIdentity returns a graph with the requested matching rules, sharing
// unchanged data. A rule transition rebuilds the index in a private copy without
// applying mutations or updating entity timestamps and versions.
func (g *Graph) WithCommitIdentity(commit Commit) (*Graph, error) {
	if commit.IdentityEncoding >= 0 && commit.IdentityEncoding <= IdentityEncodingTuple && g.legacyIdentity == legacyCommitIdentity(commit) {
		return g, nil
	}
	clone := g.cloneForStorageImpact(storageMutationImpact{})
	if err := clone.useCommitIdentity(commit); err != nil {
		return nil, err
	}
	return clone, nil
}

// Only the identity index changes when old persisted commits are replayed.
// Published source graphs remain untouched when a mutation copy changes mode.
func (g *Graph) useCommitIdentity(commit Commit) error {
	if commit.IdentityEncoding < 0 || commit.IdentityEncoding > IdentityEncodingTuple {
		return fmt.Errorf("unsupported commit identity encoding %d", commit.IdentityEncoding)
	}
	legacy := legacyCommitIdentity(commit)
	if g.legacyIdentity == legacy {
		return nil
	}
	g.legacyIdentity = legacy
	g.identityIndex = make(map[string]map[string]string)
	for id, entity := range g.Entities.All() {
		if g.identityIndex[entity.Kind] == nil {
			g.identityIndex[entity.Kind] = make(map[string]string)
		}
		for _, signature := range g.identitySignatures(entity) {
			g.identityIndex[entity.Kind][signature.Value] = id
		}
	}
	return nil
}

func sortIdentitySignatures(signatures []identitySignature) {
	sort.Slice(signatures, func(i, j int) bool {
		return signatures[i].Value < signatures[j].Value
	})
}

func mergeEntityWithSpecs(existing, incoming Entity, fieldSpecs map[string]FieldSpec) Entity {
	merged := copyEntity(existing)
	backfillFieldSources(&merged)
	incoming = copyEntity(incoming)
	backfillFieldSources(&incoming)
	for key, value := range incoming.Fields {
		if shouldAppendUnique(fieldSpecs[key], incoming, key) {
			if current, ok := merged.Fields[key]; ok {
				mergedValue, changed, ok := appendUniqueArrayField(current, value)
				if ok {
					if changed {
						merged.Fields[key] = mergedValue
						setFieldSource(&merged, key, betterFieldSource(fieldSourceOrEntityOwner(merged, key), fieldSourceOrEntityOwner(incoming, key)))
					}
					continue
				}
			}
		}
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
	clearEntityWriteMetadata(&merged)
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
