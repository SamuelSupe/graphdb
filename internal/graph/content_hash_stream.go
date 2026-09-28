package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

const ContentHashAlgorithm = "sha256-shards-v2"

// ContentHash identifies logical graph content, excluding version and timestamps.
// The algorithm prefix is part of the public digest contract.
func (g *Graph) ContentHash() (string, error) {
	value, _, err := g.ContentHashWithLogicalSize()
	return value, err
}

// ContentHashWithLogicalSize returns the canonical logical-item byte count for
// memory admission. Unchanged shard digests are reused across graph versions.
func (g *Graph) ContentHashWithLogicalSize() (string, int64, error) {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		cache, err := buildLogicalHashCache(g)
		if err != nil {
			return "", 0, err
		}
		g.logicalHashCache = cache
	}
	cache := g.logicalHashCache
	if !cache.finalReady {
		digest := sha256.New()
		_, _ = digest.Write([]byte(ContentHashAlgorithm + "\x00"))
		var empty [sha256.Size]byte
		for _, category := range cache.categories {
			for _, block := range category {
				if block == nil {
					_, _ = digest.Write(empty[:])
				} else {
					_, _ = digest.Write(block.digest[:])
				}
			}
		}
		cache.digest = ContentHashAlgorithm + ":" + hex.EncodeToString(digest.Sum(nil))
		cache.finalReady = true
	}
	return cache.digest, cache.logicalBytes, nil
}

func logicalEntityForHash(entity Entity) logicalEntity {
	mergedFrom := append([]string(nil), entity.MergedFrom...)
	sort.Strings(mergedFrom)
	return logicalEntity{
		ID:              entity.ID,
		Kind:            entity.Kind,
		Fields:          entity.Fields,
		FieldSources:    logicalFieldSources(entity.FieldSources),
		ExistenceSource: logicalOwnerPtr(entity.ExistenceSource),
		Source:          entity.Source,
		ExternalID:      entity.ExternalID,
		Identity:        entity.Identity,
		Confidence:      entity.Confidence,
		SourceRank:      entity.SourceRank,
		Sources:         logicalEntitySources(entity.Sources),
		MergedFrom:      mergedFrom,
		SplitFrom:       entity.SplitFrom,
	}
}

func logicalEdgeForHash(edge Edge) logicalEdge {
	return logicalEdge{
		ID:              edge.ID,
		Type:            edge.Type,
		From:            edge.From,
		To:              edge.To,
		Fields:          edge.Fields,
		FieldSources:    logicalFieldSources(edge.FieldSources),
		Source:          edge.Source,
		ExternalID:      edge.ExternalID,
		Confidence:      edge.Confidence,
		SourceRank:      edge.SourceRank,
		Sources:         logicalEdgeSources(edge.Sources),
		ExistenceSource: logicalOwnerPtr(edge.ExistenceSource),
	}
}
