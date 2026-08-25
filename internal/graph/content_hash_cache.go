package graph

import (
	"encoding/json"
	"sort"
)

type logicalHashCache struct {
	ciTypes       logicalHashCategory
	entities      logicalHashCategory
	relationTypes logicalHashCategory
	edges         logicalHashCategory
	digest        string
	logicalBytes  int64
	finalReady    bool
}

type logicalHashCategory struct {
	keys         []string
	encoded      [][]byte
	fingerprints [][16]byte
}

func buildLogicalHashCache(g *Graph) (*logicalHashCache, error) {
	ciTypes, err := buildLogicalHashCategory(g.CITypes, "ci_type", func(value CIType) any {
		return value
	})
	if err != nil {
		return nil, err
	}
	entities, err := buildLogicalHashCategory(g.Entities, "entity", func(value Entity) any {
		return logicalEntityForHash(value)
	})
	if err != nil {
		return nil, err
	}
	relationTypes, err := buildLogicalHashCategory(g.RelationTypes, "relation_type", func(value RelationType) any {
		return value
	})
	if err != nil {
		return nil, err
	}
	edges, err := buildLogicalHashCategory(g.Edges, "edge", func(value Edge) any {
		return logicalEdgeForHash(value)
	})
	if err != nil {
		return nil, err
	}
	return &logicalHashCache{
		ciTypes: ciTypes, entities: entities,
		relationTypes: relationTypes, edges: edges,
	}, nil
}

func buildLogicalHashCategory[T any](
	values map[string]T,
	kind string,
	logicalValue func(T) any,
) (logicalHashCategory, error) {
	type logicalItem struct {
		key         string
		encoded     []byte
		fingerprint [16]byte
	}
	items := make([]logicalItem, 0, len(values))
	for key, value := range values {
		data, err := json.Marshal(logicalValue(value))
		if err != nil {
			return logicalHashCategory{}, err
		}
		items = append(items, logicalItem{
			key: key, encoded: data,
			fingerprint: contentFingerprintEntryJSON(kind, key, data),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].key < items[j].key
	})
	category := logicalHashCategory{
		keys:         make([]string, len(items)),
		encoded:      make([][]byte, len(items)),
		fingerprints: make([][16]byte, len(items)),
	}
	for i, item := range items {
		category.keys[i] = item.key
		category.encoded[i] = item.encoded
		category.fingerprints[i] = item.fingerprint
	}
	return category, nil
}

func (g *Graph) cloneLogicalHashCache() *logicalHashCache {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		return nil
	}
	source := g.logicalHashCache
	return &logicalHashCache{
		ciTypes:       cloneLogicalHashCategory(source.ciTypes),
		entities:      cloneLogicalHashCategory(source.entities),
		relationTypes: cloneLogicalHashCategory(source.relationTypes),
		edges:         cloneLogicalHashCategory(source.edges),
		digest:        source.digest,
		logicalBytes:  source.logicalBytes,
		finalReady:    source.finalReady,
	}
}

func (g *Graph) shareLogicalHashCache() *logicalHashCache {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		return nil
	}
	shared := *g.logicalHashCache
	return &shared
}

func (g *Graph) logicalHashCacheView() *logicalHashCache {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	return g.logicalHashCache
}

func cloneLogicalHashCategory(source logicalHashCategory) logicalHashCategory {
	return logicalHashCategory{
		keys:         append([]string(nil), source.keys...),
		encoded:      append([][]byte(nil), source.encoded...),
		fingerprints: append([][16]byte(nil), source.fingerprints...),
	}
}

func (g *Graph) refreshLogicalHashCache(tracker *mutationFingerprintTracker) error {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		return nil
	}
	cache := g.logicalHashCache
	if err := updateLogicalHashCategoryBatch(
		&cache.ciTypes,
		"ci_type",
		tracker.ciTypes,
		func(key string) (any, bool) {
			value, exists := g.CITypes[key]
			return value, exists
		},
	); err != nil {
		return err
	}
	if err := updateLogicalHashCategoryBatch(
		&cache.entities,
		"entity",
		tracker.entities,
		func(key string) (any, bool) {
			value, exists := g.Entities[key]
			if !exists {
				return nil, false
			}
			return logicalEntityForHash(value), true
		},
	); err != nil {
		return err
	}
	if err := updateLogicalHashCategoryBatch(
		&cache.relationTypes,
		"relation_type",
		tracker.relationTypes,
		func(key string) (any, bool) {
			value, exists := g.RelationTypes[key]
			return value, exists
		},
	); err != nil {
		return err
	}
	if err := updateLogicalHashCategoryBatch(
		&cache.edges,
		"edge",
		tracker.edges,
		func(key string) (any, bool) {
			value, exists := g.Edges[key]
			if !exists {
				return nil, false
			}
			return logicalEdgeForHash(value), true
		},
	); err != nil {
		return err
	}
	cache.finalReady = false
	return nil
}
