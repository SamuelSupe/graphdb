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
	keys    []string
	encoded map[string][]byte
}

func buildLogicalHashCache(g *Graph) (*logicalHashCache, error) {
	ciTypes, err := buildLogicalHashCategory(g.CITypes, func(value CIType) any {
		return value
	})
	if err != nil {
		return nil, err
	}
	entities, err := buildLogicalHashCategory(g.Entities, func(value Entity) any {
		return logicalEntityForHash(value)
	})
	if err != nil {
		return nil, err
	}
	relationTypes, err := buildLogicalHashCategory(g.RelationTypes, func(value RelationType) any {
		return value
	})
	if err != nil {
		return nil, err
	}
	edges, err := buildLogicalHashCategory(g.Edges, func(value Edge) any {
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
	logicalValue func(T) any,
) (logicalHashCategory, error) {
	category := logicalHashCategory{
		keys:    make([]string, 0, len(values)),
		encoded: make(map[string][]byte, len(values)),
	}
	for key, value := range values {
		data, err := json.Marshal(logicalValue(value))
		if err != nil {
			return logicalHashCategory{}, err
		}
		category.keys = append(category.keys, key)
		category.encoded[key] = data
	}
	sort.Strings(category.keys)
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

func cloneLogicalHashCategory(source logicalHashCategory) logicalHashCategory {
	return logicalHashCategory{
		keys:    append([]string(nil), source.keys...),
		encoded: shallowCopyMap(source.encoded),
	}
}

func (g *Graph) refreshLogicalHashCache(tracker *mutationFingerprintTracker) error {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		return nil
	}
	cache := g.logicalHashCache
	for key := range tracker.ciTypes {
		value, exists := g.CITypes[key]
		if err := updateLogicalHashCategory(&cache.ciTypes, key, value, exists); err != nil {
			return err
		}
	}
	for key := range tracker.entities {
		value, exists := g.Entities[key]
		if err := updateLogicalHashCategory(
			&cache.entities, key, logicalEntityForHash(value), exists,
		); err != nil {
			return err
		}
	}
	for key := range tracker.relationTypes {
		value, exists := g.RelationTypes[key]
		if err := updateLogicalHashCategory(&cache.relationTypes, key, value, exists); err != nil {
			return err
		}
	}
	for key := range tracker.edges {
		value, exists := g.Edges[key]
		if err := updateLogicalHashCategory(
			&cache.edges, key, logicalEdgeForHash(value), exists,
		); err != nil {
			return err
		}
	}
	cache.finalReady = false
	return nil
}

func updateLogicalHashCategory(
	category *logicalHashCategory,
	key string,
	value any,
	exists bool,
) error {
	_, previouslyExisted := category.encoded[key]
	if !exists {
		delete(category.encoded, key)
		if previouslyExisted {
			category.keys = removeSortedKey(category.keys, key)
		}
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	category.encoded[key] = data
	if !previouslyExisted {
		category.keys = insertSortedKey(category.keys, key)
	}
	return nil
}

func insertSortedKey(keys []string, key string) []string {
	index := sort.SearchStrings(keys, key)
	keys = append(keys, "")
	copy(keys[index+1:], keys[index:])
	keys[index] = key
	return keys
}

func removeSortedKey(keys []string, key string) []string {
	index := sort.SearchStrings(keys, key)
	if index == len(keys) || keys[index] != key {
		return keys
	}
	copy(keys[index:], keys[index+1:])
	return keys[:len(keys)-1]
}
