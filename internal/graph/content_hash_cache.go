package graph

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"maps"
	"sort"
)

type logicalHashEntry struct {
	digest [sha256.Size]byte
	bytes  int64
}

type logicalHashBlock struct {
	entries map[string]logicalHashEntry
	digest  [sha256.Size]byte
}

type logicalHashCache struct {
	categories   [4][graphMapShards]*logicalHashBlock
	digest       string
	logicalBytes int64
	finalReady   bool
}

var logicalHashKinds = [...]string{"ci_type", "entity", "relation_type", "edge"}

func (g *Graph) CachedLogicalSize() int64 {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		return 0
	}
	return g.logicalHashCache.logicalBytes
}

func hashLogicalEntry(category int, key string, value any) (logicalHashEntry, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return logicalHashEntry{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(logicalHashKinds[category] + "\x00"))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(key))
	_, _ = hash.Write(data)
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return logicalHashEntry{digest: digest, bytes: int64(len(data))}, nil
}

func (block *logicalHashBlock) rehash() {
	keys := make([]string, 0, len(block.entries))
	for key := range block.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		entry := block.entries[key]
		_, _ = hash.Write(entry.digest[:])
	}
	copy(block.digest[:], hash.Sum(nil))
}

func buildLogicalHashCache(g *Graph) (*logicalHashCache, error) {
	cache := &logicalHashCache{}
	add := func(category int, key string, value any) error {
		entry, err := hashLogicalEntry(category, key, value)
		if err != nil {
			return err
		}
		index := graphMapShard(key)
		block := cache.categories[category][index]
		if block == nil {
			block = &logicalHashBlock{entries: make(map[string]logicalHashEntry)}
			cache.categories[category][index] = block
		}
		block.entries[key] = entry
		cache.logicalBytes += entry.bytes
		return nil
	}
	for key, value := range g.CITypes {
		if err := add(0, key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range g.Entities.All() {
		if err := add(1, key, logicalEntityForHash(value)); err != nil {
			return nil, err
		}
	}
	for key, value := range g.RelationTypes {
		if err := add(2, key, value); err != nil {
			return nil, err
		}
	}
	for key, value := range g.Edges.All() {
		if err := add(3, key, logicalEdgeForHash(value)); err != nil {
			return nil, err
		}
	}
	for _, category := range cache.categories {
		for _, block := range category {
			if block != nil {
				block.rehash()
			}
		}
	}
	return cache, nil
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

func (g *Graph) refreshLogicalHashCache(tracker *mutationFingerprintTracker) error {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	cache := g.logicalHashCache
	if cache == nil {
		return nil
	}
	for category, touched := range []map[string]trackedFingerprint{tracker.ciTypes, tracker.entities, tracker.relationTypes, tracker.edges} {
		var copied [graphMapShards]bool
		for key := range touched {
			index := graphMapShard(key)
			block := cache.categories[category][index]
			if !copied[index] {
				if block == nil {
					block = &logicalHashBlock{entries: make(map[string]logicalHashEntry)}
				} else {
					block = &logicalHashBlock{entries: maps.Clone(block.entries)}
				}
				cache.categories[category][index] = block
				copied[index] = true
			}
			cache.logicalBytes -= block.entries[key].bytes
			value, exists := g.logicalHashValue(category, key)
			if exists {
				entry, err := hashLogicalEntry(category, key, value)
				if err != nil {
					return err
				}
				block.entries[key] = entry
				cache.logicalBytes += entry.bytes
			} else {
				delete(block.entries, key)
			}
		}
		for index, changed := range copied {
			if changed {
				block := cache.categories[category][index]
				if len(block.entries) == 0 {
					cache.categories[category][index] = nil
				} else {
					block.rehash()
				}
			}
		}
	}
	cache.finalReady = false
	return nil
}

func (g *Graph) logicalHashValue(category int, key string) (any, bool) {
	switch category {
	case 0:
		value, ok := g.CITypes[key]
		return value, ok
	case 1:
		value, ok := g.Entities.Get(key)
		return logicalEntityForHash(value), ok
	case 2:
		value, ok := g.RelationTypes[key]
		return value, ok
	default:
		value, ok := g.Edges.Get(key)
		return logicalEdgeForHash(value), ok
	}
}
