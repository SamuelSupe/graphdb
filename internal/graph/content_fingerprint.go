package graph

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type trackedFingerprint struct {
	resolved bool
	exists   bool
	value    [md5.Size]byte
	encoded  []byte
}

type mutationFingerprintTracker struct {
	graph         *Graph
	previous      *mutationFingerprintTracker
	logicalCache  *logicalHashCache
	ciTypes       map[string]trackedFingerprint
	entities      map[string]trackedFingerprint
	relationTypes map[string]trackedFingerprint
	edges         map[string]trackedFingerprint
	err           error
}

func (g *Graph) ContentFingerprint() (string, error) {
	if err := g.ensureContentFingerprint(); err != nil {
		return "", err
	}
	fingerprint, _ := g.contentFingerprintState()
	return hex.EncodeToString(fingerprint[:]), nil
}

func (g *Graph) ensureContentFingerprint() error {
	g.contentFingerprintMu.Lock()
	defer g.contentFingerprintMu.Unlock()
	if g.contentFingerprintReady {
		return nil
	}
	var fingerprint [md5.Size]byte
	for key, value := range g.CITypes {
		entry, err := contentFingerprintEntry("ci_type", key, value)
		if err != nil {
			return err
		}
		xorFingerprint(&fingerprint, entry)
	}
	for key, value := range g.Entities {
		entry, err := contentFingerprintEntry("entity", key, logicalEntityForHash(value))
		if err != nil {
			return err
		}
		xorFingerprint(&fingerprint, entry)
	}
	for key, value := range g.RelationTypes {
		entry, err := contentFingerprintEntry("relation_type", key, value)
		if err != nil {
			return err
		}
		xorFingerprint(&fingerprint, entry)
	}
	for key, value := range g.Edges {
		entry, err := contentFingerprintEntry("edge", key, logicalEdgeForHash(value))
		if err != nil {
			return err
		}
		xorFingerprint(&fingerprint, entry)
	}
	g.contentFingerprint = fingerprint
	g.contentFingerprintReady = true
	return nil
}

func (g *Graph) contentFingerprintState() ([md5.Size]byte, bool) {
	g.contentFingerprintMu.Lock()
	defer g.contentFingerprintMu.Unlock()
	return g.contentFingerprint, g.contentFingerprintReady
}

func newMutationFingerprintTracker(g *Graph, previous *mutationFingerprintTracker) *mutationFingerprintTracker {
	return &mutationFingerprintTracker{
		graph:         g,
		previous:      previous,
		logicalCache:  g.logicalHashCacheView(),
		ciTypes:       map[string]trackedFingerprint{},
		entities:      map[string]trackedFingerprint{},
		relationTypes: map[string]trackedFingerprint{},
		edges:         map[string]trackedFingerprint{},
	}
}

func (t *mutationFingerprintTracker) touchCIType(name string) {
	if _, ok := t.ciTypes[name]; ok || t.err != nil {
		return
	}
	value, exists := t.graph.CITypes[name]
	t.ciTypes[name] = t.captureBefore("ci_type", name, value, exists)
}

func (t *mutationFingerprintTracker) touchEntity(id string) {
	if _, ok := t.entities[id]; ok || t.err != nil {
		return
	}
	value, exists := t.graph.Entities[id]
	if exists {
		t.entities[id] = t.captureBefore("entity", id, logicalEntityForHash(value), true)
		return
	}
	t.entities[id] = trackedFingerprint{}
}

func (t *mutationFingerprintTracker) touchEntityWithEdges(id string) {
	t.touchEntity(id)
	for edgeID := range t.graph.out[id] {
		t.touchEdge(edgeID)
	}
	for edgeID := range t.graph.in[id] {
		t.touchEdge(edgeID)
	}
}

func (t *mutationFingerprintTracker) touchMerge(request MergeRequest) {
	t.touchEntityWithEdges(request.TargetID)
	for _, id := range request.SourceIDs {
		if id != request.TargetID {
			t.touchEntityWithEdges(id)
		}
	}
}

func (t *mutationFingerprintTracker) touchSplit(request SplitRequest) {
	t.touchEntityWithEdges(request.SourceID)
	for _, entity := range request.Entities {
		if entity.ID != "" {
			t.touchEntity(entity.ID)
		}
	}
}

func (t *mutationFingerprintTracker) touchRelationType(name string) {
	if _, ok := t.relationTypes[name]; ok || t.err != nil {
		return
	}
	value, exists := t.graph.RelationTypes[name]
	t.relationTypes[name] = t.captureBefore("relation_type", name, value, exists)
}

func (t *mutationFingerprintTracker) touchEdge(id string) {
	if _, ok := t.edges[id]; ok || t.err != nil {
		return
	}
	value, exists := t.graph.Edges[id]
	if exists {
		t.edges[id] = t.captureBefore("edge", id, logicalEdgeForHash(value), true)
		return
	}
	t.edges[id] = trackedFingerprint{}
}

func (t *mutationFingerprintTracker) capture(kind string, key string, value any, exists bool) trackedFingerprint {
	if !exists {
		return trackedFingerprint{resolved: true}
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.err = err
		return trackedFingerprint{}
	}
	return trackedFingerprint{
		resolved: true,
		exists:   true,
		value:    contentFingerprintEntryJSON(kind, key, data),
		encoded:  data,
	}
}

func (t *mutationFingerprintTracker) captureBefore(kind string, key string, value any, exists bool) trackedFingerprint {
	if t.previous != nil {
		if previous, ok := t.previous.lookup(kind, key); ok {
			return previous
		}
	}
	if exists && t.logicalCache != nil {
		category := logicalHashCategoryForKind(t.logicalCache, kind)
		index := sort.SearchStrings(category.keys, key)
		if index < len(category.keys) && category.keys[index] == key {
			encoded := category.encoded[index]
			fingerprint := contentFingerprintEntryJSON(kind, key, encoded)
			if len(category.fingerprints) == len(category.keys) {
				fingerprint = category.fingerprints[index]
			}
			return trackedFingerprint{
				resolved: true,
				exists:   true,
				value:    fingerprint,
				encoded:  encoded,
			}
		}
	}
	return t.capture(kind, key, value, exists)
}

func (t *mutationFingerprintTracker) lookup(kind string, key string) (trackedFingerprint, bool) {
	switch kind {
	case "ci_type":
		value, ok := t.ciTypes[key]
		return value, ok
	case "entity":
		value, ok := t.entities[key]
		return value, ok
	case "relation_type":
		value, ok := t.relationTypes[key]
		return value, ok
	case "edge":
		value, ok := t.edges[key]
		return value, ok
	default:
		return trackedFingerprint{}, false
	}
}

func logicalHashCategoryForKind(cache *logicalHashCache, kind string) logicalHashCategory {
	switch kind {
	case "ci_type":
		return cache.ciTypes
	case "entity":
		return cache.entities
	case "relation_type":
		return cache.relationTypes
	case "edge":
		return cache.edges
	default:
		return logicalHashCategory{}
	}
}

func (t *mutationFingerprintTracker) finish(report *ApplyReport, logicalHashBatch *mutationFingerprintTracker) error {
	if t.err != nil {
		return t.err
	}
	t.graph.contentFingerprintMu.Lock()
	changed := false
	apply := func(before trackedFingerprint, after trackedFingerprint) {
		if before.exists == after.exists && before.value == after.value {
			return
		}
		changed = true
		if before.exists {
			xorFingerprint(&t.graph.contentFingerprint, before.value)
		}
		if after.exists {
			xorFingerprint(&t.graph.contentFingerprint, after.value)
		}
	}
	for key, before := range t.ciTypes {
		value, exists := t.graph.CITypes[key]
		after := t.capture("ci_type", key, value, exists)
		apply(before, after)
		t.ciTypes[key] = after
	}
	for key, before := range t.entities {
		value, exists := t.graph.Entities[key]
		if exists {
			after := t.capture("entity", key, logicalEntityForHash(value), true)
			apply(before, after)
			t.entities[key] = after
		} else {
			after := trackedFingerprint{resolved: true}
			apply(before, after)
			t.entities[key] = after
		}
	}
	for key, before := range t.relationTypes {
		value, exists := t.graph.RelationTypes[key]
		after := t.capture("relation_type", key, value, exists)
		apply(before, after)
		t.relationTypes[key] = after
	}
	for key, before := range t.edges {
		value, exists := t.graph.Edges[key]
		if exists {
			after := t.capture("edge", key, logicalEdgeForHash(value), true)
			apply(before, after)
			t.edges[key] = after
		} else {
			after := trackedFingerprint{resolved: true}
			apply(before, after)
			t.edges[key] = after
		}
	}
	if t.err != nil {
		t.graph.contentFingerprintMu.Unlock()
		return t.err
	}
	report.Changed = changed
	report.ContentFingerprint = hex.EncodeToString(t.graph.contentFingerprint[:])
	t.graph.contentFingerprintMu.Unlock()
	if changed {
		if logicalHashBatch != nil {
			mergeLogicalHashTouches(logicalHashBatch, t)
			return nil
		}
		return t.graph.refreshLogicalHashCache(t)
	}
	return nil
}

func mergeLogicalHashTouches(target *mutationFingerprintTracker, source *mutationFingerprintTracker) {
	for key, value := range source.ciTypes {
		target.ciTypes[key] = value
	}
	for key, value := range source.entities {
		target.entities[key] = value
	}
	for key, value := range source.relationTypes {
		target.relationTypes[key] = value
	}
	for key, value := range source.edges {
		target.edges[key] = value
	}
}

func contentFingerprintEntry(kind string, key string, value any) ([md5.Size]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return [md5.Size]byte{}, err
	}
	return contentFingerprintEntryJSON(kind, key, data), nil
}

func contentFingerprintEntryJSON(kind string, key string, data []byte) [md5.Size]byte {
	payload := make([]byte, 0, len(kind)+len(key)+len(data)+2)
	payload = append(payload, kind...)
	payload = append(payload, 0)
	payload = append(payload, key...)
	payload = append(payload, 0)
	payload = append(payload, data...)
	return md5.Sum(payload)
}

func xorFingerprint(target *[md5.Size]byte, value [md5.Size]byte) {
	for i := range target {
		target[i] ^= value[i]
	}
}
