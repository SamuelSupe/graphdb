package graph

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
)

type trackedFingerprint struct {
	exists bool
	value  [md5.Size]byte
}

type mutationFingerprintTracker struct {
	graph         *Graph
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

func newMutationFingerprintTracker(g *Graph) *mutationFingerprintTracker {
	return &mutationFingerprintTracker{
		graph:         g,
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
	t.ciTypes[name] = t.capture("ci_type", name, value, exists)
}

func (t *mutationFingerprintTracker) touchEntity(id string) {
	if _, ok := t.entities[id]; ok || t.err != nil {
		return
	}
	value, exists := t.graph.Entities[id]
	if exists {
		t.entities[id] = t.capture("entity", id, logicalEntityForHash(value), true)
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
	t.relationTypes[name] = t.capture("relation_type", name, value, exists)
}

func (t *mutationFingerprintTracker) touchEdge(id string) {
	if _, ok := t.edges[id]; ok || t.err != nil {
		return
	}
	value, exists := t.graph.Edges[id]
	if exists {
		t.edges[id] = t.capture("edge", id, logicalEdgeForHash(value), true)
		return
	}
	t.edges[id] = trackedFingerprint{}
}

func (t *mutationFingerprintTracker) capture(kind string, key string, value any, exists bool) trackedFingerprint {
	if !exists {
		return trackedFingerprint{}
	}
	fingerprint, err := contentFingerprintEntry(kind, key, value)
	if err != nil {
		t.err = err
		return trackedFingerprint{}
	}
	return trackedFingerprint{exists: true, value: fingerprint}
}

func (t *mutationFingerprintTracker) finish(report *ApplyReport) error {
	if t.err != nil {
		return t.err
	}
	t.graph.contentFingerprintMu.Lock()
	defer t.graph.contentFingerprintMu.Unlock()
	changed := false
	apply := func(before trackedFingerprint, after trackedFingerprint) {
		if before == after {
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
		apply(before, t.capture("ci_type", key, value, exists))
	}
	for key, before := range t.entities {
		value, exists := t.graph.Entities[key]
		if exists {
			apply(before, t.capture("entity", key, logicalEntityForHash(value), true))
		} else {
			apply(before, trackedFingerprint{})
		}
	}
	for key, before := range t.relationTypes {
		value, exists := t.graph.RelationTypes[key]
		apply(before, t.capture("relation_type", key, value, exists))
	}
	for key, before := range t.edges {
		value, exists := t.graph.Edges[key]
		if exists {
			apply(before, t.capture("edge", key, logicalEdgeForHash(value), true))
		} else {
			apply(before, trackedFingerprint{})
		}
	}
	if t.err != nil {
		return t.err
	}
	report.Changed = changed
	report.ContentFingerprint = hex.EncodeToString(t.graph.contentFingerprint[:])
	return nil
}

func contentFingerprintEntry(kind string, key string, value any) ([md5.Size]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return [md5.Size]byte{}, err
	}
	payload := make([]byte, 0, len(kind)+len(key)+len(data)+2)
	payload = append(payload, kind...)
	payload = append(payload, 0)
	payload = append(payload, key...)
	payload = append(payload, 0)
	payload = append(payload, data...)
	return md5.Sum(payload), nil
}

func xorFingerprint(target *[md5.Size]byte, value [md5.Size]byte) {
	for i := range target {
		target[i] ^= value[i]
	}
}
