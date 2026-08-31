package graph

import (
	"sort"
	"sync"
	"time"
)

type Graph struct {
	Version       int64
	CITypes       map[string]CIType
	Entities      map[string]Entity
	RelationTypes map[string]RelationType
	Edges         map[string]Edge

	out               map[string]map[string]struct{}
	in                map[string]map[string]struct{}
	edgeAliasIndex    map[string]map[string]struct{}
	edgeTypeIndex     map[string]map[string]struct{}
	entityAliasIndex  map[string]map[string]struct{}
	kindCounts        map[string]int
	fieldIndex        map[string]map[string]map[string]map[string]struct{}
	identityIndex     map[string]map[string]string
	cow               *copyOnWriteState
	entityOrder       map[string][]string
	entityOrderMu     sync.Mutex
	fieldIndexOrder   map[fieldIndexOrderKey][]string
	fieldIndexOrderMu sync.Mutex

	contentFingerprint      [16]byte
	contentFingerprintReady bool
	contentFingerprintMu    sync.Mutex
	logicalHashCache        *logicalHashCache
	logicalHashMu           sync.Mutex
}

func New() *Graph {
	g := &Graph{
		CITypes:       map[string]CIType{},
		Entities:      map[string]Entity{},
		RelationTypes: map[string]RelationType{},
		Edges:         map[string]Edge{},
	}
	for _, relationType := range StandardRelationTypes() {
		g.RelationTypes[relationType.Name] = relationType
	}
	g.rebuildIndexes()
	return g
}

func FromSnapshot(snapshot Snapshot) (*Graph, error) {
	g := New()
	g.Version = snapshot.Version
	for _, ciType := range snapshot.CITypes {
		normalized, err := normalizeCIType(ciType)
		if err != nil {
			return nil, err
		}
		g.CITypes[normalized.Name] = normalized
	}
	for _, relationType := range snapshot.RelationTypes {
		normalized, err := normalizeRelationType(relationType)
		if err != nil {
			return nil, err
		}
		g.RelationTypes[normalized.Name] = normalized
	}
	for _, entity := range snapshot.Entities {
		normalized, err := normalizeEntity(entity)
		if err != nil {
			return nil, err
		}
		g.Entities[normalized.ID] = normalized
	}
	g.rebuildIndexes()
	entityAliases := g.canonicalizeEntitySet(snapshot.Version, snapshotVersionTime(snapshot))
	edges := make([]Edge, 0, len(snapshot.Edges))
	for _, edge := range snapshot.Edges {
		normalized, err := normalizeEdge(edge)
		if err != nil {
			return nil, err
		}
		if id := entityAliases[normalized.From]; id != "" {
			normalized.From = id
		}
		if id := entityAliases[normalized.To]; id != "" {
			normalized.To = id
		}
		edges = append(edges, normalized)
	}
	g.Edges, _ = mergeEdgeList(edges, snapshot.Version, snapshotVersionTime(snapshot))
	g.rebuildIndexes()
	if err := g.validateAllEdges(); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Graph) Clone() *Graph {
	fingerprint, fingerprintReady := g.contentFingerprintState()
	logicalHashCache := g.cloneLogicalHashCache()
	clone := &Graph{
		Version:                 g.Version,
		CITypes:                 map[string]CIType{},
		Entities:                map[string]Entity{},
		RelationTypes:           map[string]RelationType{},
		Edges:                   map[string]Edge{},
		out:                     copySetMap(g.out),
		in:                      copySetMap(g.in),
		edgeAliasIndex:          copySetMap(g.edgeAliasIndex),
		edgeTypeIndex:           copySetMap(g.edgeTypeIndex),
		entityAliasIndex:        copySetMap(g.entityAliasIndex),
		kindCounts:              shallowCopyMap(g.kindCounts),
		fieldIndex:              copyFieldIndex(g.fieldIndex),
		identityIndex:           copyStringMap(g.identityIndex),
		contentFingerprint:      fingerprint,
		contentFingerprintReady: fingerprintReady,
		logicalHashCache:        logicalHashCache,
	}
	for name, ciType := range g.CITypes {
		clone.CITypes[name] = copyCIType(ciType)
	}
	for id, entity := range g.Entities {
		clone.Entities[id] = copyEntity(entity)
	}
	for name, relationType := range g.RelationTypes {
		clone.RelationTypes[name] = copyRelationType(relationType)
	}
	for id, edge := range g.Edges {
		clone.Edges[id] = copyEdge(edge)
	}
	return clone
}

func snapshotVersionTime(snapshot Snapshot) time.Time {
	for _, edge := range snapshot.Edges {
		if !edge.UpdatedAt.IsZero() {
			return edge.UpdatedAt
		}
	}
	for _, entity := range snapshot.Entities {
		if !entity.UpdatedAt.IsZero() {
			return entity.UpdatedAt
		}
	}
	return time.Time{}
}

func (g *Graph) Snapshot() Snapshot {
	snapshot := Snapshot{
		Version:       g.Version,
		CITypes:       make([]CIType, 0, len(g.CITypes)),
		Entities:      make([]Entity, 0, len(g.Entities)),
		RelationTypes: make([]RelationType, 0, len(g.RelationTypes)),
		Edges:         make([]Edge, 0, len(g.Edges)),
	}
	for _, ciType := range g.CITypes {
		snapshot.CITypes = append(snapshot.CITypes, copyCIType(ciType))
	}
	for _, entity := range g.Entities {
		snapshot.Entities = append(snapshot.Entities, copyEntity(entity))
	}
	for _, relationType := range g.RelationTypes {
		snapshot.RelationTypes = append(snapshot.RelationTypes, copyRelationType(relationType))
	}
	for _, edge := range g.Edges {
		snapshot.Edges = append(snapshot.Edges, copyEdge(edge))
	}
	index := g.indexSnapshot()
	snapshot.Index = &index
	sort.Slice(snapshot.CITypes, func(i, j int) bool {
		return snapshot.CITypes[i].Name < snapshot.CITypes[j].Name
	})
	sort.Slice(snapshot.Entities, func(i, j int) bool {
		return snapshot.Entities[i].ID < snapshot.Entities[j].ID
	})
	sort.Slice(snapshot.RelationTypes, func(i, j int) bool {
		return snapshot.RelationTypes[i].Name < snapshot.RelationTypes[j].Name
	})
	sort.Slice(snapshot.Edges, func(i, j int) bool {
		return snapshot.Edges[i].ID < snapshot.Edges[j].ID
	})
	return snapshot
}

func (g *Graph) GetEntity(id string) (Entity, bool) {
	entity, ok := g.Entities[id]
	return copyEntity(entity), ok
}

func (g *Graph) GetEntityByReference(id string) (Entity, bool) {
	entityID := g.ResolveEntityReference(id)
	if entityID == "" {
		return Entity{}, false
	}
	return g.GetEntity(entityID)
}

func (g *Graph) ResolveEntityReference(id string) string {
	if id == "" {
		return ""
	}
	if _, ok := g.Entities[id]; ok {
		return id
	}
	matches := g.entityAliasIndex[id]
	if len(matches) != 1 {
		return ""
	}
	for entityID := range matches {
		if _, ok := g.Entities[entityID]; ok {
			return entityID
		}
	}
	return ""
}

func (g *Graph) ListCITypes() []CIType {
	items := make([]CIType, 0, len(g.CITypes))
	for _, ciType := range g.CITypes {
		items = append(items, copyCIType(ciType))
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})
	return items
}

func (g *Graph) ListRelationTypes() []RelationType {
	items := make([]RelationType, 0, len(g.RelationTypes))
	for _, relationType := range g.RelationTypes {
		items = append(items, copyRelationType(relationType))
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})
	return items
}
