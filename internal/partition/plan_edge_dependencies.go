package partition

import (
	"fmt"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// A merge or split can rewrite thousands of canonical edge IDs. Resolve the
// new locators and directories in bounded waves before following their owners.
func (l *fragmentLoader) rewrittenEdgeDependencies(g *graph.Graph) error {
	edges := make([]graph.Edge, 0, min(g.Edges.Len(), 128))
	for _, edge := range g.Edges.All() {
		edges = append(edges, edge)
		if len(edges) == cap(edges) {
			if err := l.edgeDependencyBatch(edges, g.RelationTypes); err != nil {
				return err
			}
			edges = edges[:0]
		}
	}
	return l.edgeDependencyBatch(edges, g.RelationTypes)
}

func (l *fragmentLoader) edgeDependencyBatch(edges []graph.Edge, types map[string]graph.RelationType) error {
	if len(edges) == 0 {
		return nil
	}
	var ids []string
	var directoryKeys []string
	for _, edge := range edges {
		edge.Type, edge.From, edge.To = strings.TrimSpace(edge.Type), strings.TrimSpace(edge.From), strings.TrimSpace(edge.To)
		ids = append(ids, graph.CanonicalEdgeID(edge), edge.ID)
		directoryKeys = append(directoryKeys, cardinalityKeys(edge, types)...)
	}
	if err := l.prefetchEdgeLocatorsAndRecords(ids, directoryKeys); err != nil {
		return err
	}
	for _, edge := range edges {
		if err := l.edgeDependencies(edge, types); err != nil {
			return err
		}
	}
	return nil
}

func (l *fragmentLoader) prefetchEdgeIDs(ids []string) error {
	if len(ids) < 2 {
		return nil
	}
	if _, ok := l.view.Remote.(batchReader); !ok {
		return nil
	}
	for start := 0; start < len(ids); start += 128 {
		if err := l.prefetchEdgeLocatorsAndRecords(ids[start:min(len(ids), start+128)], nil); err != nil {
			return err
		}
	}
	return nil
}

func (l *fragmentLoader) prefetchEdgeLocatorsAndRecords(ids []string, extraDirectoryKeys []string) error {
	if _, ok := l.view.Remote.(batchReader); !ok {
		return nil
	}
	if l.edgeLocations == nil {
		l.edgeLocations = make(map[string]edgeLocation)
	}
	resolveLocators := func(candidates []string) ([]edgeLocation, []string, error) {
		byPartition := make(map[uint8][]string)
		lookupIDs := make(map[string]string)
		var located []edgeLocation
		var missingAliases []string
		for _, id := range candidates {
			if id == "" || l.edgeLookups[id] {
				continue
			}
			if loc, cached := l.edgeLocations[id]; cached {
				if loc.ID != "" {
					located = append(located, loc)
				} else {
					missingAliases = append(missingAliases, EdgeAliasKey(id))
				}
				continue
			}
			key := EdgeLocatorKey(id)
			if _, exists := lookupIDs[key]; !exists {
				lookupIDs[key] = id
				bucket := Bucket(key)
				byPartition[bucket] = append(byPartition[bucket], key)
			}
		}
		if len(byPartition) > 0 {
			if err := l.view.visitKeys(l.ctx, byPartition, func(bucket uint8, row Row) error {
				var loc edgeLocation
				if err := decodeRecord(row, &loc); err != nil {
					return err
				}
				if loc.ID == "" || loc.ID != lookupIDs[row.Key] || Bucket(row.Key) != bucket {
					return fmt.Errorf("%w: edge locator identity mismatch", ErrPartitionCorrupt)
				}
				l.edgeLocations[loc.ID] = loc
				located = append(located, loc)
				return nil
			}); err != nil {
				return nil, nil, err
			}
		}
		for _, id := range lookupIDs {
			if _, found := l.edgeLocations[id]; !found {
				l.edgeLocations[id] = edgeLocation{}
				missingAliases = append(missingAliases, EdgeAliasKey(id))
			}
		}
		return located, missingAliases, nil
	}

	located, aliasKeys, err := resolveLocators(ids)
	if err != nil {
		return err
	}
	allDirKeys := make([]string, 0, len(extraDirectoryKeys)+len(aliasKeys))
	allDirKeys = append(allDirKeys, extraDirectoryKeys...)
	allDirKeys = append(allDirKeys, aliasKeys...)
	if err := l.directories.prefetch(allDirKeys); err != nil {
		return err
	}
	var ownerIDs []string
	for _, key := range allDirKeys {
		if owners, ok := l.directories.owners[key]; ok {
			ownerIDs = append(ownerIDs, owners...)
		}
	}
	if len(ownerIDs) > 0 {
		moreLocated, moreAliases, err := resolveLocators(ownerIDs)
		if err != nil {
			return err
		}
		located = append(located, moreLocated...)
		if err := l.directories.prefetch(moreAliases); err != nil {
			return err
		}
	}
	edgeKeys := make(map[uint8][]string)
	seenEdges := make(map[string]edgeLocation)
	for _, loc := range located {
		if loc.ID == "" {
			continue
		}
		if _, loaded := l.edges[loc.ID]; loaded {
			continue
		}
		if _, seen := seenEdges[loc.ID]; seen {
			continue
		}
		seenEdges[loc.ID] = loc
		edgeKeys[loc.Partition] = append(edgeKeys[loc.Partition], EdgeKey(loc.ID))
	}
	if len(edgeKeys) == 0 {
		return nil
	}
	fetched, err := l.view.fetchEdges(l.ctx, edgeKeys)
	if err != nil {
		return err
	}
	endpoints := make([]string, 0, len(fetched)*2)
	for id, loc := range seenEdges {
		edge, ok := fetched[id]
		if !ok || edge.ID != loc.ID {
			return fmt.Errorf("edge locator has no matching primary record")
		}
		l.edges[edge.ID] = edge
		if err := l.charge(edge); err != nil {
			return err
		}
		endpoints = append(endpoints, edge.From, edge.To)
	}
	return l.prefetchEntitiesAndAliases(endpoints)
}

func (l *fragmentLoader) prefetchEntitiesAndAliases(ids []string) error {
	if err := l.prefetchEntityIDs(ids); err != nil {
		return err
	}
	var aliasKeys []string
	for _, id := range ids {
		if id != "" && l.primaryMissing[id] {
			aliasKeys = append(aliasKeys, EntityAliasKey(id))
		}
	}
	if len(aliasKeys) == 0 {
		return nil
	}
	if err := l.directories.prefetch(aliasKeys); err != nil {
		return err
	}
	var owners []string
	for _, key := range aliasKeys {
		owners = append(owners, l.directories.owners[key]...)
	}
	return l.prefetchEntityIDs(owners)
}
