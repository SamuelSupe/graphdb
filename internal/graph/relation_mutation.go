package graph

import "fmt"

func (g *Graph) applyRelationTypeDeletes(
	names []string,
	tracker *mutationFingerprintTracker,
	affectedEdges *uniqueStringCollector,
) error {
	if len(names) == 0 {
		return nil
	}
	deleted := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("delete relation type name is required")
		}
		tracker.touchRelationType(name)
		delete(g.RelationTypes, name)
		deleted[name] = struct{}{}
	}
	for name := range deleted {
		for edgeID := range g.edgeTypeIndex[name] {
			edge, ok := g.Edges[edgeID]
			if !ok {
				continue
			}
			tracker.touchEdge(edgeID)
			affectedEdges.add(edgeID)
			g.removeEdgeFromIndexes(edgeID, edge)
			delete(g.Edges, edgeID)
		}
	}
	return nil
}

func (g *Graph) applyRelationTypeUpserts(
	relationTypes []RelationType,
	tracker *mutationFingerprintTracker,
) error {
	if len(relationTypes) == 0 {
		return nil
	}
	affected := make(map[string]struct{}, len(relationTypes))
	for _, relationType := range relationTypes {
		normalized, err := normalizeRelationType(relationType)
		if err != nil {
			return err
		}
		tracker.touchRelationType(normalized.Name)
		g.RelationTypes[normalized.Name] = normalized
		affected[normalized.Name] = struct{}{}
	}
	return g.validateRelationTypeEdges(affected)
}
