package graph

func (g *Graph) NeighborCount(entityID string, direction string, relationTypes map[string]struct{}, nodeKinds []string, impact bool) int {
	if direction == "" {
		direction = "both"
	}
	count := 0
	if direction == "out" || direction == "both" {
		count += g.countNeighborEdges(entityID, g.out[entityID], "out", relationTypes, nodeKinds, impact)
	}
	if direction == "in" || direction == "both" {
		count += g.countNeighborEdges(entityID, g.in[entityID], "in", relationTypes, nodeKinds, impact)
	}
	return count
}

func (g *Graph) countNeighborEdges(entityID string, edgeIDs map[string]struct{}, direction string, relationTypes map[string]struct{}, nodeKinds []string, impact bool) int {
	count := 0
	for edgeID := range edgeIDs {
		edge := g.Edges[edgeID]
		if !neighborRelationAllowed(edge.Type, relationTypes) || !neighborImpactAllowed(g, edge.Type, direction, impact) {
			continue
		}
		neighborID := edge.To
		if direction == "in" {
			neighborID = edge.From
		}
		neighbor, ok := g.Entities[neighborID]
		if !ok || !neighborKindAllowed(neighbor.Kind, nodeKinds) {
			continue
		}
		count++
	}
	return count
}

func neighborRelationAllowed(relationType string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[relationType]
	return ok
}

func neighborKindAllowed(kind string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, value := range allowed {
		if kind == value {
			return true
		}
	}
	return false
}

func neighborImpactAllowed(g *Graph, relationType string, direction string, impact bool) bool {
	if !impact {
		return true
	}
	relation, ok := g.RelationTypes[relationType]
	if !ok {
		return false
	}
	switch relation.ImpactDirection {
	case "forward":
		return direction == "out"
	case "reverse":
		return direction == "in"
	case "both":
		return true
	default:
		return false
	}
}
