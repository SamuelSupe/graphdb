package graph

import "sort"

func (g *Graph) invalidateNeighborOrder(ids ...string) {
	g.neighborOrderMu.Lock()
	defer g.neighborOrderMu.Unlock()
	for _, id := range ids {
		for _, direction := range []string{"out", "in", "both"} {
			key := id + "\x00" + direction
			g.neighborOrderBytes -= int64(len(g.neighborOrder[key])) * 48
			delete(g.neighborOrder, key)
		}
	}
}

func (g *Graph) sortedNeighborRefs(id, selectedDirection string, charge func() error) ([]neighborRef, error) {
	if selectedDirection == "" {
		selectedDirection = "both"
	}
	key := id + "\x00" + selectedDirection
	g.neighborOrderMu.Lock()
	if refs, ok := g.neighborOrder[key]; ok {
		g.neighborOrderMu.Unlock()
		return refs, nil
	}
	g.neighborOrderMu.Unlock()
	var refs []neighborRef
	for _, direction := range []string{"out", "in"} {
		if selectedDirection != "both" && direction != selectedDirection {
			continue
		}
		adjacency := g.out
		if direction == "in" {
			adjacency = g.in
		}
		for edgeID := range adjacency.At(id) {
			if charge != nil {
				if err := charge(); err != nil {
					return nil, err
				}
			}
			edge := g.Edges.At(edgeID)
			endpoint := edge.To
			if direction == "in" {
				endpoint = edge.From
			}
			refs = append(refs, neighborRef{edgeID: edgeID, entityID: endpoint, direction: direction})
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].edgeID != refs[j].edgeID {
			return refs[i].edgeID < refs[j].edgeID
		}
		if refs[i].entityID != refs[j].entityID {
			return refs[i].entityID < refs[j].entityID
		}
		return refs[i].direction < refs[j].direction
	})
	g.neighborOrderMu.Lock()
	defer g.neighborOrderMu.Unlock()
	if existing, ok := g.neighborOrder[key]; ok {
		return existing, nil
	}
	// Bound both retained adjacency memory and the map copied by a commit.
	bytes := int64(len(refs)) * 48
	if bytes > 32<<20 {
		return refs, nil
	}
	for len(g.neighborOrder) >= 256 || g.neighborOrderBytes+bytes > 32<<20 {
		for key, old := range g.neighborOrder {
			g.neighborOrderBytes -= int64(len(old)) * 48
			delete(g.neighborOrder, key)
			break
		}
	}
	g.neighborOrderBytes += bytes
	if g.neighborOrder == nil {
		g.neighborOrder = map[string][]neighborRef{}
	}
	g.neighborOrder[key] = refs
	return refs, nil
}

// VisitNeighborsByEdgeID seeks the immutable adjacency order. Building a cold
// order charges every edge once; subsequent pages charge only their candidates.
func (g *Graph) VisitNeighborsByEdgeID(id, direction string, types map[string]struct{}, kinds []string, impact bool, startEdgeID string, charge func() error, visit func(Neighbor) (bool, error)) error {
	refs, err := g.sortedNeighborRefs(id, direction, charge)
	if err != nil {
		return err
	}
	start := sort.Search(len(refs), func(i int) bool { return refs[i].edgeID >= startEdgeID })
	for _, ref := range refs[start:] {
		if charge != nil {
			if err := charge(); err != nil {
				return err
			}
		}
		if direction != "" && direction != "both" && ref.direction != direction {
			continue
		}
		edge, ok := g.Edges.Get(ref.edgeID)
		if !ok || !neighborRelationAllowed(edge.Type, types) || !neighborImpactAllowed(g, edge.Type, ref.direction, impact) {
			continue
		}
		entity, ok := g.Entities.Get(ref.entityID)
		if !ok || !neighborKindAllowed(entity.Kind, kinds) {
			continue
		}
		more, err := visit(Neighbor{Entity: copyEntity(entity), Edge: copyEdge(edge), Direction: ref.direction})
		if err != nil || !more {
			return err
		}
	}
	return nil
}
