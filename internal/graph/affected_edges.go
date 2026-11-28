package graph

import "sort"

func (g *Graph) incidentEdgeIDs(entityID string) []string {
	seen := make(map[string]struct{}, g.out.At(entityID).Len()+g.in.At(entityID).Len())
	for edgeID := range g.out.At(entityID).Keys() {
		seen[edgeID] = struct{}{}
	}
	for edgeID := range g.in.At(entityID).Keys() {
		seen[edgeID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for edgeID := range seen {
		ids = append(ids, edgeID)
	}
	sort.Strings(ids)
	return ids
}
