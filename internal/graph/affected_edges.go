package graph

import "sort"

func (g *Graph) incidentEdgeIDs(entityID string) []string {
	seen := make(map[string]struct{}, len(g.out.At(entityID))+len(g.in.At(entityID)))
	for edgeID := range g.out.At(entityID) {
		seen[edgeID] = struct{}{}
	}
	for edgeID := range g.in.At(entityID) {
		seen[edgeID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for edgeID := range seen {
		ids = append(ids, edgeID)
	}
	sort.Strings(ids)
	return ids
}
