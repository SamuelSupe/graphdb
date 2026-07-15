package graph

import "sort"

func (g *Graph) incidentEdgeIDs(entityID string) []string {
	seen := make(map[string]struct{}, len(g.out[entityID])+len(g.in[entityID]))
	for edgeID := range g.out[entityID] {
		seen[edgeID] = struct{}{}
	}
	for edgeID := range g.in[entityID] {
		seen[edgeID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for edgeID := range seen {
		ids = append(ids, edgeID)
	}
	sort.Strings(ids)
	return ids
}
