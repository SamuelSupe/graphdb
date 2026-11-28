package query

import "github.com/SamuelSupe/graphdb/v2/internal/graph"

type shortestLink struct {
	id     string
	edgeID string
	edge   graph.Edge
	depth  int
}

func (l shortestLink) linkEdgeID() string {
	if l.edgeID != "" {
		return l.edgeID
	}
	return l.edge.ID
}

func (l shortestLink) resolveEdge(g *graph.Graph) graph.Edge {
	if l.edge.ID != "" {
		return l.edge
	}
	edge, _ := g.GetEdge(l.edgeID)
	return edge
}

type shortestFrontier struct {
	ids   []string
	links map[string]shortestLink
	depth int
}

func materializedShortestPath(g *graph.Graph, start, target string, request Request, budget *budget) (graph.Path, bool, error) {
	forward := shortestFrontier{ids: []string{start}, links: map[string]shortestLink{start: {}}}
	backward := shortestFrontier{ids: []string{target}, links: map[string]shortestLink{target: {}}}
	reverse := request
	switch request.Direction {
	case "in":
		reverse.Direction = "out"
	case "out":
		reverse.Direction = "in"
	}
	for range request.Depth {
		if len(forward.ids) == 0 || len(backward.ids) == 0 {
			break
		}
		if err := budget.check(); err != nil {
			return graph.Path{}, false, err
		}
		side, step, backwards := &forward, request, false
		if backward.scanSize(g, reverse.Direction) < forward.scanSize(g, request.Direction) {
			side, step, backwards = &backward, reverse, true
		}
		meet, err := side.expand(g, step, budget, backwards, backward.links)
		if err != nil {
			return graph.Path{}, false, err
		}
		if meet != "" {
			return joinedShortestPath(g, meet, forward.links, backward.links), true, nil
		}
		// At the first intersection the two depths sum to the shortest
		// distance. Forward discovery order preserves the original BFS tie.
		for _, id := range forward.ids {
			if _, ok := backward.links[id]; ok {
				return joinedShortestPath(g, id, forward.links, backward.links), true, nil
			}
		}
	}
	return graph.Path{}, false, nil
}

func (f shortestFrontier) scanSize(g *graph.Graph, direction string) int {
	count := 0
	for _, id := range f.ids {
		count += g.NeighborScanSize(id, direction)
	}
	return count
}

func (f *shortestFrontier) expand(g *graph.Graph, request Request, budget *budget, backwards bool, destinations map[string]shortestLink) (string, error) {
	var next []string
	f.depth++
	fastIDs := (budget == nil || (budget.lookup == nil && budget.entityPredicate == nil)) &&
		len(request.EdgeWhere) == 0 && request.EdgeWhereExpr == nil
	var allowed map[string]struct{}
	var direction string
	impact := false
	if fastIDs {
		allowed = relationTypeSet(request)
		if request.DirectionStrategy == "impact" {
			direction = "both"
			impact = true
		} else {
			direction = request.Direction
		}
	}
	charge := func() error {
		if budget == nil {
			return nil
		}
		return budget.add(1)
	}
	for _, id := range f.ids {
		if err := budget.check(); err != nil {
			return "", err
		}
		if fastIDs {
			var met string
			err := g.VisitFilteredNeighborIDs(id, direction, allowed, request.Path.NodeKinds, impact, charge, func(neighborID, edgeID string) (bool, error) {
				budget.visited++
				link, seen := f.links[neighborID]
				if !seen {
					next = append(next, neighborID)
				} else if !backwards || link.depth != f.depth ||
					link.linkEdgeID() < edgeID || link.linkEdgeID() == edgeID && link.id <= id {
					return true, nil
				}
				f.links[neighborID] = shortestLink{id: id, edgeID: edgeID, depth: f.depth}
				if _, found := destinations[neighborID]; !backwards && found {
					met = neighborID
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				return "", err
			}
			if met != "" {
				return met, nil
			}
			continue
		}
		neighbors, err := neighborsForBudget(g, id, request, budget)
		if err != nil {
			return "", err
		}
		for _, neighbor := range neighbors {
			budget.visited++
			if !budget.allowsEntity(neighbor.Entity) {
				continue
			}
			link, seen := f.links[neighbor.Entity.ID]
			if !seen {
				next = append(next, neighbor.Entity.ID)
			} else if !backwards || link.depth != f.depth ||
				link.linkEdgeID() < neighbor.Edge.ID || link.linkEdgeID() == neighbor.Edge.ID && link.id <= id {
				continue
			}
			// Reverse discovery order is irrelevant. For each shortest suffix
			// keep the first edge in the original forward adjacency order.
			f.links[neighbor.Entity.ID] = shortestLink{id: id, edge: neighbor.Edge, depth: f.depth}
			if _, found := destinations[neighbor.Entity.ID]; !backwards && found {
				// The first forward match is already the earliest ordered
				// prefix in this layer. The reverse layer is complete.
				return neighbor.Entity.ID, nil
			}
		}
	}
	f.ids = next
	return "", nil
}

func joinedShortestPath(g *graph.Graph, meet string, forward, backward map[string]shortestLink) graph.Path {
	var path graph.Path
	for id := meet; id != ""; {
		entity, _ := g.GetEntity(id)
		path.Entities = append(path.Entities, entity)
		link := forward[id]
		if link.id != "" {
			path.Edges = append(path.Edges, link.resolveEdge(g))
		}
		id = link.id
	}
	reverseEntities(path.Entities)
	reverseEdges(path.Edges)
	for link := backward[meet]; link.id != ""; link = backward[link.id] {
		entity, _ := g.GetEntity(link.id)
		path.Entities = append(path.Entities, entity)
		path.Edges = append(path.Edges, link.resolveEdge(g))
	}
	return path
}
