package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

// visitPathsInIdentityOrder walks depth first so a path is emitted immediately
// before its extensions. With edge neighbors ordered by ID, this is the same
// order used by resultIdentity and lets stable cursors stop at a page boundary.
func visitPathsInIdentityOrder(
	g *graph.Graph,
	start graph.Entity,
	request Request,
	maxResults int,
	budget *budget,
	visit func(graph.Path) error,
) (int, error) {
	root := pendingPath{
		entityID: start.ID,
		path:     graph.Path{Entities: []graph.Entity{start}},
		visited:  map[string]struct{}{start.ID: {}},
	}
	if !pathPrefixMatches(root.path, request.Path, false) {
		return 0, nil
	}
	matched := 0
	depth := normalizedDepth(request.Depth)
	var walk func(pendingPath, int) (bool, error)
	walk = func(current pendingPath, level int) (bool, error) {
		if err := budget.check(); err != nil {
			return false, err
		}
		finalLevel := level+1 == depth
		levelRequest := requestForPathLevel(request, level)
		processNeighbor := func(neighbor graph.Neighbor) (bool, error) {
			budget.visited++
			if _, seen := current.visited[neighbor.Entity.ID]; seen {
				return true, nil
			}
			nextPath := clonePath(current.path)
			nextPath.Entities = append(nextPath.Entities, neighbor.Entity)
			nextPath.Edges = append(nextPath.Edges, neighbor.Edge)
			if !pathPrefixMatches(nextPath, request.Path, finalLevel) {
				return true, nil
			}
			if pathMatches(nextPath, request.Path) {
				if err := visit(nextPath); err != nil {
					return false, err
				}
				matched++
				if maxResults > 0 && matched >= maxResults {
					return false, nil
				}
			}
			if finalLevel {
				return true, nil
			}
			visited := copyVisited(current.visited)
			visited[neighbor.Entity.ID] = struct{}{}
			return walk(pendingPath{
				entityID: neighbor.Entity.ID,
				path:     nextPath,
				visited:  visited,
			}, level+1)
		}
		used, err := visitIndexedPathNeighbors(
			g,
			current.entityID,
			levelRequest,
			budget,
			processNeighbor,
		)
		if err != nil {
			return false, err
		}
		if used {
			return maxResults <= 0 || matched < maxResults, nil
		}
		neighbors, err := neighborsForBudget(
			g, current.entityID, levelRequest, budget,
		)
		if err != nil {
			return false, err
		}
		for _, neighbor := range neighbors {
			keepGoing, err := processNeighbor(neighbor)
			if err != nil || !keepGoing {
				return false, err
			}
		}
		return true, nil
	}
	_, err := walk(root, 0)
	return matched, err
}
