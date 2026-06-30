package query

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func executeShortestPath(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	if request.ID == "" || request.TargetID == "" {
		return Response{}, fmt.Errorf("%w: shortest_path requires id and target_id", ErrInvalid)
	}
	if err := validateDirection(request.Direction); err != nil {
		return Response{}, err
	}
	start, ok, err := materializeEntity(g, request.ID, request, budget)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return Response{}, ErrIndexUnavailable
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.ID)
	}
	target, ok, err := materializeEntity(g, request.TargetID, request, budget)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return Response{}, ErrIndexUnavailable
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.TargetID)
	}
	request.Depth = normalizedDepth(request.Depth)
	var path graph.Path
	var found bool
	if err := budget.measure("shortest-bfs", fmt.Sprintf("depth=%d", request.Depth), 0, func() (int, error) {
		var err error
		path, found, err = shortestPath(g, start, target, request, budget)
		if found {
			return len(path.Edges), err
		}
		return 0, err
	}); err != nil {
		return Response{}, err
	}
	results := []Result{}
	if found {
		results = pathResults([]graph.Path{path}, request.Project)
	}
	return buildResponse(g.Version, results, request, cursor, budget)
}

func shortestPath(g *graph.Graph, start graph.Entity, target graph.Entity, request Request, budget *budget) (graph.Path, bool, error) {
	queue := []pendingPath{{entityID: start.ID, path: graph.Path{Entities: []graph.Entity{start}}, visited: map[string]struct{}{start.ID: {}}}}
	depth := normalizedDepth(request.Depth)
	if !pathPrefixMatches(queue[0].path, request.Path, false) {
		return graph.Path{}, false, nil
	}
	for level := 0; level < depth && len(queue) > 0; level++ {
		if err := budget.check(); err != nil {
			return graph.Path{}, false, err
		}
		next := make([]pendingPath, 0)
		finalLevel := level+1 == depth
		for _, current := range queue {
			neighbors, err := neighborsForBudget(g, current.entityID, request, budget)
			if err != nil {
				return graph.Path{}, false, err
			}
			for _, neighbor := range neighbors {
				budget.visited++
				if _, seen := current.visited[neighbor.Entity.ID]; seen {
					continue
				}
				newPath := clonePath(current.path)
				newPath.Entities = append(newPath.Entities, neighbor.Entity)
				newPath.Edges = append(newPath.Edges, neighbor.Edge)
				if !pathPrefixMatches(newPath, request.Path, finalLevel || neighbor.Entity.ID == target.ID) {
					continue
				}
				if neighbor.Entity.ID == target.ID && pathMatches(newPath, request.Path) {
					return newPath, true, nil
				}
				if finalLevel {
					continue
				}
				visited := copyVisited(current.visited)
				visited[neighbor.Entity.ID] = struct{}{}
				next = append(next, pendingPath{entityID: neighbor.Entity.ID, path: newPath, visited: visited})
			}
		}
		queue = next
	}
	return graph.Path{}, false, nil
}
