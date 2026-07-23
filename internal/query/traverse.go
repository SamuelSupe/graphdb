package query

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func executeTraverse(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	if request.ID == "" {
		return Response{}, fmt.Errorf("%w: traverse requires id", ErrInvalid)
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
	if err := validateDirection(request.Direction); err != nil {
		return Response{}, err
	}
	var paths []graph.Path
	if err := budget.measure("bounded-bfs", fmt.Sprintf("depth=%d", normalizedDepth(request.Depth)), 0, func() (int, error) {
		var err error
		paths, err = collectPaths(g, start, request, maxPathResults(request), budget)
		return len(paths), err
	}); err != nil {
		return Response{}, err
	}
	results := pathResults(paths, request.Project)
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		sortResults(results, request.Sort)
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	return buildResponse(g.Version, results, request, cursor, budget)
}

func executeImpact(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	request.DirectionStrategy = "impact"
	if request.Depth <= 0 {
		request.Depth = 4
	}
	return executeTraverse(g, request, cursor, budget)
}

func collectPaths(g *graph.Graph, start graph.Entity, request Request, maxResults int, budget *budget) ([]graph.Path, error) {
	queue := []pendingPath{{
		entityID: start.ID,
		path:     graph.Path{Entities: []graph.Entity{start}},
		visited:  map[string]struct{}{start.ID: {}},
	}}
	results := make([]graph.Path, 0)
	depth := normalizedDepth(request.Depth)
	if !pathPrefixMatches(queue[0].path, request.Path, false) {
		return results, nil
	}
	for level := 0; level < depth && len(queue) > 0; level++ {
		if err := budget.check(); err != nil {
			return nil, err
		}
		next := make([]pendingPath, 0)
		finalLevel := level+1 == depth
		for _, current := range queue {
			neighbors, err := neighborsForBudget(g, current.entityID, requestForPathLevel(request, level), budget)
			if err != nil {
				return nil, err
			}
			for _, neighbor := range neighbors {
				budget.visited++
				if _, seen := current.visited[neighbor.Entity.ID]; seen {
					continue
				}
				newPath := clonePath(current.path)
				newPath.Entities = append(newPath.Entities, neighbor.Entity)
				newPath.Edges = append(newPath.Edges, neighbor.Edge)
				if !pathPrefixMatches(newPath, request.Path, finalLevel) {
					continue
				}
				if pathMatches(newPath, request.Path) {
					results = append(results, newPath)
					if len(results) >= maxResults {
						return results, nil
					}
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
	return results, nil
}

func requestForPathLevel(request Request, level int) Request {
	if level >= len(request.Path.Steps) {
		return request
	}
	step := request.Path.Steps[level]
	if step.Direction != "" {
		request.Direction = step.Direction
	}
	if len(step.RelationTypes) == 0 {
		return request
	}
	allowed := relationTypeSet(request)
	relations := make([]string, 0, len(step.RelationTypes))
	for _, relation := range step.RelationTypes {
		if len(allowed) == 0 {
			relations = append(relations, relation)
			continue
		}
		if _, ok := allowed[relation]; ok {
			relations = append(relations, relation)
		}
	}
	if len(relations) == 0 {
		relations = []string{"\x00"}
	}
	request.RelationType = ""
	request.RelationTypes = relations
	request.Path.RelationTypes = nil
	return request
}
