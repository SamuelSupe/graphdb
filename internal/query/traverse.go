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
	if requiresCompletePathScan(request) {
		return executeCompleteTraverse(
			g, start, request, cursor, budget,
		)
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

func executeCompleteTraverse(
	g *graph.Graph,
	start graph.Entity,
	request Request,
	cursor cursorState,
	budget *budget,
) (Response, error) {
	collector := newPathResultCollector(request)
	if err := budget.measure(
		"bounded-bfs",
		fmt.Sprintf("depth=%d", normalizedDepth(request.Depth)),
		0,
		func() (int, error) {
			return visitPaths(
				g, start, request, completePathScanLimit(request),
				budget, collector.add,
			)
		},
	); err != nil {
		return Response{}, err
	}
	var results []Result
	if err := budget.measure(
		"sort", "", collector.count,
		func() (int, error) {
			results = collector.results()
			return len(results), nil
		},
	); err != nil {
		return Response{}, err
	}
	return buildResponseWithAggregatesAndGroups(
		g.Version,
		results,
		request,
		cursor,
		budget,
		collector.aggregates(),
		collector.aggregateGroups(),
	)
}

func executeImpact(g *graph.Graph, request Request, cursor cursorState, budget *budget) (Response, error) {
	request.DirectionStrategy = "impact"
	if request.Depth <= 0 {
		request.Depth = 4
	}
	return executeTraverse(g, request, cursor, budget)
}

func collectPaths(g *graph.Graph, start graph.Entity, request Request, maxResults int, budget *budget) ([]graph.Path, error) {
	results := make([]graph.Path, 0, min(maxResults, maxQueryLimit+1))
	_, err := visitPathsInIdentityOrder(
		g, start, request, maxResults, budget,
		func(path graph.Path) error {
			results = append(results, path)
			return nil
		},
	)
	return results, err
}

func visitPaths(
	g *graph.Graph,
	start graph.Entity,
	request Request,
	maxResults int,
	budget *budget,
	visit func(graph.Path) error,
) (int, error) {
	queue := []pendingPath{{
		entityID: start.ID,
		path:     graph.Path{Entities: []graph.Entity{start}},
		visited:  map[string]struct{}{start.ID: {}},
	}}
	matched := 0
	depth := normalizedDepth(request.Depth)
	if !pathPrefixMatches(queue[0].path, request.Path, false) {
		return 0, nil
	}
	for level := 0; level < depth && len(queue) > 0; level++ {
		if err := budget.check(); err != nil {
			return matched, err
		}
		next := make([]pendingPath, 0)
		finalLevel := level+1 == depth
		for _, current := range queue {
			levelRequest := requestForPathLevel(request, level)
			limitReached := false
			processNeighbor := func(
				neighbor graph.Neighbor,
			) (bool, error) {
				budget.visited++
				if _, seen := current.visited[neighbor.Entity.ID]; seen {
					return true, nil
				}
				newPath := clonePath(current.path)
				newPath.Entities = append(newPath.Entities, neighbor.Entity)
				newPath.Edges = append(newPath.Edges, neighbor.Edge)
				if !pathPrefixMatches(newPath, request.Path, finalLevel) {
					return true, nil
				}
				if pathMatches(newPath, request.Path) {
					if err := visit(newPath); err != nil {
						return false, err
					}
					matched++
					if maxResults > 0 && matched >= maxResults {
						limitReached = true
						return false, nil
					}
				}
				if finalLevel {
					return true, nil
				}
				visited := copyVisited(current.visited)
				visited[neighbor.Entity.ID] = struct{}{}
				next = append(next, pendingPath{entityID: neighbor.Entity.ID, path: newPath, visited: visited})
				return true, nil
			}
			used, err := visitIndexedPathNeighbors(
				g,
				current.entityID,
				levelRequest,
				budget,
				processNeighbor,
			)
			if err != nil {
				return matched, err
			}
			if !used {
				neighbors, err := neighborsForBudget(
					g, current.entityID, levelRequest, budget,
				)
				if err != nil {
					return matched, err
				}
				for _, neighbor := range neighbors {
					keepGoing, err := processNeighbor(neighbor)
					if err != nil {
						return matched, err
					}
					if !keepGoing {
						break
					}
				}
			}
			if limitReached {
				return matched, nil
			}
		}
		queue = next
	}
	return matched, nil
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
