package query

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
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
		if lazyExecution(g, budget) && !budget.authoritativeEntities {
			return Response{}, ErrMaterializedFallback
		}
		return Response{}, fmt.Errorf("%w: entity %q not found", ErrInvalid, request.ID)
	}
	if !budget.allowsEntity(start) {
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
			if request.Path.MaxPaths > 0 {
				return visitPathsInIdentityOrder(
					g, start, request, request.Path.MaxPaths,
					budget, collector.add,
				)
			}
			return visitPaths(
				g, start, request, 0,
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

const maxTraverseFrontier = 65536

type bfsPathNode struct {
	parent *bfsPathNode
	entity graph.Entity
	edge   *graph.Edge
	depth  int
}

func (n *bfsPathNode) contains(id string) bool {
	for cur := n; cur != nil; cur = cur.parent {
		if cur.entity.ID == id {
			return true
		}
	}
	return false
}

func (n *bfsPathNode) toPath() graph.Path {
	entities := make([]graph.Entity, n.depth+1)
	var edges []graph.Edge
	if n.depth > 0 {
		edges = make([]graph.Edge, n.depth)
	}
	for cur := n; cur != nil; cur = cur.parent {
		entities[cur.depth] = cur.entity
		if cur.edge != nil {
			edges[cur.depth-1] = *cur.edge
		}
	}
	return graph.Path{Entities: entities, Edges: edges}
}

func bfsStepMatches(edge graph.Edge, entity graph.Entity, filter PathFilter, stepIdx int) bool {
	if len(filter.NodeKinds) > 0 && !pathAllowsKind(entity.Kind, filter) {
		return false
	}
	if len(filter.Steps) == 0 {
		return true
	}
	if stepIdx >= len(filter.Steps) {
		return false
	}
	step := filter.Steps[stepIdx]
	if !stringSliceAllows(edge.Type, step.RelationTypes) {
		return false
	}
	if !edgeMatches(edge, step.EdgeWhere) || !edgeExprMatches(edge, step.EdgeWhereExpr) {
		return false
	}
	if !stringSliceAllows(entity.Kind, step.NodeKinds) {
		return false
	}
	if len(step.Where) > 0 && !entityMatches(&entity, step.Where) {
		return false
	}
	if !entityExprMatches(&entity, step.WhereExpr) {
		return false
	}
	return true
}

func visitPaths(
	g *graph.Graph,
	start graph.Entity,
	request Request,
	maxResults int,
	budget *budget,
	visit func(graph.Path) error,
) (int, error) {
	if len(request.Path.NodeKinds) > 0 && !pathAllowsKind(start.Kind, request.Path) {
		return 0, nil
	}
	queue := []*bfsPathNode{{entity: start, depth: 0}}
	matched := 0
	depth := normalizedDepth(request.Depth)
	if len(request.Path.Steps) > 0 {
		if depth < len(request.Path.Steps) {
			return 0, nil
		}
		depth = len(request.Path.Steps)
	}
	for level := 0; level < depth && len(queue) > 0; level++ {
		if err := budget.check(); err != nil {
			return matched, err
		}
		next := make([]*bfsPathNode, 0)
		finalLevel := level+1 == depth
		for _, current := range queue {
			levelRequest := requestForPathLevel(request, level)
			limitReached := false
			processNeighbor := func(
				neighbor graph.Neighbor,
			) (bool, error) {
				budget.visited++
				if current.contains(neighbor.Entity.ID) {
					return true, nil
				}
				if !budget.allowsEntity(neighbor.Entity) {
					return true, nil
				}
				if !bfsStepMatches(neighbor.Edge, neighbor.Entity, request.Path, level) {
					return true, nil
				}
				edgeCopy := neighbor.Edge
				node := &bfsPathNode{
					parent: current,
					entity: neighbor.Entity,
					edge:   &edgeCopy,
					depth:  current.depth + 1,
				}
				if len(request.Path.Steps) == 0 || node.depth == len(request.Path.Steps) {
					end := node.entity
					endOK := (request.Path.EndKind == "" || end.Kind == request.Path.EndKind) &&
						(len(request.Path.EndWhere) == 0 || entityMatches(&end, request.Path.EndWhere)) &&
						entityExprMatches(&end, request.Path.EndWhereExpr)
					if endOK {
						if err := visit(node.toPath()); err != nil {
							return false, err
						}
						matched++
						if maxResults > 0 && matched >= maxResults {
							limitReached = true
							return false, nil
						}
					}
				}
				if finalLevel {
					return true, nil
				}
				if len(next) >= maxTraverseFrontier {
					return false, fmt.Errorf("%w: traversal frontier limit %d exceeded", ErrLimitExceeded, maxTraverseFrontier)
				}
				next = append(next, node)
				return true, nil
			}
			used, err := visitIndexedPathNeighbors(
				g,
				current.entity.ID,
				levelRequest,
				budget,
				processNeighbor,
			)
			if err != nil {
				return matched, err
			}
			if !used {
				neighbors, err := neighborsForBudget(
					g, current.entity.ID, levelRequest, budget,
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
