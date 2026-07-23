package query

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func executePattern(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	traversal := request
	traversal.Depth = len(request.Path.Steps)
	if traversal.Direction == "" {
		traversal.Direction = "out"
	}
	collectionLimit := patternCollectionLimit(request)
	paths := make([]graph.Path, 0, collectionLimit)
	processStart := func(entity graph.Entity) (bool, error) {
		if err := budget.add(1); err != nil {
			return false, err
		}
		budget.scanned++
		if !requestEntityMatches(request, entity) {
			return true, nil
		}
		remaining := collectionLimit - len(paths)
		if remaining <= 0 {
			return false, nil
		}
		matched, err := collectPaths(g, entity, traversal, remaining, budget)
		if err != nil {
			return false, err
		}
		paths = append(paths, matched...)
		return len(paths) < collectionLimit, nil
	}

	if lazyKindScanAvailable(g, request, plan, budget) {
		ok, err := visitLazyKindScanEntities(request, budget, "", processStart)
		if err != nil {
			return Response{}, err
		}
		if !ok {
			return Response{}, ErrIndexUnavailable
		}
	} else {
		starts, err := matchCandidatesForPlan(g, request, plan, budget)
		if err != nil {
			return Response{}, err
		}
		for _, entity := range starts {
			more, err := processStart(entity)
			if err != nil {
				return Response{}, err
			}
			if !more {
				break
			}
		}
	}

	results := pathResults(paths, request.Project)
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		sortResults(results, request.Sort)
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	response, err := buildResponse(g.Version, results, request, cursor, budget)
	if err != nil {
		return Response{}, fmt.Errorf("build pattern response: %w", err)
	}
	return response, nil
}

func patternCollectionLimit(request Request) int {
	if request.Path.MaxPaths > 0 {
		return maxPathResults(request)
	}
	return maxQueryLimit + 1
}
