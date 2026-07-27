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
	complete := requiresCompletePathScan(request) ||
		plan.Strategy != "entity-id"
	collectionLimit := patternCollectionLimit(request)
	if complete {
		collectionLimit = completePathScanLimit(request)
	}
	paths := make([]graph.Path, 0, min(collectionLimit, maxQueryLimit+1))
	var collector *pathResultCollector
	if complete {
		collector = newPathResultCollector(request)
	}
	emit := func(path graph.Path) error {
		if collector != nil {
			return collector.add(path)
		}
		paths = append(paths, path)
		return nil
	}
	matchedTotal := 0
	processStart := func(entity graph.Entity) (bool, error) {
		if err := budget.add(1); err != nil {
			return false, err
		}
		budget.scanned++
		if !requestEntityMatches(request, entity) {
			return true, nil
		}
		remaining := 0
		if collectionLimit > 0 {
			remaining = collectionLimit - matchedTotal
		}
		if collectionLimit > 0 && remaining <= 0 {
			return false, nil
		}
		matched, err := visitPaths(
			g, entity, traversal, remaining, budget, emit,
		)
		if err != nil {
			return false, err
		}
		matchedTotal += matched
		return collectionLimit == 0 || matchedTotal < collectionLimit, nil
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

	var results []Result
	sortCost := len(paths)
	if collector != nil {
		sortCost = collector.count
	}
	if err := budget.measure("sort", "", sortCost, func() (int, error) {
		if collector != nil {
			results = collector.results()
		} else {
			results = pathResults(paths, request.Project)
			sortResults(results, request.Sort)
		}
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	var response Response
	var err error
	if collector != nil {
		response, err = buildResponseWithAggregatesAndGroups(
			g.Version,
			results,
			request,
			cursor,
			budget,
			collector.aggregates(),
			collector.aggregateGroups(),
		)
	} else {
		response, err = buildResponse(
			g.Version, results, request, cursor, budget,
		)
	}
	if err != nil {
		return Response{}, fmt.Errorf("build pattern response: %w", err)
	}
	return response, nil
}

func patternCollectionLimit(request Request) int {
	return maxPathResults(request)
}
