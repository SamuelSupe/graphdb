package query

import (
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
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
		if plan.Strategy != "entity-id" && request.Path.MaxPaths > 0 {
			// When scanning multiple start entities (possibly in non-canonical
			// shard order), do not short-circuit before scanning all starts;
			// pathResultCollector keeps the deterministic top-MaxPaths results.
			collectionLimit = 0
		} else {
			collectionLimit = completePathScanLimit(request)
		}
	}
	paths := make([]graph.Path, 0, min(collectionLimit, maxQueryLimit+1))
	var collector *pathResultCollector
	if complete {
		collector = newPathResultCollector(request)
	}
	borrowStart := plan.Strategy == "kind-scan" && !lazyExecution(g, budget)
	emit := func(path graph.Path) error {
		if borrowStart {
			path.Entities[0] = graph.CopyEntity(path.Entities[0])
		}
		if collector != nil {
			return collector.add(path)
		}
		paths = append(paths, path)
		return nil
	}
	matchedTotal := 0
	processStart := func(entity graph.Entity) (bool, error) {
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

	if err := scanSourceEntities(g, request, plan, budget, "", borrowStart, processStart); err != nil {
		return Response{}, err
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
