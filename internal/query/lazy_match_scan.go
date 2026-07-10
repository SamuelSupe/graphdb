package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func executeLazyKindScan(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	if canPageMatchEarly(request) {
		return executeLazyKindPage(g, request, plan, cursor, budget)
	}
	return executeLazyKindMaterialized(g, request, plan, cursor, budget)
}

func executeLazyKindPage(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	afterID, _ := cursorEntityID(cursor)
	err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		startScanned := budget.scanned
		ok, err := visitLazyKindScanEntities(request, budget, afterID, func(entity graph.Entity) (bool, error) {
			stop, err := appendPageMatch(&results, entity, request, cursor, state, budget)
			return !stop, err
		})
		if err == nil && !ok {
			err = ErrIndexUnavailable
		}
		return budget.scanned - startScanned, err
	})
	if err != nil {
		return Response{}, err
	}
	if err := validatePageCursor(cursor, state); err != nil {
		return Response{}, err
	}
	return pageResponse(g.Version, results, state.hasNext, request, budget), nil
}

func executeLazyKindMaterialized(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	acc := newAggregateAccumulator(request.Aggregate)
	groupAcc := newGroupAccumulator(request.GroupBy, request.Aggregate)
	bounded := canBuildBoundedMatchPage(request, cursor)
	keep := normalizedLimit(request.Limit) + 1
	results := make([]Result, 0, keep)
	err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		startScanned := budget.scanned
		ok, err := visitLazyKindScanEntities(request, budget, "", func(entity graph.Entity) (bool, error) {
			if err := budget.add(1); err != nil {
				return false, err
			}
			budget.scanned++
			if !requestEntityMatches(request, entity) {
				return true, nil
			}
			result := Result{Entity: &entity}
			acc.add(result)
			groupAcc.add(result)
			if bounded && len(request.Sort) > 0 {
				results = appendBoundedSorted(results, result, request.Sort, keep)
			} else if !bounded || len(results) < keep {
				results = append(results, result)
			}
			return true, nil
		})
		if err == nil && !ok {
			err = ErrIndexUnavailable
		}
		return budget.scanned - startScanned, err
	})
	if err != nil {
		return Response{}, err
	}
	if bounded {
		return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
	}
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		sortResults(results, request.Sort)
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
}
