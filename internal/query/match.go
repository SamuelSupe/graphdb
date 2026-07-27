package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func executeMatch(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	if lazyKindScanAvailable(g, request, plan, budget) {
		if canPageMatchEarly(request) &&
			cursor.Order != "" &&
			cursor.Order != lazyEntityPageOrder(budget) {
			return Response{}, ErrIndexUnavailable
		}
		return executeLazyKindScan(g, request, plan, cursor, budget)
	}
	if materializedKindPageAvailable(g, request, plan) {
		return executeMaterializedKindPage(g, request, plan, cursor, budget)
	}
	if canPageMatchEarly(request) {
		return executeMatchPage(g, request, plan, cursor, budget)
	}
	var entities []graph.Entity
	if err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		var err error
		entities, err = matchCandidatesForPlan(g, request, plan, budget)
		return len(entities), err
	}); err != nil {
		return Response{}, err
	}
	if canBuildBoundedMatchPage(request, cursor) {
		return executeBoundedMatchPage(g, request, entities, cursor, budget)
	}
	results := make([]Result, 0, len(entities))
	if err := budget.measure("filter-project", "", len(entities), func() (int, error) {
		for _, entity := range entities {
			if err := budget.add(1); err != nil {
				return len(results), err
			}
			budget.scanned++
			if !requestEntityMatches(request, entity) {
				continue
			}
			result := Result{Entity: &entity}
			results = append(results, result)
		}
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	if err := budget.check(); err != nil {
		return Response{}, err
	}
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		sortResults(results, request.Sort)
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	return buildResponse(g.Version, results, request, cursor, budget)
}

func canBuildBoundedMatchPage(request Request, cursor cursorState) bool {
	hasBoundedCursor := cursor.After == "" || cursor.Offset > 0
	return !cursor.Legacy && hasBoundedCursor &&
		(len(request.Sort) > 0 ||
			len(request.Aggregate) > 0 ||
			len(request.GroupBy) > 0)
}

func boundedMatchPageLimit(request Request, cursor cursorState) int {
	required := cursor.Offset + normalizedLimit(request.Limit) + 1
	if required < cursor.Offset || required > costResultLimit(request) {
		return costResultLimit(request)
	}
	return required
}

func executeBoundedMatchPage(g *graph.Graph, request Request, entities []graph.Entity, cursor cursorState, budget *budget) (Response, error) {
	acc := newAggregateAccumulator(request.Aggregate)
	groupAcc := newGroupAccumulator(request.GroupBy, request.Aggregate)
	keep := boundedMatchPageLimit(request, cursor)
	if len(request.Sort) == 0 {
		cursor.Order = matchPageOrder(cursor, EntityPageOrderIdentity)
		entities = orderEntities(entities, cursor.Order)
	}
	results := make([]Result, 0, keep)
	var sorted *boundedResults
	if len(request.Sort) > 0 {
		sorted = newBoundedResults(request.Sort, keep)
	}
	if err := budget.measure("filter-project", "", len(entities), func() (int, error) {
		for _, entity := range entities {
			if err := budget.add(1); err != nil {
				return len(results) + sorted.Len(), err
			}
			budget.scanned++
			if !requestEntityMatches(request, entity) {
				continue
			}
			result := Result{Entity: &entity}
			acc.add(result)
			groupAcc.add(result)
			if sorted != nil {
				sorted.Add(result)
				continue
			}
			if len(results) < keep {
				results = append(results, result)
			}
		}
		return len(results) + sorted.Len(), nil
	}); err != nil {
		return Response{}, err
	}
	if err := budget.check(); err != nil {
		return Response{}, err
	}
	if sorted != nil {
		results = sorted.Sorted()
	}
	return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
}

func matchOperatorName(plan Plan) string {
	switch plan.Strategy {
	case "entity-id":
		return "id-lookup"
	case "field-index":
		return "index-seek"
	case "field-index-scan":
		return "index-scan"
	default:
		return "entity-scan"
	}
}

func matchCandidatesForPlan(g *graph.Graph, request Request, plan Plan, budget *budget) ([]graph.Entity, error) {
	switch plan.Strategy {
	case "entity-id":
		if len(plan.IndexValues) != 1 {
			return nil, nil
		}
		id, ok := plan.IndexValues[0].(string)
		if !ok {
			return nil, nil
		}
		entity, ok, err := materializeEntity(g, id, request, budget)
		if err != nil {
			return nil, err
		}
		if !ok {
			if lazyExecution(g, budget) {
				return nil, ErrIndexUnavailable
			}
			return nil, nil
		}
		return []graph.Entity{entity}, nil
	case "field-index":
		if budget.lookup != nil {
			ids, ok, err := budget.lookup.MatchFieldIndex(budget.ctx, request.Kind, plan.IndexField, plan.IndexValues)
			if err != nil {
				return nil, err
			}
			if ok {
				return materializeEntities(g, ids, request, budget)
			}
			if lazyExecution(g, budget) {
				return nil, ErrIndexUnavailable
			}
		}
		return g.MatchFieldIndex(request.Kind, plan.IndexField, plan.IndexValues), nil
	case "field-index-scan":
		if budget.lookup != nil {
			if scanner, ok := budget.lookup.(FieldIndexScanLookup); ok {
				ids, ok, err := scanFieldIndexIDs(budget.ctx, scanner, request.Kind, plan.IndexField, requestFilters(request))
				if err != nil {
					return nil, err
				}
				if ok {
					return materializeEntities(g, ids, request, budget)
				}
			}
			if lazyExecution(g, budget) {
				return nil, ErrIndexUnavailable
			}
		}
		ids, err := scanRuntimeFieldIndexIDs(
			budget.ctx,
			g,
			request.Kind,
			plan.IndexField,
			requestFilters(request),
		)
		if err != nil {
			return nil, err
		}
		return materializeEntities(g, ids, request, budget)
	default:
		if lazyExecution(g, budget) {
			return nil, ErrIndexUnavailable
		}
		return g.MatchEntities(request.Kind, nil), nil
	}
}
