package query

import "graphdb/internal/graph"

func lazyKindScanAvailable(g *graph.Graph, request Request, plan Plan, budget *budget) bool {
	if !lazyExecution(g, budget) || plan.Strategy != "kind-scan" || request.Kind == "" || !canPageMatchEarly(request) {
		return false
	}
	_, ok := entityPageLookup(budget)
	return ok
}

func lazyKindScanEntities(request Request, budget *budget) ([]graph.Entity, bool, error) {
	pager, ok := entityPageLookup(budget)
	if !ok {
		return nil, false, nil
	}
	return pager.ListEntities(budget.ctx, request.Kind, materializeFields(request))
}

func entityPageLookup(budget *budget) (EntityPageLookup, bool) {
	if budget == nil || budget.entities == nil {
		return nil, false
	}
	pager, ok := budget.entities.(EntityPageLookup)
	return pager, ok
}
