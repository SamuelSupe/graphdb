package query

import "graphdb/internal/graph"

func lazyKindScanAvailable(g *graph.Graph, request Request, plan Plan, budget *budget) bool {
	if !lazyExecution(g, budget) || plan.Strategy != "kind-scan" || request.Kind == "" {
		return false
	}
	_, ok := entityPageLookup(budget)
	return ok
}

func visitLazyKindScanEntities(request Request, budget *budget, afterID string, visit func(graph.Entity) (bool, error)) (bool, error) {
	pager, ok := entityPageLookup(budget)
	if !ok {
		return false, nil
	}
	return pager.VisitEntities(budget.ctx, request.Kind, materializeFields(request), afterID, visit)
}

func entityPageLookup(budget *budget) (EntityPageLookup, bool) {
	if budget == nil || budget.entities == nil {
		return nil, false
	}
	pager, ok := budget.entities.(EntityPageLookup)
	return pager, ok
}
