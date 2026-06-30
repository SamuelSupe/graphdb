package query

import (
	"sort"

	"graphdb/internal/graph"
)

func materializeEntity(g *graph.Graph, id string, request Request, budget *budget) (graph.Entity, bool, error) {
	if entity, ok := g.GetEntity(id); ok {
		return entity, true, nil
	}
	if budget == nil || budget.entities == nil {
		return graph.Entity{}, false, nil
	}
	return budget.entities.GetEntity(budget.ctx, id, materializeFields(request))
}

func materializeEntities(g *graph.Graph, ids []string, request Request, budget *budget) ([]graph.Entity, error) {
	if lazyExecution(g, budget) {
		if batch, ok := budget.entities.(EntityBatchLookup); ok {
			entities, ok, err := batch.GetEntities(budget.ctx, ids, materializeFields(request))
			if err != nil {
				return nil, err
			}
			if ok {
				out := make([]graph.Entity, 0, len(ids))
				for _, id := range ids {
					if entity, ok := entities[id]; ok {
						out = append(out, entity)
					}
				}
				return out, nil
			}
			return nil, ErrIndexUnavailable
		}
	}
	entities := make([]graph.Entity, 0, len(ids))
	for _, id := range ids {
		if budget != nil {
			if err := budget.check(); err != nil {
				return nil, err
			}
		}
		entity, ok, err := materializeEntity(g, id, request, budget)
		if err != nil {
			return nil, err
		}
		if ok {
			entities = append(entities, entity)
		} else if lazyExecution(g, budget) {
			return nil, ErrIndexUnavailable
		}
	}
	return entities, nil
}

func lazyExecution(g *graph.Graph, budget *budget) bool {
	return g != nil && len(g.Entities) == 0 && budget != nil && budget.entities != nil
}

func materializeFields(request Request) []string {
	if pathResultQuery(request) {
		return nil
	}
	if len(request.Project) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, field := range request.Project {
		seen[field] = struct{}{}
	}
	for _, filter := range requestFilters(request) {
		seen[filter.Field] = struct{}{}
	}
	addFilterExprFields(seen, request.WhereExpr)
	for _, filter := range request.Path.EndWhere {
		seen[filter.Field] = struct{}{}
	}
	addFilterExprFields(seen, request.Path.EndWhereExpr)
	for _, step := range request.Path.Steps {
		for _, filter := range step.Where {
			seen[filter.Field] = struct{}{}
		}
		addFilterExprFields(seen, step.WhereExpr)
	}
	for _, sortSpec := range request.Sort {
		seen[sortSpec.Field] = struct{}{}
	}
	for _, aggregation := range request.Aggregate {
		if aggregation.Field != "" {
			seen[aggregation.Field] = struct{}{}
		}
	}
	for _, field := range request.GroupBy {
		seen[field] = struct{}{}
	}
	for _, filter := range request.Having {
		seen[filter.Field] = struct{}{}
	}
	addFilterExprFields(seen, request.HavingExpr)
	fields := make([]string, 0, len(seen))
	for field := range seen {
		if field != "" {
			fields = append(fields, field)
		}
	}
	sort.Strings(fields)
	return fields
}

func addFilterExprFields(seen map[string]struct{}, expr *FilterExpr) {
	if expr == nil {
		return
	}
	if expr.Field != "" {
		seen[expr.Field] = struct{}{}
	}
	for i := range expr.Children {
		addFilterExprFields(seen, &expr.Children[i])
	}
}

func pathResultQuery(request Request) bool {
	switch request.Op {
	case "traverse", "impact", "shortest_path":
		return true
	default:
		return false
	}
}
