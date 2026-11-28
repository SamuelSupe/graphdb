package query

import (
	"container/heap"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func executeLazyKindScan(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	if canPageMatchEarly(request) {
		return executeLazyKindPage(g, request, plan, cursor, budget)
	}
	return executeLazyKindMaterialized(g, request, plan, cursor, budget)
}

func executeLazyKindPage(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	afterID, hasAfter := cursorEntityID(cursor)
	if !cursor.Legacy && cursor.After != "" && !hasAfter {
		return Response{}, invalidCursorAfter(cursor)
	}
	order := lazyEntityPageOrder(budget)
	err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		startScanned := budget.scanned
		ok, err := visitLazyKindScanEntities(request, budget, afterID, func(entity graph.Entity) (bool, error) {
			if hasAfter && !state.pastCursor && entity.ID != afterID {
				return false, invalidCursorAfter(cursor)
			}
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
	return matchPageResponse(
		g.Version, results, state.hasNext, request, budget, order,
	), nil
}

func executeLazyKindMaterialized(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	acc, groupAcc := newQueryAggregators(request)
	if len(request.Sort) == 0 {
		cursor.Order = matchPageOrder(cursor, EntityPageOrderIdentity)
	}
	shardOrder := len(request.Sort) == 0 && cursor.Order == EntityPageOrderShard
	bounded := canBuildBoundedMatchPage(request, cursor)
	keep := boundedMatchPageLimit(request, cursor)
	var results []Result
	var sorted *boundedResults
	var shardBounded *boundedShardResults
	if bounded && shardOrder {
		shardBounded = &boundedShardResults{
			values: make([]Result, 0, boundedInitialCap(keep)),
			order:  cursor.Order,
			limit:  keep,
		}
	} else if bounded {
		sorted = newBoundedResults(request.Sort, keep)
	} else {
		results = make([]Result, 0, boundedInitialCap(keep))
	}
	err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		startScanned := budget.scanned
		ok, err := visitLazyKindScanEntities(request, budget, "", func(entity graph.Entity) (bool, error) {
			if err := budget.add(1); err != nil {
				return false, err
			}
			budget.scanned++
			if !requestEntityMatches(&request, &entity) {
				return true, nil
			}
			if budget.entityPredicate != nil && !budget.entityPredicate(entity) {
				return true, nil
			}
			result := Result{Entity: &entity}
			if err := acc.add(result); err != nil {
				return false, err
			}
			if err := groupAcc.add(result); err != nil {
				return false, err
			}
			if shardBounded != nil {
				shardBounded.Add(result)
			} else if sorted != nil {
				sorted.Add(result)
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
		if shardBounded != nil {
			results = orderResults(shardBounded.values, cursor.Order)
		} else if sorted != nil {
			results = sorted.Sorted()
		}
		return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
	}
	if err := budget.measure("sort", "", len(results), func() (int, error) {
		if shardOrder {
			results = orderResults(results, cursor.Order)
		} else {
			sortResults(results, request.Sort)
		}
		return len(results), nil
	}); err != nil {
		return Response{}, err
	}
	return buildResponseWithAggregatesAndGroups(g.Version, results, request, cursor, budget, acc.results(), groupAcc.results(request.Having, request.HavingExpr))
}

type boundedShardResults struct {
	values []Result
	order  string
	limit  int
}

func (b *boundedShardResults) Add(result Result) {
	if b.limit <= 0 {
		return
	}
	if len(b.values) < b.limit {
		heap.Push(b, result)
		return
	}
	if compareEntityPageOrder(result.Entity.ID, b.values[0].Entity.ID, b.order) >= 0 {
		return
	}
	b.values[0] = result
	heap.Fix(b, 0)
}

func (b boundedShardResults) Len() int { return len(b.values) }

func (b boundedShardResults) Less(i, j int) bool {
	return compareEntityPageOrder(b.values[i].Entity.ID, b.values[j].Entity.ID, b.order) > 0
}

func (b boundedShardResults) Swap(i, j int) {
	b.values[i], b.values[j] = b.values[j], b.values[i]
}

func (b *boundedShardResults) Push(value any) {
	b.values = append(b.values, value.(Result))
}

func (b *boundedShardResults) Pop() any {
	last := len(b.values) - 1
	value := b.values[last]
	b.values[last] = Result{}
	b.values = b.values[:last]
	return value
}
