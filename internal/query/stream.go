package query

import (
	"context"

	"graphdb/internal/graph"
)

type StreamMeta struct {
	Version    int64            `json:"version,omitempty"`
	NextCursor string           `json:"next_cursor,omitempty"`
	Stats      *Stats           `json:"stats,omitempty"`
	Aggregates map[string]any   `json:"aggregates,omitempty"`
	Groups     []AggregateGroup `json:"groups,omitempty"`
	Plan       *Plan            `json:"plan,omitempty"`
	Profile    []OperatorStat   `json:"profile,omitempty"`
	Stream     bool             `json:"stream,omitempty"`
	Done       bool             `json:"done,omitempty"`
}

func StreamContextWithOptions(ctx context.Context, g *graph.Graph, request Request, options ExecuteOptions, emit func(any) error) (bool, error) {
	if !canStreamLazyMatch(request, options) {
		return false, nil
	}
	if request.Op == "profile" {
		target, err := targetRequest(request)
		if err != nil {
			return true, err
		}
		target.Profile = true
		request = target
	}
	if err := validateRequest(request); err != nil {
		return true, err
	}
	profiler := newProfiler(request.Profile)
	var plan Plan
	_ = profiler.measure("plan", request.Op, 0, func() (int, error) {
		plan = PlanQueryWithStats(g, request, options.PlannerStats)
		return len(plan.Steps), nil
	})
	budget, cancel := newBudget(ctx, request, profiler, options.IndexLookup, options.EntityLookup)
	defer cancel()
	if err := budget.measure("admission", "", plan.EstimatedCost, func() (int, error) {
		return 0, admitQuery(plan, budget)
	}); err != nil {
		return true, err
	}
	cursor, err := parseCursor(request.Cursor)
	if err != nil {
		return true, err
	}
	if err := validateCursor(cursor, g.Version, request); err != nil {
		return true, err
	}
	ids, ok, err := candidateIDsForPlan(plan, budget)
	if err != nil {
		return true, err
	}
	var results []Result
	var nextCursor string
	if ok {
		results, nextCursor, err = streamLazyMatch(g, request, cursor, budget, ids)
	} else if lazyKindScanAvailable(g, request, plan, budget) {
		var entities []graph.Entity
		entities, ok, err = lazyKindScanEntities(request, budget)
		if err == nil && !ok {
			err = ErrIndexUnavailable
		}
		if err == nil {
			results, nextCursor, err = streamLazyMatchEntities(g, request, cursor, budget, entities)
		}
	} else {
		return true, ErrIndexUnavailable
	}
	if err != nil {
		return true, err
	}
	if err := emit(StreamMeta{Version: g.Version, Plan: streamPlan(plan, request), Stream: true}); err != nil {
		return true, err
	}
	for _, result := range results {
		if err := emit(result); err != nil {
			return true, err
		}
	}
	stats := budget.stats()
	return true, emit(StreamMeta{
		Version:    g.Version,
		NextCursor: nextCursor,
		Stats:      &stats,
		Profile:    budget.profile(),
		Done:       true,
	})
}

func canStreamLazyMatch(request Request, options ExecuteOptions) bool {
	target := lazyTarget(request)
	return target.Op == "match" &&
		len(target.Sort) == 0 &&
		len(target.Aggregate) == 0 &&
		len(target.GroupBy) == 0 &&
		options.IndexLookup != nil &&
		options.EntityLookup != nil &&
		SupportsLazyRead(request, options.PlannerStats)
}

func streamLazyMatch(g *graph.Graph, request Request, cursor cursorState, budget *budget, ids []string) ([]Result, string, error) {
	limit := normalizedLimit(request.Limit)
	results := make([]Result, 0, limit)
	seen := 0
	pastCursor := cursor.After == ""
	cursorID, skipByID := cursorEntityID(cursor)
	last := ""
	for _, id := range ids {
		if skipByID && !pastCursor {
			if id != cursorID {
				continue
			}
			if err := confirmCursorEntity(g, id, request, cursor, budget); err != nil {
				return nil, "", err
			}
			pastCursor = true
			continue
		}
		entity, ok, err := materializeEntity(g, id, request, budget)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			return nil, "", ErrIndexUnavailable
		}
		if err := budget.add(1); err != nil {
			return nil, "", err
		}
		budget.scanned++
		if !requestEntityMatches(request, entity) {
			continue
		}
		result := Result{Entity: &entity}
		identity := resultIdentity(result)
		if cursor.Legacy && seen < cursor.Offset {
			seen++
			continue
		}
		if !pastCursor {
			pastCursor = identity == cursor.After
			continue
		}
		if len(results) >= limit {
			budget.returned = len(results)
			budget.truncated = last != ""
			return results, encodeCursor(cursorState{Version: g.Version, After: last, Query: cursorQueryHash(request)}), nil
		}
		applyProjection(&result, request.Project)
		results = append(results, result)
		last = identity
		seen++
	}
	if !cursor.Legacy && cursor.After != "" && !pastCursor {
		return nil, "", invalidCursorAfter(cursor)
	}
	budget.returned = len(results)
	return results, "", nil
}

func streamLazyMatchEntities(g *graph.Graph, request Request, cursor cursorState, budget *budget, entities []graph.Entity) ([]Result, string, error) {
	limit := normalizedLimit(request.Limit)
	results := make([]Result, 0, limit)
	state := newMatchPageState(cursor)
	for _, entity := range entities {
		stop, err := appendPageMatch(&results, entity, request, cursor, state, budget)
		if err != nil {
			return nil, "", err
		}
		if stop {
			next := ""
			if len(results) > 0 {
				next = encodeCursor(cursorState{Version: g.Version, After: resultIdentity(results[len(results)-1]), Query: cursorQueryHash(request)})
			}
			budget.returned = len(results)
			budget.truncated = next != ""
			return results, next, nil
		}
	}
	if err := validatePageCursor(cursor, state); err != nil {
		return nil, "", err
	}
	budget.returned = len(results)
	return results, "", nil
}

func candidateIDsForPlan(plan Plan, budget *budget) ([]string, bool, error) {
	switch plan.Strategy {
	case "entity-id":
		if len(plan.IndexValues) != 1 {
			return nil, true, nil
		}
		id, ok := plan.IndexValues[0].(string)
		if !ok {
			return nil, true, nil
		}
		return []string{id}, true, nil
	case "field-index":
		if budget.lookup == nil {
			return nil, false, nil
		}
		return budget.lookup.MatchFieldIndex(budget.ctx, planIndexKind(plan), plan.IndexField, plan.IndexValues)
	default:
		return nil, false, nil
	}
}

func planIndexKind(plan Plan) string {
	prefix := "field:"
	value := plan.Index
	if len(value) <= len(prefix) || value[:len(prefix)] != prefix {
		return ""
	}
	rest := value[len(prefix):]
	for i, ch := range rest {
		if ch == '.' {
			return rest[:i]
		}
	}
	return ""
}

func streamPlan(plan Plan, request Request) *Plan {
	if request.Profile {
		return &plan
	}
	return nil
}
