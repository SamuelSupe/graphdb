package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

type matchPageState struct {
	pastCursor    bool
	legacySkipped int
	hasNext       bool
}

func canPageMatchEarly(request Request) bool {
	return len(request.Sort) == 0 && len(request.Aggregate) == 0 && len(request.GroupBy) == 0
}

func executeMatchPage(g *graph.Graph, request Request, plan Plan, cursor cursorState, budget *budget) (Response, error) {
	ids, ok, err := measuredCandidateIDs(g, request, plan, budget)
	if err != nil {
		return Response{}, err
	}
	if ok {
		return pageMatchedIDs(g, request, cursor, budget, ids)
	}
	entities, err := measuredCandidateEntities(g, request, plan, budget)
	if err != nil {
		return Response{}, err
	}
	return pageMatchedEntities(g, request, cursor, budget, entities)
}

func measuredCandidateIDs(g *graph.Graph, request Request, plan Plan, budget *budget) ([]string, bool, error) {
	var ids []string
	var ok bool
	err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		var err error
		ids, ok, err = candidateIDsForPlan(plan, budget)
		if ok || err != nil {
			return len(ids), err
		}
		if lazyExecution(g, budget) {
			return 0, ErrIndexUnavailable
		}
		ids, ok, err = runtimeCandidateIDs(g, request, plan, budget)
		return len(ids), err
	})
	return ids, ok, err
}

func measuredCandidateEntities(g *graph.Graph, request Request, plan Plan, budget *budget) ([]graph.Entity, error) {
	var entities []graph.Entity
	err := budget.measure(matchOperatorName(plan), plan.Index, plan.EstimatedCost, func() (int, error) {
		var err error
		entities, err = matchCandidatesForPlan(g, request, plan, budget)
		return len(entities), err
	})
	return entities, err
}

func runtimeCandidateIDs(
	g *graph.Graph,
	request Request,
	plan Plan,
	budget *budget,
) ([]string, bool, error) {
	switch plan.Strategy {
	case "field-index":
		return g.MatchFieldIndexIDs(request.Kind, plan.IndexField, plan.IndexValues), true, nil
	case "field-index-scan":
		ids, err := scanRuntimeFieldIndexIDs(
			budget.ctx,
			g,
			request.Kind,
			plan.IndexField,
			requestFilters(request),
		)
		return ids, true, err
	case "kind-scan":
		return g.MatchEntityIDs(request.Kind), true, nil
	default:
		return nil, false, nil
	}
}

func pageMatchedIDs(g *graph.Graph, request Request, cursor cursorState, budget *budget, ids []string) (Response, error) {
	order := matchPageOrder(cursor, EntityPageOrderIdentity)
	ids = orderEntityIDs(ids, order)
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	cursorID, skipByID := cursorEntityID(cursor)
	err := budget.measure("filter-project", "", len(ids), func() (int, error) {
		startScanned := budget.scanned
		for _, id := range ids {
			if skipByID && !state.pastCursor {
				if id != cursorID {
					continue
				}
				if err := confirmCursorEntity(g, id, request, cursor, budget); err != nil {
					return budget.scanned - startScanned, err
				}
				state.pastCursor = true
				continue
			}
			entity, ok, err := materializeEntity(g, id, request, budget)
			if err != nil {
				return budget.scanned - startScanned, err
			}
			if !ok {
				if lazyExecution(g, budget) {
					return budget.scanned - startScanned, ErrIndexUnavailable
				}
				continue
			}
			stop, err := appendPageMatch(&results, entity, request, cursor, state, budget)
			if err != nil || stop {
				return budget.scanned - startScanned, err
			}
		}
		return budget.scanned - startScanned, nil
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

func confirmCursorEntity(g *graph.Graph, id string, request Request, cursor cursorState, budget *budget) error {
	entity, ok, err := materializeEntity(g, id, request, budget)
	if err != nil {
		return err
	}
	if !ok {
		if lazyExecution(g, budget) {
			return ErrIndexUnavailable
		}
		return invalidCursorAfter(cursor)
	}
	if err := budget.add(1); err != nil {
		return err
	}
	budget.scanned++
	if !requestEntityMatches(request, entity) {
		return invalidCursorAfter(cursor)
	}
	return nil
}

func pageMatchedEntities(g *graph.Graph, request Request, cursor cursorState, budget *budget, entities []graph.Entity) (Response, error) {
	order := matchPageOrder(cursor, EntityPageOrderIdentity)
	entities = orderEntities(entities, order)
	results := make([]Result, 0, normalizedLimit(request.Limit))
	state := newMatchPageState(cursor)
	err := budget.measure("filter-project", "", len(entities), func() (int, error) {
		startScanned := budget.scanned
		for _, entity := range entities {
			stop, err := appendPageMatch(&results, entity, request, cursor, state, budget)
			if err != nil || stop {
				return budget.scanned - startScanned, err
			}
		}
		return budget.scanned - startScanned, nil
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

func newMatchPageState(cursor cursorState) *matchPageState {
	return &matchPageState{pastCursor: cursor.After == ""}
}

func appendPageMatch(results *[]Result, entity graph.Entity, request Request, cursor cursorState, state *matchPageState, budget *budget) (bool, error) {
	if err := budget.add(1); err != nil {
		return true, err
	}
	budget.scanned++
	if !requestEntityMatches(request, entity) {
		return false, nil
	}
	result := Result{Entity: &entity}
	identity := resultIdentity(result)
	if skipCursorResult(identity, cursor, state) {
		return false, nil
	}
	if len(*results) >= normalizedLimit(request.Limit) {
		state.hasNext = true
		return true, nil
	}
	applyProjection(&result, request.Project)
	*results = append(*results, result)
	return false, nil
}

func skipCursorResult(identity string, cursor cursorState, state *matchPageState) bool {
	if cursor.Legacy && state.legacySkipped < cursor.Offset {
		state.legacySkipped++
		return true
	}
	if !cursor.Legacy && !state.pastCursor {
		if identity == cursor.After {
			state.pastCursor = true
		}
		return true
	}
	return false
}

func validatePageCursor(cursor cursorState, state *matchPageState) error {
	if cursor.Legacy || cursor.After == "" || state.pastCursor {
		return nil
	}
	return invalidCursorAfter(cursor)
}

func pageResponse(version int64, results []Result, hasNext bool, request Request, budget *budget) Response {
	return pageResponseWithOrder(
		version, results, hasNext, request, budget, "",
	)
}

func matchPageResponse(
	version int64,
	results []Result,
	hasNext bool,
	request Request,
	budget *budget,
	order string,
) Response {
	return pageResponseWithOrder(
		version, results, hasNext, request, budget, order,
	)
}

func pageResponseWithOrder(
	version int64,
	results []Result,
	hasNext bool,
	request Request,
	budget *budget,
	order string,
) Response {
	next := ""
	if hasNext && len(results) > 0 {
		next = encodeCursor(cursorState{
			Version: version,
			After:   resultIdentity(results[len(results)-1]),
			Query:   cursorQueryHash(request),
			Order:   order,
		})
	}
	budget.returned = len(results)
	budget.truncated = next != ""
	return Response{
		Version:    version,
		Results:    results,
		NextCursor: next,
		Stats:      budget.stats(),
		Profile:    budget.profile(),
	}
}
