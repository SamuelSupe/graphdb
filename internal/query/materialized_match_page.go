package query

import (
	"container/heap"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func materializedKindPageAvailable(
	g *graph.Graph,
	request Request,
	plan Plan,
) bool {
	return len(g.Entities) > 0 &&
		plan.Strategy == "kind-scan" &&
		canPageMatchEarly(request)
}

func executeMaterializedKindPage(
	g *graph.Graph,
	request Request,
	plan Plan,
	cursor cursorState,
	budget *budget,
) (Response, error) {
	afterID, err := validateMaterializedMatchCursor(g, request, cursor)
	if err != nil {
		return Response{}, err
	}
	order := matchPageOrder(cursor, EntityPageOrderIdentity)
	keep, start := materializedMatchWindow(request, cursor)
	ids := maxEntityIDHeap{
		values: make([]string, 0, keep),
		order:  order,
	}
	err = budget.measure(
		matchOperatorName(plan),
		plan.Index,
		plan.EstimatedCost,
		func() (int, error) {
			err := budget.measure(
				"filter-project",
				"",
				len(g.Entities),
				func() (int, error) {
					for _, entity := range g.Entities {
						if err := budget.add(1); err != nil {
							return ids.Len(), err
						}
						budget.scanned++
						if (request.Kind != "" && entity.Kind != request.Kind) ||
							!requestEntityMatches(request, entity) ||
							(afterID != "" &&
								compareEntityPageOrder(entity.ID, afterID, order) <= 0) {
							continue
						}
						addBoundedEntityID(&ids, entity.ID, keep)
					}
					return ids.Len(), nil
				},
			)
			if err != nil {
				return ids.Len(), err
			}
			return ids.Len(), nil
		},
	)
	if err != nil {
		return Response{}, err
	}
	sort.Slice(ids.values, func(i, j int) bool {
		return compareEntityPageOrder(
			ids.values[i], ids.values[j], order,
		) < 0
	})
	start = min(start, ids.Len())
	end := min(start+normalizedLimit(request.Limit), ids.Len())
	results := make([]Result, 0, end-start)
	for _, id := range ids.values[start:end] {
		entity, ok := g.GetEntity(id)
		if !ok {
			continue
		}
		result := Result{Entity: &entity}
		applyProjection(&result, request.Project)
		results = append(results, result)
	}
	return matchPageResponse(
		g.Version, results, end < ids.Len(), request, budget, order,
	), nil
}

func validateMaterializedMatchCursor(
	g *graph.Graph,
	request Request,
	cursor cursorState,
) (string, error) {
	if cursor.Legacy || cursor.After == "" {
		return "", nil
	}
	id, ok := cursorEntityID(cursor)
	if !ok {
		return "", invalidCursorAfter(cursor)
	}
	entity, ok := g.Entities[id]
	if !ok ||
		(request.Kind != "" && entity.Kind != request.Kind) ||
		!requestEntityMatches(request, entity) {
		return "", invalidCursorAfter(cursor)
	}
	return id, nil
}

func materializedMatchWindow(request Request, cursor cursorState) (keep int, start int) {
	keep = normalizedLimit(request.Limit) + 1
	if !cursor.Legacy {
		return keep, 0
	}
	start = cursor.Offset
	required := start + keep
	if required < start || required > costResultLimit(request) {
		required = costResultLimit(request)
	}
	return required, start
}

type maxEntityIDHeap struct {
	values []string
	order  string
}

func addBoundedEntityID(ids *maxEntityIDHeap, id string, keep int) {
	if keep <= 0 {
		return
	}
	if ids.Len() < keep {
		heap.Push(ids, id)
		return
	}
	if compareEntityPageOrder(id, ids.values[0], ids.order) >= 0 {
		return
	}
	ids.values[0] = id
	heap.Fix(ids, 0)
}

func (ids maxEntityIDHeap) Len() int { return len(ids.values) }

func (ids maxEntityIDHeap) Less(i, j int) bool {
	return compareEntityPageOrder(
		ids.values[i], ids.values[j], ids.order,
	) > 0
}

func (ids maxEntityIDHeap) Swap(i, j int) {
	ids.values[i], ids.values[j] = ids.values[j], ids.values[i]
}

func (ids *maxEntityIDHeap) Push(value any) {
	ids.values = append(ids.values, value.(string))
}

func (ids *maxEntityIDHeap) Pop() any {
	values := ids.values
	last := len(values) - 1
	value := values[last]
	values[last] = ""
	ids.values = values[:last]
	return value
}
