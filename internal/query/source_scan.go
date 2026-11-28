package query

import "github.com/SamuelSupe/graphdb/v2/internal/graph"

// EntityOrderComparator returns a canonical comparator for entity IDs under
// order (EntityPageOrderIdentity or EntityPageOrderShard).
func EntityOrderComparator(order string) func(left, right string) int {
	return func(left, right string) int {
		return compareEntityPageOrder(left, right, order)
	}
}

// scanSourceEntities is the single source-entity iteration adapter shared by
// operators that start from entity scans (such as pattern and match). It
// enforces requestEntityMatches AND budget.allowsEntity (EntityPredicate) in
// one place so individual operators cannot accidentally bypass predicates.
func scanSourceEntities(
	g *graph.Graph,
	request Request,
	plan Plan,
	budget *budget,
	afterID string,
	borrowStart bool,
	visit func(graph.Entity) (bool, error),
) error {
	filtered := func(entity graph.Entity) (bool, error) {
		if err := budget.add(1); err != nil {
			return false, err
		}
		budget.scanned++
		if !requestEntityMatches(&request, &entity) || !budget.allowsEntity(entity) {
			return true, nil
		}
		return visit(entity)
	}
	if lazyKindScanAvailable(g, request, plan, budget) {
		ok, err := visitLazyKindScanEntities(request, budget, afterID, filtered)
		if err != nil {
			return err
		}
		if !ok {
			return ErrIndexUnavailable
		}
		return nil
	}
	if borrowStart {
		return g.VisitEntitiesByID(request.Kind, afterID, filtered)
	}
	starts, err := matchCandidatesForPlan(g, request, plan, budget)
	if err != nil {
		return err
	}
	for _, entity := range starts {
		more, err := filtered(entity)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}
