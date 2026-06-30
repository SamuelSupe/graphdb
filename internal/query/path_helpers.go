package query

import "gitlab.jiagouyun.com/guance/graphdb/internal/graph"

func pathMatches(path graph.Path, filter PathFilter) bool {
	if !pathStepsMatch(path, filter, true) {
		return false
	}
	if filter.EndKind != "" && pathEnd(path).Kind != filter.EndKind {
		return false
	}
	if len(filter.EndWhere) > 0 && !entityMatches(pathEnd(path), filter.EndWhere) {
		return false
	}
	if !entityExprMatches(pathEnd(path), filter.EndWhereExpr) {
		return false
	}
	if len(filter.NodeKinds) > 0 {
		allowed := map[string]struct{}{}
		for _, kind := range filter.NodeKinds {
			allowed[kind] = struct{}{}
		}
		for _, entity := range path.Entities {
			if _, ok := allowed[entity.Kind]; !ok {
				return false
			}
		}
	}
	return true
}

func pathPrefixMatches(path graph.Path, filter PathFilter, final bool) bool {
	if len(filter.NodeKinds) > 0 {
		for _, entity := range path.Entities {
			if !pathAllowsKind(entity.Kind, filter) {
				return false
			}
		}
	}
	if !pathStepsMatch(path, filter, final) {
		return false
	}
	if !final {
		return true
	}
	if filter.EndKind != "" && pathEnd(path).Kind != filter.EndKind {
		return false
	}
	if len(filter.EndWhere) > 0 && !entityMatches(pathEnd(path), filter.EndWhere) {
		return false
	}
	if !entityExprMatches(pathEnd(path), filter.EndWhereExpr) {
		return false
	}
	return true
}

func pathStepsMatch(path graph.Path, filter PathFilter, final bool) bool {
	if len(filter.Steps) == 0 {
		return true
	}
	for i, edge := range path.Edges {
		if i >= len(filter.Steps) {
			return !final
		}
		step := filter.Steps[i]
		if !relationAllowed(edge.Type, stringSet(step.RelationTypes)) {
			return false
		}
		if !edgeMatches(edge, step.EdgeWhere) || !edgeExprMatches(edge, step.EdgeWhereExpr) {
			return false
		}
		if i+1 >= len(path.Entities) {
			return false
		}
		entity := path.Entities[i+1]
		if len(step.NodeKinds) > 0 && !stringAllowed(entity.Kind, stringSet(step.NodeKinds)) {
			return false
		}
		if len(step.Where) > 0 && !entityMatches(entity, step.Where) {
			return false
		}
		if !entityExprMatches(entity, step.WhereExpr) {
			return false
		}
	}
	if final && len(path.Edges) < len(filter.Steps) {
		return false
	}
	return true
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func stringAllowed(value string, allowed map[string]struct{}) bool {
	if len(allowed) == 0 {
		return true
	}
	_, ok := allowed[value]
	return ok
}

func pathAllowsKind(kind string, filter PathFilter) bool {
	if len(filter.NodeKinds) == 0 {
		return true
	}
	for _, allowed := range filter.NodeKinds {
		if kind == allowed {
			return true
		}
	}
	return false
}

func pathResults(paths []graph.Path, projection []string) []Result {
	results := make([]Result, 0, len(paths))
	for _, path := range paths {
		path := path
		result := Result{Path: &path}
		if len(path.Entities) > 0 {
			entity := pathEnd(path)
			result.Entity = &entity
			applyProjection(&result, projection)
			result.Entity = nil
		}
		results = append(results, result)
	}
	return results
}

func maxPathResults(request Request) int {
	if request.Path.MaxPaths > 0 {
		if request.Path.MaxPaths > maxQueryLimit+1 {
			return maxQueryLimit + 1
		}
		return request.Path.MaxPaths
	}
	return normalizedLimit(request.Limit) + 1
}

func normalizedDepth(depth int) int {
	if depth <= 0 {
		return 1
	}
	if depth > 16 {
		return 16
	}
	return depth
}

type pendingPath struct {
	entityID string
	path     graph.Path
	visited  map[string]struct{}
}

func clonePath(path graph.Path) graph.Path {
	entities := append([]graph.Entity(nil), path.Entities...)
	edges := append([]graph.Edge(nil), path.Edges...)
	return graph.Path{Entities: entities, Edges: edges}
}

func copyVisited(visited map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(visited)+1)
	for key := range visited {
		out[key] = struct{}{}
	}
	return out
}
