package query

func SupportsLazyRead(request Request, stats PlannerStats) bool {
	if len(stats.EntityPages) == 0 {
		return false
	}
	target := lazyTarget(request)
	switch target.Op {
	case "match":
		return lazyMatchSupported(target, stats)
	case "neighbors", "traverse", "impact", "shortest_path":
		return target.Direction == "out" && target.ID != "" && len(stats.EdgeShards) > 0
	default:
		return false
	}
}

func lazyTarget(request Request) Request {
	if request.Op == "profile" || request.Op == "explain" {
		target, err := targetRequest(request)
		if err == nil {
			if request.Op == "profile" {
				target.Profile = true
			}
			return target
		}
	}
	return request
}

func lazyMatchSupported(request Request, stats PlannerStats) bool {
	if request.Kind != "" {
		return true
	}
	filters := requestFilters(request)
	if _, ok := idEquality(filters); ok {
		return true
	}
	for _, filter := range filters {
		op := filter.Op
		if op == "" {
			op = "eq"
		}
		if op != "eq" && op != "in" {
			continue
		}
		if !indexableFilterValues(filterValues(filter)) {
			continue
		}
		field, ok := indexedField(filter.Field)
		if ok && request.Kind != "" && statsHasField(stats, request.Kind, field) {
			return true
		}
	}
	return false
}
