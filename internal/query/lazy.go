package query

func SupportsLazyRead(request Request, stats PlannerStats) bool {
	if len(stats.EntityPages) == 0 {
		return false
	}
	target := lazyTarget(request)
	switch target.Op {
	case "match":
		return lazyMatchSupported(target, stats)
	case "pattern":
		return lazyMatchSupported(target, stats) && lazyPatternDirectionsSupported(target, stats)
	case "neighbors", "traverse", "impact", "shortest_path":
		if target.Op == "impact" && target.Direction != "out" {
			return false
		}
		return target.ID != "" && lazyDirectionSupported(target.Direction, stats)
	default:
		return false
	}
}

func lazyPatternDirectionsSupported(request Request, stats PlannerStats) bool {
	direction := request.Direction
	if direction == "" {
		direction = "out"
	}
	for _, step := range request.Path.Steps {
		stepDirection := step.Direction
		if stepDirection == "" {
			stepDirection = direction
		}
		if !lazyDirectionSupported(stepDirection, stats) {
			return false
		}
	}
	return true
}

func lazyDirectionSupported(direction string, stats PlannerStats) bool {
	switch direction {
	case "out":
		return len(stats.EdgeShards) > 0
	case "in":
		return len(stats.ReverseEdgeShards) > 0
	case "both", "":
		return len(stats.EdgeShards) > 0 && len(stats.ReverseEdgeShards) > 0
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
