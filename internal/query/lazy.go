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
		return lazyMatchSupported(target, stats) &&
			lazyPathDirectionsSupported(target, stats)
	case "traverse", "shortest_path":
		return target.ID != "" &&
			lazyPathDirectionsSupported(target, stats)
	case "neighbors":
		return target.ID != "" && lazyDirectionSupported(target.Direction, stats)
	case "impact":
		return lazyImpactSupported(target, stats)
	default:
		return false
	}
}

func RequiresReverseIndex(request Request) bool {
	target := lazyTarget(request)
	if target.Op == "impact" || target.DirectionStrategy == "impact" {
		return true
	}
	switch target.Op {
	case "match":
		return false
	case "neighbors":
		return target.Direction != "out"
	case "pattern":
		target.Depth = len(target.Path.Steps)
		if target.Direction == "" {
			target.Direction = "out"
		}
	case "traverse", "shortest_path":
	default:
		return false
	}
	for level := 0; level < normalizedDepth(target.Depth); level++ {
		if requestForPathLevel(target, level).Direction != "out" {
			return true
		}
	}
	return false
}

func lazyPathDirectionsSupported(request Request, stats PlannerStats) bool {
	direction := request.Direction
	if direction == "" && request.Op == "pattern" {
		direction = "out"
	}
	if len(request.Path.Steps) == 0 {
		return lazyDirectionSupported(direction, stats)
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
		return forwardEdgeIndexAvailable(stats)
	case "in":
		return reverseEdgeIndexAvailable(stats)
	case "both", "":
		return forwardEdgeIndexAvailable(stats) &&
			reverseEdgeIndexAvailable(stats)
	default:
		return false
	}
}

func forwardEdgeIndexAvailable(stats PlannerStats) bool {
	return stats.ForwardEdgeIndexAvailable || len(stats.EdgeShards) > 0
}

func reverseEdgeIndexAvailable(stats PlannerStats) bool {
	return stats.ReverseEdgeIndexAvailable ||
		len(stats.ReverseEdgeShards) > 0
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
