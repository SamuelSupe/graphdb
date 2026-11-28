package query

// lazyOperatorCapability binds an operator's admission predicate (CanExecute)
// and reverse-index requirement (RequiresReverse) to the same descriptor so
// planner admission and physical lazy execution stay 100% aligned.
type lazyOperatorCapability struct {
	Op              string
	CanExecute      func(request Request, stats PlannerStats) bool
	RequiresReverse func(request Request) bool
}

var lazyOperatorCapabilities = map[string]lazyOperatorCapability{
	"match": {
		Op:              "match",
		CanExecute:      lazyMatchSupported,
		RequiresReverse: func(Request) bool { return false },
	},
	"pattern": {
		Op: "pattern",
		CanExecute: func(target Request, stats PlannerStats) bool {
			if !lazyMatchSupported(target, stats) {
				return false
			}
			if target.DirectionStrategy == "impact" {
				return lazyImpactSupportedForDepth(target, len(target.Path.Steps), stats)
			}
			return lazyPathDirectionsSupported(target, stats)
		},
		RequiresReverse: func(target Request) bool {
			if target.DirectionStrategy == "impact" {
				return true
			}
			target.Depth = len(target.Path.Steps)
			if target.Direction == "" {
				target.Direction = "out"
			}
			return pathLevelsRequireReverse(target, normalizedDepth(target.Depth))
		},
	},
	"traverse": {
		Op: "traverse",
		CanExecute: func(target Request, stats PlannerStats) bool {
			if target.DirectionStrategy == "impact" {
				return lazyImpactSupported(target, stats)
			}
			return target.ID != "" && lazyPathDirectionsSupported(target, stats)
		},
		RequiresReverse: func(target Request) bool {
			if target.DirectionStrategy == "impact" {
				return true
			}
			return pathLevelsRequireReverse(target, normalizedDepth(target.Depth))
		},
	},
	"shortest_path": {
		Op: "shortest_path",
		CanExecute: func(target Request, stats PlannerStats) bool {
			if target.DirectionStrategy == "impact" {
				return lazyImpactSupported(target, stats)
			}
			return target.ID != "" && lazyPathDirectionsSupported(target, stats)
		},
		RequiresReverse: func(target Request) bool {
			if target.DirectionStrategy == "impact" {
				return true
			}
			return pathLevelsRequireReverse(target, normalizedDepth(target.Depth))
		},
	},
	"neighbors": {
		Op: "neighbors",
		CanExecute: func(target Request, stats PlannerStats) bool {
			if target.DirectionStrategy == "impact" {
				return target.ID != "" && lazyImpactSupportedForDepth(target, 1, stats)
			}
			return target.ID != "" && lazyDirectionSupported(target.Direction, stats)
		},
		RequiresReverse: func(target Request) bool {
			if target.DirectionStrategy == "impact" {
				return true
			}
			return target.Direction != "out"
		},
	},
	"impact": {
		Op:              "impact",
		CanExecute:      lazyImpactSupported,
		RequiresReverse: func(Request) bool { return true },
	},
}

func SupportsLazyRead(request Request, stats PlannerStats) bool {
	if len(stats.EntityPages) == 0 {
		return false
	}
	target := normalizeCompiledRequest(lazyTarget(request))
	cap, ok := lazyOperatorCapabilities[target.Op]
	if !ok {
		return false
	}
	return cap.CanExecute(target, stats)
}

func RequiresReverseIndex(request Request) bool {
	target := normalizeCompiledRequest(lazyTarget(request))
	cap, ok := lazyOperatorCapabilities[target.Op]
	if !ok {
		return false
	}
	return cap.RequiresReverse(target)
}

func pathLevelsRequireReverse(target Request, depth int) bool {
	for level := 0; level < depth; level++ {
		if requestForPathLevel(target, level).Direction != "out" {
			return true
		}
	}
	return false
}

func lazyImpactSupportedForDepth(request Request, depth int, stats PlannerStats) bool {
	if depth <= 0 {
		return false
	}
	for level := 0; level < depth; level++ {
		levelRequest := requestForPathLevel(request, level)
		directions, ok := impactDirectionsForRequest(levelRequest, stats)
		if !ok || !impactIndexesAvailable(directions, stats) {
			return false
		}
	}
	return true
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
	if stats.EntityScanAvailable {
		return true
	}
	if request.Kind != "" {
		return true
	}
	filters := requestIndexFilters(request)
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
