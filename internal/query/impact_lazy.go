package query

func lazyImpactSupported(request Request, stats PlannerStats) bool {
	if request.ID == "" {
		return false
	}
	depth := request.Depth
	if depth <= 0 {
		depth = 4
	}
	for level := 0; level < normalizedDepth(depth); level++ {
		levelRequest := requestForPathLevel(request, level)
		directions, ok := impactDirectionsForRequest(levelRequest, stats)
		if !ok || !impactIndexesAvailable(directions, stats) {
			return false
		}
	}
	return true
}

func impactDirectionsForRequest(request Request, stats PlannerStats) (map[string]string, bool) {
	if stats.Version <= 0 {
		return nil, false
	}
	seen := map[string]struct{}{}
	directions := map[string]string{}
	valid := true
	add := func(stat PlannerEdgeStat) {
		if stat.RelationType == "" {
			return
		}
		seen[stat.RelationType] = struct{}{}
		if stat.ImpactDirection == "" {
			return
		}
		if !validPlannerImpactDirection(stat.ImpactDirection) {
			valid = false
			return
		}
		if previous, ok := directions[stat.RelationType]; ok && previous != stat.ImpactDirection {
			valid = false
			return
		}
		directions[stat.RelationType] = stat.ImpactDirection
	}
	for _, stat := range stats.EdgeShards {
		add(stat)
	}
	for _, stat := range stats.ReverseEdgeShards {
		add(stat)
	}
	if !valid {
		return nil, false
	}

	allowed := relationTypeSet(request)
	if len(allowed) == 0 {
		allowed = seen
	}
	selected := make(map[string]string, len(allowed))
	for relationType := range allowed {
		direction, ok := directions[relationType]
		if !ok {
			if _, hasEdges := seen[relationType]; hasEdges {
				return nil, false
			}
			// A relation absent from both catalogs has no edges at this version.
			selected[relationType] = "none"
			continue
		}
		selected[relationType] = direction
	}
	return selected, true
}

func impactIndexesAvailable(directions map[string]string, stats PlannerStats) bool {
	out, in := impactIndexRelations(directions)
	return plannerStatsHaveRelations(stats.EdgeShards, out) &&
		plannerStatsHaveRelations(stats.ReverseEdgeShards, in)
}

func impactIndexRelations(directions map[string]string) (map[string]struct{}, map[string]struct{}) {
	out := map[string]struct{}{}
	in := map[string]struct{}{}
	for relationType, direction := range directions {
		switch direction {
		case "forward":
			out[relationType] = struct{}{}
		case "reverse":
			in[relationType] = struct{}{}
		case "both":
			out[relationType] = struct{}{}
			in[relationType] = struct{}{}
		}
	}
	return out, in
}

func plannerStatsHaveRelations(stats []PlannerEdgeStat, required map[string]struct{}) bool {
	if len(required) == 0 {
		return true
	}
	found := make(map[string]struct{}, len(required))
	for _, stat := range stats {
		if _, ok := required[stat.RelationType]; ok {
			found[stat.RelationType] = struct{}{}
		}
	}
	return len(found) == len(required)
}

func validPlannerImpactDirection(direction string) bool {
	switch direction {
	case "none", "forward", "reverse", "both":
		return true
	default:
		return false
	}
}

func impactDirectionAllows(impactDirection string, edgeDirection string) bool {
	switch impactDirection {
	case "forward":
		return edgeDirection == "out"
	case "reverse":
		return edgeDirection == "in"
	case "both":
		return edgeDirection == "out" || edgeDirection == "in"
	default:
		return false
	}
}
