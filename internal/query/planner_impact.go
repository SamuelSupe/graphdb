package query

import "math"

func estimateImpactFanoutFromStats(
	request Request,
	stats PlannerStats,
) (int, bool) {
	directions, ok := impactDirectionsForRequest(request, stats)
	if !ok {
		return 0, false
	}
	shardID := ""
	if request.ID != "" {
		shardID = plannerEdgeShardID(request.ID)
	}
	return estimateImpactDirectionalTotal(directions, stats, shardID)
}

func estimateImpactEdgeCap(
	request Request,
	stats PlannerStats,
) (int, bool) {
	if stats.Version == 0 {
		return 0, false
	}
	directions, ok := impactDirectionsForRequest(request, stats)
	if !ok {
		return 0, false
	}
	return estimateImpactDirectionalTotal(directions, stats, "")
}

func estimateImpactDirectionalTotal(
	directions map[string]string,
	stats PlannerStats,
	shardID string,
) (int, bool) {
	out, in := impactIndexRelations(directions)
	outCount, ok := selectedDirectionalCount(
		stats.EdgeShards, shardID, out,
	)
	if !ok {
		return 0, false
	}
	inCount, ok := selectedDirectionalCount(
		stats.ReverseEdgeShards, shardID, in,
	)
	if !ok {
		return 0, false
	}
	if outCount > math.MaxInt-inCount {
		return math.MaxInt, true
	}
	return outCount + inCount, true
}

func selectedDirectionalCount(
	stats []PlannerEdgeStat,
	shardID string,
	required map[string]struct{},
) (int, bool) {
	if len(required) == 0 {
		return 0, true
	}
	if !plannerStatsHaveRelations(stats, required) {
		return 0, false
	}
	count, _ := estimateDirectionalFanout(stats, shardID, required)
	return count, true
}
