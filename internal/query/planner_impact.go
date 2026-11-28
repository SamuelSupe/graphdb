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
	count, ok, usedGlobal := estimateImpactDirectionalTotal(directions, stats, shardID)
	if !ok {
		return 0, false
	}
	return scaleShardFanoutToNodeDegree(count, stats, shardID, usedGlobal), true
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
	count, ok, _ := estimateImpactDirectionalTotal(directions, stats, "")
	return count, ok
}

func estimateImpactDirectionalTotal(
	directions map[string]string,
	stats PlannerStats,
	shardID string,
) (int, bool, bool) {
	out, in := impactIndexRelations(directions)
	outCount, ok, outGlobal := selectedDirectionalCount(
		stats.EdgeShards, shardID, out,
	)
	if !ok {
		return 0, false, false
	}
	inCount, ok, inGlobal := selectedDirectionalCount(
		stats.ReverseEdgeShards, shardID, in,
	)
	if !ok {
		return 0, false, false
	}
	if outCount > math.MaxInt-inCount {
		return math.MaxInt, true, outGlobal || inGlobal
	}
	return outCount + inCount, true, outGlobal || inGlobal
}

func selectedDirectionalCount(
	stats []PlannerEdgeStat,
	shardID string,
	required map[string]struct{},
) (int, bool, bool) {
	if len(required) == 0 {
		return 0, true, false
	}
	if !plannerStatsHaveRelations(stats, required) {
		return 0, false, false
	}
	count, matched, usedGlobal := estimateDirectionalFanout(stats, shardID, required)
	if !matched && shardID != "" {
		for _, stat := range stats {
			if _, ok := required[stat.RelationType]; ok && stat.Shard == "" {
				return 0, false, false
			}
		}
	}
	return count, true, usedGlobal
}
