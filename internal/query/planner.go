package query

import (
	"crypto/sha256"
	"fmt"
	"math"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func PlanQuery(g *graph.Graph, request Request) Plan {
	return PlanQueryWithStats(g, request, PlannerStats{})
}

func PlanQueryWithStats(g *graph.Graph, request Request, stats PlannerStats) Plan {
	plan := Plan{Op: request.Op, Strategy: "scan"}
	if stats.Version != 0 && stats.Version != g.Version {
		plan.Warnings = append(plan.Warnings, "index catalog version does not match snapshot")
	}
	switch request.Op {
	case "match":
		planMatch(&plan, g, request, stats)
	case "pattern":
		planPattern(&plan, g, request, stats)
	case "neighbors":
		plan.Strategy = "adjacency"
		plan.Index = "edge_adjacency"
		plan.Steps = append(plan.Steps, PlanStep{Name: "load-start", Detail: request.ID, Cost: 1})
		cost := estimateFanout(g, request, stats)
		if (request.Direction == "out" || request.Direction == "in" ||
			request.Direction == "both" || request.Direction == "") &&
			request.DirectionStrategy != "impact" &&
			canPageNeighborsEarly(request) {
			cost = min(cost, normalizedLimit(request.Limit)+1)
		}
		plan.Steps = append(plan.Steps, PlanStep{Name: "expand-neighbors", Detail: directionDetail(request), Cost: cost})
		plan.EstimatedRows = cost
		plan.EstimatedCost = cost + 1
	case "traverse", "shortest_path":
		plan.Strategy = "bounded-bfs"
		plan.Index = "edge_adjacency"
		cost := estimateTraversalCost(g, request, stats)
		plan.Steps = append(plan.Steps, PlanStep{Name: "bounded-expand", Detail: fmt.Sprintf("depth=%d", normalizedDepth(request.Depth)), Cost: cost})
		plan.EstimatedRows = cost
		plan.EstimatedCost = cost
	case "impact":
		plan.Strategy = "impact-bfs"
		plan.Index = "edge_adjacency+relation_semantics"
		cost := estimateTraversalCost(g, request, stats)
		plan.Steps = append(plan.Steps, PlanStep{Name: "impact-expand", Detail: "uses relation impact_direction", Cost: cost})
		plan.EstimatedRows = cost
		plan.EstimatedCost = cost
	default:
		plan.Warnings = append(plan.Warnings, "unsupported op")
	}
	if len(request.Sort) > 0 {
		cost := max(plan.EstimatedRows, 1)
		plan.Steps = append(plan.Steps, PlanStep{Name: "sort", Cost: cost})
		plan.EstimatedCost += cost
	}
	if len(request.Aggregate) > 0 {
		cost := max(plan.EstimatedRows, 1)
		plan.Steps = append(plan.Steps, PlanStep{Name: "aggregate", Cost: cost})
		plan.EstimatedCost += cost
	}
	if request.CostLimit > 0 && plan.EstimatedCost > request.CostLimit {
		plan.Warnings = append(plan.Warnings, "estimated cost exceeds cost_limit")
	}
	return plan
}

func planPattern(plan *Plan, g *graph.Graph, request Request, stats PlannerStats) {
	planMatch(plan, g, request, stats)
	startRows := max(plan.EstimatedRows, 1)
	edges := estimateEdgeShardTotal(stats, relationTypeSet(request))
	entities := 0
	for _, page := range stats.EntityPages {
		entities += max(page.EntityCount, 0)
	}
	if edges == 0 {
		edges = len(g.Edges)
	}
	if entities == 0 {
		entities = len(g.Entities)
	}
	fanout := 1
	if entities > 0 && edges > entities {
		fanout = (edges + entities - 1) / entities
	}
	paths := startRows
	for range request.Path.Steps {
		if paths > 100000/fanout {
			paths = 100000
			break
		}
		paths *= fanout
	}
	plan.Steps = append(plan.Steps, PlanStep{Name: "bounded-pattern-expand", Detail: fmt.Sprintf("steps=%d", len(request.Path.Steps)), Cost: paths})
	plan.EstimatedRows = paths
	plan.EstimatedCost += paths
}

func planMatch(plan *Plan, g *graph.Graph, request Request, stats PlannerStats) {
	filters := requestFilters(request)
	if id, ok := idEquality(filters); ok {
		plan.Strategy = "entity-id"
		plan.Index = "entity_id"
		plan.IndexField = "id"
		plan.IndexOp = "eq"
		plan.IndexValues = []any{id}
		plan.EstimatedRows = 1
		plan.EstimatedCost = 1
		plan.Steps = append(plan.Steps, PlanStep{Name: "id-lookup", Detail: id, Cost: 1})
		return
	}
	best := indexChoice{count: math.MaxInt}
	bestScan := indexChoice{count: math.MaxInt}
	for _, filter := range filters {
		op := filter.Op
		if op == "" {
			op = "eq"
		}
		field, ok := indexedField(filter.Field)
		if !ok {
			continue
		}
		if request.Kind == "" || (!g.HasFieldIndex(request.Kind, field) && !statsHasField(stats, request.Kind, field)) {
			continue
		}
		if op == "eq" || op == "in" {
			values := filterValues(filter)
			if !indexableFilterValues(values) {
				continue
			}
			count, source := estimateIndexCount(g, request.Kind, field, values, stats)
			if count < best.count {
				best = indexChoice{field: field, op: op, values: values, count: count, source: source}
			}
			continue
		}
		if indexScanFilterSupported(filter) {
			count, source := estimateIndexScanCount(g, request.Kind, field, stats)
			if count < bestScan.count {
				bestScan = indexChoice{field: field, op: "scan", count: count, source: source}
			}
		}
	}
	if best.field != "" {
		plan.Strategy = "field-index"
		plan.Index = "field:" + request.Kind + "." + best.field
		plan.IndexField = best.field
		plan.IndexOp = best.op
		plan.IndexValues = best.values
		plan.StatsSource = best.source
		plan.EstimatedRows = best.count
		plan.Steps = append(plan.Steps, PlanStep{Name: "index-seek", Detail: plan.Index, Cost: 1})
		plan.EstimatedCost = max(best.count, 1)
		return
	}
	if bestScan.field != "" {
		plan.Strategy = "field-index-scan"
		plan.Index = "field:" + request.Kind + "." + bestScan.field
		plan.IndexField = bestScan.field
		plan.IndexOp = bestScan.op
		plan.StatsSource = bestScan.source
		plan.EstimatedRows = bestScan.count
		plan.Steps = append(plan.Steps, PlanStep{Name: "index-scan", Detail: plan.Index, Cost: 1})
		plan.EstimatedCost = max(bestScan.count, 1)
		return
	}
	plan.Strategy = "kind-scan"
	plan.EstimatedRows = estimatedKindScanRows(g, request, stats)
	if len(g.Entities) == 0 && stats.Version == g.Version && len(stats.EntityPages) > 0 {
		plan.StatsSource = "persisted-catalog"
	}
	plan.Steps = append(plan.Steps, PlanStep{Name: "scan-entities", Detail: request.Kind, Cost: plan.EstimatedRows})
	plan.EstimatedCost = plan.EstimatedRows
	if request.Kind == "" {
		plan.Warnings = append(plan.Warnings, "match without kind scans all entities")
	}
}

func estimatedKindScanRows(g *graph.Graph, request Request, stats PlannerStats) int {
	rows := len(g.Entities)
	lazyPageScan := len(g.Entities) == 0 && stats.Version == g.Version
	if lazyPageScan {
		rows = estimateEntityPageTotal(stats)
	}
	if lazyPageScan &&
		request.Op == "match" &&
		canPageMatchEarly(request) &&
		len(requestFilters(request)) == 0 &&
		request.WhereExpr == nil {
		rows = min(rows, normalizedLimit(request.Limit)+1)
	}
	return rows
}

func estimateEntityPageTotal(stats PlannerStats) int {
	total := 0
	for _, page := range stats.EntityPages {
		count := max(page.EntityCount, 0)
		if count > math.MaxInt-total {
			return math.MaxInt
		}
		total += count
	}
	return total
}

type indexChoice struct {
	field  string
	op     string
	values []any
	count  int
	source string
}

func estimateIndexCount(g *graph.Graph, kind string, field string, values []any, stats PlannerStats) (int, string) {
	if stats.Version == g.Version {
		if stat, ok := stats.field(kind, field); ok {
			return estimateFromIndexStat(stat, values), "persisted-catalog"
		}
	}
	return g.FieldIndexCount(kind, field, values), "runtime-index"
}

func estimateIndexScanCount(g *graph.Graph, kind string, field string, stats PlannerStats) (int, string) {
	if stats.Version == g.Version {
		if stat, ok := stats.field(kind, field); ok {
			return max(stat.EntryCount, 1), "persisted-catalog"
		}
	}
	return max(g.KindCount(kind), 1), "runtime-index"
}

func indexScanFilterSupported(filter Filter) bool {
	op := filter.Op
	if op == "" {
		op = "eq"
	}
	switch op {
	case "gt", "gte", "lt", "lte", "prefix":
		return len(filterValues(filter)) == 1 && indexKeyComparableValue(filter.Value)
	default:
		return false
	}
}

func statsHasField(stats PlannerStats, kind string, field string) bool {
	if stats.Version == 0 {
		return false
	}
	_, ok := stats.field(kind, field)
	return ok
}

func idEquality(filters []Filter) (string, bool) {
	for _, filter := range filters {
		op := filter.Op
		if op == "" {
			op = "eq"
		}
		if filter.Field == "id" && op == "eq" {
			value, ok := filter.Value.(string)
			return value, ok && value != ""
		}
	}
	return "", false
}

func filterValues(filter Filter) []any {
	if filter.Op == "in" {
		return anySlice(filter.Value)
	}
	return []any{filter.Value}
}

func estimateFanout(g *graph.Graph, request Request, stats PlannerStats) int {
	if count, ok := estimateFanoutFromStats(g, request, stats); ok {
		return count
	}
	if request.ID == "" {
		return len(g.Edges)
	}
	return g.NeighborCount(request.ID, request.Direction, relationTypeSet(request), request.Path.NodeKinds, request.Op == "impact" || request.DirectionStrategy == "impact")
}

func estimateTraversalCost(g *graph.Graph, request Request, stats PlannerStats) int {
	depth := normalizedDepth(request.Depth)
	edgeCap := estimateEdgeCap(
		g, stats, traversalEdgeDirection(request, depth),
	)
	if request.Op == "impact" ||
		request.DirectionStrategy == "impact" {
		if impactCap, ok := estimateImpactEdgeCap(request, stats); ok {
			edgeCap = max(impactCap, 1)
		}
	}
	cost := 0
	level := 1
	for i := 0; i < depth; i++ {
		levelRequest := requestForPathLevel(request, i)
		fanout := estimateFanout(g, levelRequest, stats)
		if fanout < 1 {
			break
		}
		if level > edgeCap/fanout {
			return edgeCap
		}
		level *= fanout
		if cost > edgeCap-level {
			return edgeCap
		}
		cost += level
	}
	return max(cost, 1)
}

func traversalEdgeDirection(request Request, depth int) string {
	if request.Op == "impact" ||
		request.DirectionStrategy == "impact" {
		return "both"
	}
	out := false
	in := false
	for level := 0; level < depth; level++ {
		switch requestForPathLevel(request, level).Direction {
		case "out":
			out = true
		case "in":
			in = true
		case "both", "":
			out = true
			in = true
		}
	}
	switch {
	case out && in:
		return "both"
	case in:
		return "in"
	default:
		return "out"
	}
}

func estimateFanoutFromStats(g *graph.Graph, request Request, stats PlannerStats) (int, bool) {
	if stats.Version == 0 || stats.Version != g.Version {
		return 0, false
	}
	if request.Op == "impact" || request.DirectionStrategy == "impact" {
		return estimateImpactFanoutFromStats(request, stats)
	}
	shardID := ""
	if request.ID != "" {
		shardID = plannerEdgeShardID(request.ID)
	}
	allowed := relationTypeSet(request)
	switch request.Direction {
	case "out":
		return estimateDirectionalFanout(
			stats.EdgeShards, shardID, allowed,
		)
	case "in":
		return estimateDirectionalFanout(
			stats.ReverseEdgeShards, shardID, allowed,
		)
	case "both", "":
		out, outOK := estimateDirectionalFanout(
			stats.EdgeShards, shardID, allowed,
		)
		in, inOK := estimateDirectionalFanout(
			stats.ReverseEdgeShards, shardID, allowed,
		)
		if !outOK || !inOK {
			return 0, false
		}
		return out + in, true
	default:
		return 0, false
	}
}

func estimateDirectionalFanout(
	shards []PlannerEdgeStat,
	shardID string,
	allowed map[string]struct{},
) (int, bool) {
	if len(shards) == 0 {
		return 0, false
	}
	count := 0
	matched := false
	for _, shard := range shards {
		if (shardID != "" && shard.Shard != shardID) ||
			!relationAllowed(shard.RelationType, allowed) {
			continue
		}
		matched = true
		count += max(shard.EdgeCount, 0)
	}
	if shardID == "" {
		matched = true
	}
	return count, matched
}

func estimateEdgeCap(
	g *graph.Graph,
	stats PlannerStats,
	direction string,
) int {
	if stats.Version != 0 && stats.Version == g.Version {
		switch direction {
		case "in":
			if count, ok := estimateDirectionalFanout(
				stats.ReverseEdgeShards, "", nil,
			); ok {
				return max(count, 1)
			}
		case "both", "":
			out, outOK := estimateDirectionalFanout(
				stats.EdgeShards, "", nil,
			)
			in, inOK := estimateDirectionalFanout(
				stats.ReverseEdgeShards, "", nil,
			)
			if outOK && inOK {
				return max(out+in, 1)
			}
		default:
			if count, ok := estimateDirectionalFanout(
				stats.EdgeShards, "", nil,
			); ok {
				return max(count, 1)
			}
		}
	}
	return max(len(g.Edges), 1)
}

func estimateEdgeShardTotal(stats PlannerStats, allowed map[string]struct{}) int {
	total := 0
	for _, shard := range stats.EdgeShards {
		if !relationAllowed(shard.RelationType, allowed) {
			continue
		}
		total += max(shard.EdgeCount, 0)
	}
	return total
}

func plannerEdgeShardID(from string) string {
	if from == "" {
		return "default"
	}
	sum := sha256.Sum256([]byte(strings.ToLower(from)))
	return fmt.Sprintf("%02x", int(sum[0])%64)
}

func directionDetail(request Request) string {
	direction := request.Direction
	if direction == "" {
		direction = "both"
	}
	return "direction=" + direction
}
