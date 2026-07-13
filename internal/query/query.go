package query

import (
	"context"
	"fmt"
	"sort"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

func Execute(g *graph.Graph, request Request) (Response, error) {
	return ExecuteContext(context.Background(), g, request)
}

func ExecuteContext(ctx context.Context, g *graph.Graph, request Request) (Response, error) {
	return ExecuteContextWithOptions(ctx, g, request, ExecuteOptions{})
}

func ExecuteContextWithOptions(ctx context.Context, g *graph.Graph, request Request, options ExecuteOptions) (Response, error) {
	if request.Op == "explain" {
		target, err := targetRequest(request)
		if err != nil {
			return Response{}, err
		}
		if err := validateRequest(target); err != nil {
			return Response{}, err
		}
		plan := measureQueryPlan(ctx, g, target, options.PlannerStats, newProfiler(false))
		return Response{Version: g.Version, Plan: &plan}, nil
	}
	if request.Op == "profile" {
		target, err := targetRequest(request)
		if err != nil {
			return Response{}, err
		}
		target.Profile = true
		return ExecuteContextWithOptions(ctx, g, target, options)
	}
	if err := validateRequest(request); err != nil {
		return Response{}, err
	}
	profiler := newProfiler(request.Profile)
	plan := measureQueryPlan(ctx, g, request, options.PlannerStats, profiler)
	budget, cancel := newBudget(ctx, request, profiler, options.IndexLookup, options.EntityLookup)
	defer cancel()
	if err := budget.measure("admission", "", plan.EstimatedCost, func() (int, error) {
		return 0, admitQuery(plan, budget)
	}); err != nil {
		return Response{}, err
	}
	var cursor cursorState
	if err := budget.measure("cursor", "", 0, func() (int, error) {
		var err error
		cursor, err = parseCursor(request.Cursor)
		if err != nil {
			return 0, err
		}
		return 0, validateCursor(cursor, g.Version, request)
	}); err != nil {
		return Response{}, err
	}
	run := func(response Response, err error) (Response, error) {
		return withPlan(response, err, plan, request)
	}
	switch request.Op {
	case "match":
		return run(executeMatch(g, request, plan, cursor, budget))
	case "neighbors":
		return run(executeNeighbors(g, request, cursor, budget))
	case "traverse":
		return run(executeTraverse(g, request, cursor, budget))
	case "impact":
		return run(executeImpact(g, request, cursor, budget))
	case "shortest_path":
		return run(executeShortestPath(g, request, cursor, budget))
	default:
		return Response{}, fmt.Errorf("%w: unsupported op %q", ErrInvalid, request.Op)
	}
}

func measureQueryPlan(ctx context.Context, g *graph.Graph, request Request, stats PlannerStats, profiler *profiler) Plan {
	_, span := startQueryOperatorSpan(ctx, "plan", request.Op, 0)
	var plan Plan
	_ = profiler.measure("plan", request.Op, 0, func() (int, error) {
		plan = PlanQueryWithStats(g, request, stats)
		return len(plan.Steps), nil
	})
	if span != nil {
		span.SetAttributes(
			attribute.String("graphdb.query.kind", request.Kind),
			attribute.String("graphdb.query.plan.strategy", plan.Strategy),
			attribute.String("graphdb.query.plan.index", plan.Index),
			attribute.String("graphdb.query.plan.stats_source", plan.StatsSource),
			attribute.Int("graphdb.query.plan.steps", len(plan.Steps)),
			attribute.Int("graphdb.query.plan.estimated_rows", plan.EstimatedRows),
			attribute.Int("graphdb.query.plan.estimated_cost", plan.EstimatedCost),
			attribute.Int("graphdb.query.plan.warnings", len(plan.Warnings)),
		)
	}
	endQueryOperatorSpan(span, nil)
	return plan
}

func targetRequest(request Request) (Request, error) {
	if request.TargetOp == "" {
		return Request{}, fmt.Errorf("%w: %s requires target_op", ErrInvalid, request.Op)
	}
	target := request
	target.Op = request.TargetOp
	target.TargetOp = ""
	return target, nil
}

func withPlan(response Response, err error, plan Plan, request Request) (Response, error) {
	if err != nil {
		return response, err
	}
	if request.Profile {
		response.Plan = &plan
	}
	return response, nil
}

func buildResponse(version int64, results []Result, request Request, cursor cursorState, budget *budget) (Response, error) {
	return buildResponseWithAggregates(version, results, request, cursor, budget, aggregateResults(results, request.Aggregate))
}

func buildResponseWithAggregates(version int64, results []Result, request Request, cursor cursorState, budget *budget, aggregates map[string]any) (Response, error) {
	return buildResponseWithAggregatesAndGroups(version, results, request, cursor, budget, aggregates, aggregateGroups(results, request.GroupBy, request.Aggregate, request.Having, request.HavingExpr))
}

func buildResponseWithAggregatesAndGroups(version int64, results []Result, request Request, cursor cursorState, budget *budget, aggregates map[string]any, groups []AggregateGroup) (Response, error) {
	page, next, err := paginate(version, results, request, cursor)
	if err != nil {
		return Response{}, err
	}
	applyProjectionToResults(page, request.Project)
	budget.returned = len(page)
	budget.truncated = next != ""
	return Response{
		Version:    version,
		Results:    page,
		NextCursor: next,
		Stats:      budget.stats(),
		Aggregates: aggregates,
		Groups:     groups,
		Profile:    budget.profile(),
	}, nil
}

func applyProjectionToResults(results []Result, fields []string) {
	for i := range results {
		applyProjection(&results[i], fields)
	}
}

func validateDirection(direction string) error {
	switch direction {
	case "", "out", "in", "both":
		return nil
	default:
		return fmt.Errorf("%w: unsupported direction %q", ErrInvalid, direction)
	}
}

const (
	defaultQueryLimit = 100
	maxQueryLimit     = 1000
)

func normalizedLimit(limit int) int {
	if limit <= 0 {
		return defaultQueryLimit
	}
	if limit > maxQueryLimit {
		return maxQueryLimit
	}
	return limit
}

func relationTypeSet(request Request) map[string]struct{} {
	values := append([]string(nil), request.RelationTypes...)
	if request.RelationType != "" {
		values = append(values, request.RelationType)
	}
	if len(request.Path.RelationTypes) > 0 {
		values = append(values, request.Path.RelationTypes...)
	}
	out := map[string]struct{}{}
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortResults(results []Result, specs []SortSpec) {
	sort.SliceStable(results, func(i, j int) bool {
		return compareResults(results[i], results[j], specs) < 0
	})
}
