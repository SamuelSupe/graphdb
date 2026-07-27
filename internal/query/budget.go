package query

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

const defaultQueryCostLimit = 100000

type budget struct {
	ctx       context.Context
	cancel    context.CancelFunc
	limit     int
	cost      int
	scanned   int
	visited   int
	returned  int
	truncated bool
	timedOut  bool
	profiler  *profiler
	lookup    IndexLookup
	entities  EntityLookup
	planner   PlannerStats
}

func newBudget(parent context.Context, request Request, profiler *profiler, lookup IndexLookup, entities EntityLookup, planner PlannerStats) (*budget, context.CancelFunc) {
	ctx := parent
	cancel := func() {}
	if request.TimeoutMS > 0 {
		ctx, cancel = context.WithTimeout(parent, time.Duration(request.TimeoutMS)*time.Millisecond)
	}
	limit := normalizedCostLimit(request.CostLimit)
	b := &budget{
		ctx: ctx, cancel: cancel, limit: limit, profiler: profiler,
		lookup: lookup, entities: entities, planner: planner,
	}
	return b, cancel
}

func normalizedCostLimit(limit int) int {
	if limit <= 0 {
		return defaultQueryCostLimit
	}
	return limit
}

func (b *budget) add(cost int) error {
	if err := b.check(); err != nil {
		return err
	}
	b.cost += cost
	if b.cost > b.limit {
		return fmt.Errorf("%w: cost %d exceeds limit %d", ErrLimitExceeded, b.cost, b.limit)
	}
	return nil
}

func (b *budget) check() error {
	select {
	case <-b.ctx.Done():
		b.timedOut = true
		return fmt.Errorf("%w: query timeout or cancellation", ErrLimitExceeded)
	default:
	}
	return nil
}

func (b *budget) checkAdditionalCost(cost int) error {
	if err := b.check(); err != nil {
		return err
	}
	if cost <= 0 {
		return nil
	}
	remaining := b.limit - b.cost
	if remaining < 0 || cost > remaining {
		return fmt.Errorf(
			"%w: additional cost %d exceeds remaining limit %d",
			ErrLimitExceeded,
			cost,
			max(remaining, 0),
		)
	}
	return nil
}

func admitQuery(plan Plan, budget *budget) error {
	if plan.EstimatedCost > budget.limit {
		return fmt.Errorf("%w: estimated cost %d exceeds admission limit %d", ErrLimitExceeded, plan.EstimatedCost, budget.limit)
	}
	return budget.check()
}

func (b *budget) measure(name string, detail string, cost int, fn func() (int, error)) (err error) {
	ctx, span := startQueryOperatorSpan(b.ctx, name, detail, cost)
	previousCtx := b.ctx
	b.ctx = ctx
	beforeScanned := b.scanned
	beforeVisited := b.visited
	beforeCost := b.cost
	rows := 0
	defer func() {
		b.ctx = previousCtx
		if span != nil {
			span.SetAttributes(
				attribute.Int("graphdb.query.operator.rows", rows),
				attribute.Int("graphdb.query.operator.scanned", b.scanned-beforeScanned),
				attribute.Int("graphdb.query.operator.visited", b.visited-beforeVisited),
				attribute.Int("graphdb.query.operator.cost", b.cost-beforeCost),
			)
		}
		endQueryOperatorSpan(span, err)
	}()
	err = b.profiler.measure(name, detail, cost, func() (int, error) {
		var measureErr error
		rows, measureErr = fn()
		return rows, measureErr
	})
	return err
}

func (b *budget) profile() []OperatorStat {
	return b.profiler.snapshot()
}

func (b *budget) stats() Stats {
	return Stats{
		Scanned:   b.scanned,
		Visited:   b.visited,
		Returned:  b.returned,
		Cost:      b.cost,
		TimedOut:  b.timedOut,
		Truncated: b.truncated,
	}
}
