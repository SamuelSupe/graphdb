package query

import (
	"context"
	"fmt"
	"time"
)

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
}

func newBudget(parent context.Context, request Request, profiler *profiler, lookup IndexLookup, entities EntityLookup) (*budget, context.CancelFunc) {
	ctx := parent
	cancel := func() {}
	if request.TimeoutMS > 0 {
		ctx, cancel = context.WithTimeout(parent, time.Duration(request.TimeoutMS)*time.Millisecond)
	}
	limit := request.CostLimit
	if limit <= 0 {
		limit = 100000
	}
	b := &budget{ctx: ctx, cancel: cancel, limit: limit, profiler: profiler, lookup: lookup, entities: entities}
	return b, cancel
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

func admitQuery(plan Plan, budget *budget) error {
	if plan.EstimatedCost > budget.limit {
		return fmt.Errorf("%w: estimated cost %d exceeds admission limit %d", ErrLimitExceeded, plan.EstimatedCost, budget.limit)
	}
	return budget.check()
}

func (b *budget) measure(name string, detail string, cost int, fn func() (int, error)) error {
	return b.profiler.measure(name, detail, cost, fn)
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
