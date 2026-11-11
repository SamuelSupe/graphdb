package query

import (
	"context"
	"fmt"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type cyRuntime struct {
	plan      *CypherPlan
	g         *graph.Graph
	options   ExecuteOptions
	budget    *budget
	nodes     map[string]*cyNodeValue
	rels      map[string]*cyRelValue
	memory    int
	memoryErr error
}

func (r *cyRuntime) reserve(n int) error {
	if r.memoryErr != nil {
		return r.memoryErr
	}
	if n < 0 || n > cypherMaxMemory-r.memory {
		r.memoryErr = fmt.Errorf("%w: Cypher allocation budget exceeds 64 MiB", ErrLimitExceeded)
		return r.memoryErr
	}
	r.memory += n
	return nil
}
func (r *cyRuntime) check() error {
	if r.memoryErr != nil {
		return r.memoryErr
	}
	return r.budget.check()
}
func (r *cyRuntime) cloneRow(row cyRow) cyRow {
	_ = r.reserve(r.plan.slots*16 + 24)
	out := make(cyRow, r.plan.slots)
	if row == nil {
		for i := range out {
			out[i] = cyUnbound{}
		}
	} else {
		copy(out, row)
	}
	return out
}
func (r *cyRuntime) appendRow(rows *[]cyRow, row cyRow) error {
	if len(*rows) >= cypherMaxRows {
		return fmt.Errorf("%w: Cypher intermediate result exceeds %d rows", ErrLimitExceeded, cypherMaxRows)
	}
	if err := r.reserve(24); err != nil {
		return err
	}
	*rows = append(*rows, row)
	return nil
}
func (r *cyRuntime) predicate(e *cyExpr, row cyRow) (bool, error) {
	if e == nil {
		return true, nil
	}
	v, err := r.eval(e, row, nil)
	if err != nil {
		return false, err
	}
	v, err = cyBool(v)
	if err != nil {
		return false, e.pos.fail("cypher_type_error", err.Error())
	}
	return v == true, nil
}

func (p *CypherPlan) execute(ctx context.Context, g *graph.Graph, options ExecuteOptions, emit func([]any) error) (CypherResponse, error) {
	if options.PlannerStats.Version != 0 && options.PlannerStats.Version != g.Version {
		return CypherResponse{}, ErrIndexUnavailable
	}
	budget, cancel := newBudget(ctx, p.ReadRequest(), newProfiler(p.profile), options.IndexLookup, options.EntityLookup, options.PlannerStats)
	defer cancel()
	r := &cyRuntime{plan: p, g: g, options: options, budget: budget, nodes: map[string]*cyNodeValue{}, rels: map[string]*cyRelValue{}}
	if err := r.reserve(cySize(p.request.Parameters)); err != nil {
		return CypherResponse{}, err
	}
	response := CypherResponse{Version: g.Version, Columns: append([]string(nil), p.branches[0].columns...), Rows: [][]any{}}
	if p.explain || p.profile {
		plan := &Plan{Op: "cypher", Strategy: "row-pipeline", StatsSource: "snapshot"}
		if options.EntityLookup != nil {
			plan.StatsSource = "persisted_catalog"
		}
		for _, b := range p.branches {
			for _, cl := range b.clauses {
				plan.Steps = append(plan.Steps, PlanStep{Name: "cypher-" + cl.kind})
			}
		}
		response.Plan = plan
	}
	if p.explain {
		response.Stats = budget.stats()
		return response, r.check()
	}
	emitValues := func(row []any) error {
		limit := maxQueryLimit
		if emit != nil {
			limit = cypherMaxRows
		}
		if budget.returned >= limit {
			return fmt.Errorf("%w: Cypher output exceeds %d rows; use LIMIT", ErrLimitExceeded, limit)
		}
		if err := budget.add(1); err != nil {
			return err
		}
		encoded := make([]any, len(row))
		for i, v := range row {
			wire, err := cyWire(v)
			if err != nil {
				return err
			}
			if err := r.reserve(cySize(wire)); err != nil {
				return err
			}
			encoded[i] = wire
		}
		if emit != nil {
			if err := emit(encoded); err != nil {
				return err
			}
		} else {
			response.Rows = append(response.Rows, encoded)
		}
		budget.returned++
		return nil
	}
	if len(p.branches) == 1 && cyCanPipeline(p.branches[0]) {
		b := p.branches[0]
		last := b.clauses[len(b.clauses)-1]
		err := budget.measure("cypher-pipeline", "", 0, func() (int, error) {
			err := r.pipeline(b, func(row cyRow) error {
				values := make([]any, len(last.items))
				for i, it := range last.items {
					values[i] = row[it.slot]
				}
				return emitValues(values)
			})
			return budget.returned, err
		})
		if err != nil {
			return CypherResponse{}, err
		}
		response.Stats = budget.stats()
		if p.profile {
			response.Profile = budget.profile()
		}
		return response, r.check()
	}
	var rows [][]any
	for branchIndex, b := range p.branches {
		current := []cyRow{r.cloneRow(nil)}
		for _, cl := range b.clauses {
			var next []cyRow
			err := budget.measure("cypher-"+cl.kind, "", 0, func() (int, error) {
				var err error
				switch cl.kind {
				case "match":
					next, err = r.match(current, cl)
				case "unwind":
					next, err = r.unwind(current, cl)
				case "with", "return":
					next, err = r.project(current, cl)
				default:
					err = fmt.Errorf("%w: unsupported Cypher clause", ErrInvalid)
				}
				return len(next), err
			})
			if err != nil {
				return CypherResponse{}, err
			}
			current = next
		}
		last := b.clauses[len(b.clauses)-1]
		for _, row := range current {
			values := make([]any, len(last.items))
			for i, it := range last.items {
				values[i] = row[it.slot]
			}
			rows = append(rows, values)
			if len(rows) > cypherMaxRows {
				return CypherResponse{}, fmt.Errorf("%w: UNION result exceeds row budget", ErrLimitExceeded)
			}
		}
		if branchIndex > 0 && !b.unionAll {
			seen := map[string]bool{}
			unique := make([][]any, 0, len(rows))
			for _, row := range rows {
				key := cyKey(row)
				if err := r.reserve(len(key) + 32); err != nil {
					return CypherResponse{}, err
				}
				if !seen[key] {
					seen[key] = true
					unique = append(unique, row)
				}
			}
			rows = unique
		}
	}
	for _, row := range rows {
		if err := emitValues(row); err != nil {
			return CypherResponse{}, err
		}
	}
	response.Stats = budget.stats()
	if p.profile {
		response.Profile = budget.profile()
	}
	return response, r.check()
}

func (r *cyRuntime) unwind(rows []cyRow, cl cyClause) ([]cyRow, error) {
	out := []cyRow{}
	for _, row := range rows {
		v, err := r.eval(cl.expr, row, nil)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		xs, ok := v.([]any)
		if !ok {
			xs = []any{v}
		}
		for _, x := range xs {
			next := r.cloneRow(row)
			next[cl.slot] = x
			if err := r.appendRow(&out, next); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

type cyProjected struct {
	row  cyRow
	keys []any
}

func (r *cyRuntime) project(rows []cyRow, cl cyClause) ([]cyRow, error) {
	// ponytail: blocking operators retain bounded candidates; add Top-K or spill
	// when workloads need to exceed the existing row/allocation budgets.
	var values []cyProjected
	seen := map[string]bool{}
	emit := func(row cyRow, aggs map[*cyExpr]any) error {
		next := r.cloneRow(row)
		tuple := make([]any, len(cl.items))
		for i, it := range cl.items {
			v, err := r.eval(it.expr, row, aggs)
			if err != nil {
				return err
			}
			tuple[i] = v
			next[it.slot] = v
		}
		if cl.distinct {
			key := cyKey(tuple)
			if seen[key] {
				return nil
			}
			if err := r.reserve(len(key) + 32); err != nil {
				return err
			}
			seen[key] = true
		}
		keys := make([]any, len(cl.sort))
		for i, s := range cl.sort {
			v, err := r.eval(s.expr, next, nil)
			if err != nil {
				return err
			}
			keys[i] = v
		}
		if err := r.reserve(32 + len(keys)*16); err != nil {
			return err
		}
		values = append(values, cyProjected{next, keys})
		return nil
	}
	if len(cl.aggregates) > 0 {
		if err := r.aggregate(rows, cl, emit); err != nil {
			return nil, err
		}
	} else {
		for _, row := range rows {
			if err := emit(row, nil); err != nil {
				return nil, err
			}
		}
	}
	if len(cl.sort) > 0 {
		if err := r.budget.add(len(values)); err != nil {
			return nil, err
		}
		var sortErr error
		sort.SliceStable(values, func(i, j int) bool {
			if sortErr != nil {
				return false
			}
			sortErr = r.budget.add(1)
			if sortErr != nil {
				return false
			}
			for k, s := range cl.sort {
				cmp := cyCompare(values[i].keys[k], values[j].keys[k])
				if cmp != 0 {
					if s.desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
		if sortErr != nil {
			return nil, sortErr
		}
	}
	bound := func(e *cyExpr, def int64) (int64, error) {
		if e == nil {
			return def, nil
		}
		v, err := r.eval(e, r.cloneRow(nil), nil)
		if err != nil {
			return 0, err
		}
		i, ok := v.(int64)
		if !ok || i < 0 {
			return 0, e.pos.fail("cypher_type_error", "SKIP/LIMIT must be a nonnegative integer")
		}
		return i, nil
	}
	skip, err := bound(cl.skip, 0)
	if err != nil {
		return nil, err
	}
	limit, err := bound(cl.limit, int64(len(values)))
	if err != nil {
		return nil, err
	}
	start := min(int64(len(values)), skip)
	end := start + min(int64(len(values))-start, limit)
	out := make([]cyRow, 0, end-start)
	for _, v := range values[start:end] {
		ok, err := r.predicate(cl.where, v.row)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, v.row)
		}
	}
	return out, nil
}
