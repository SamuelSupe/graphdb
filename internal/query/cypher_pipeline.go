package query

import (
	"errors"
)

var errCypherStop = errors.New("Cypher pipeline complete")

func cyCanPipeline(b cyBranch) bool {
	for _, cl := range b.clauses {
		if cl.distinct || len(cl.aggregates) > 0 || len(cl.sort) > 0 {
			return false
		}
	}
	return true
}
func (r *cyRuntime) pagination(e *cyExpr, def int64) (int64, error) {
	if e == nil {
		return def, nil
	}
	v, err := r.eval(e, r.cloneRow(nil), nil)
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok || n < 0 {
		return 0, e.pos.fail("cypher_type_error", "SKIP/LIMIT must be a nonnegative integer")
	}
	return n, nil
}

func (r *cyRuntime) pipeline(b cyBranch, emit func(cyRow) error) error {
	skip := make([]int64, len(b.clauses))
	limits := make([]int64, len(b.clauses))
	seen := make([]int64, len(b.clauses))
	returned := make([]int64, len(b.clauses))
	for i, cl := range b.clauses {
		if cl.kind != "with" && cl.kind != "return" {
			continue
		}
		var err error
		skip[i], err = r.pagination(cl.skip, 0)
		if err != nil {
			return err
		}
		limits[i], err = r.pagination(cl.limit, 1<<62)
		if err != nil {
			return err
		}
		if limits[i] == 0 {
			return nil
		}
	}
	var next func(int, cyRow) error
	next = func(index int, row cyRow) error {
		if err := r.budget.check(); err != nil {
			return err
		}
		if index == len(b.clauses) {
			return emit(row)
		}
		cl := b.clauses[index]
		switch cl.kind {
		case "match":
			return r.visitMatch(row, cl, func(row cyRow) error { return next(index+1, row) })
		case "unwind":
			v, err := r.eval(cl.expr, row, nil)
			if err != nil || v == nil {
				return err
			}
			list, ok := v.([]any)
			if !ok {
				list = []any{v}
			}
			for _, v := range list {
				copy := r.cloneRow(row)
				copy[cl.slot] = v
				if err := next(index+1, copy); err != nil {
					return err
				}
			}
			return nil
		case "with", "return":
			copy := r.cloneRow(row)
			for _, it := range cl.items {
				v, err := r.eval(it.expr, row, nil)
				if err != nil {
					return err
				}
				copy[it.slot] = v
			}
			seen[index]++
			if seen[index] <= skip[index] {
				return nil
			}
			if returned[index] >= limits[index] {
				return errCypherStop
			}
			returned[index]++
			ok, err := r.predicate(cl.where, copy)
			if err != nil {
				return err
			}
			if ok {
				if err := next(index+1, copy); err != nil {
					return err
				}
			}
			if returned[index] >= limits[index] {
				return errCypherStop
			}
			return nil
		}
		return nil
	}
	err := next(0, r.cloneRow(nil))
	if errors.Is(err, errCypherStop) {
		return nil
	}
	return err
}
