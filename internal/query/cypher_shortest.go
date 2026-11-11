package query

import "errors"

// Iterate exact lengths so a shorter accepted path always wins, including
// when a WHERE predicate rejects an otherwise shorter path. All attempts use
// the enclosing statement's read view, cancellation and resource budget.
func (r *cyRuntime) visitShortest(input cyRow, cl cyClause, emit func(cyRow) error) error {
	p := cl.patterns[0]
	optional := cl.optional
	cl.optional = false
	cl.patterns = append([]cyPattern(nil), cl.patterns...)
	cl.patterns[0].rels = append([]cyRelPattern(nil), p.rels...)
	cl.patterns[0].shortest = ""
	for depth := p.rels[0].min; depth <= p.rels[0].max; depth++ {
		cl.patterns[0].rels[0].min = depth
		cl.patterns[0].rels[0].max = depth
		found := false
		var rows []cyRow
		err := r.visitMatch(input, cl, func(row cyRow) error {
			found = true
			if err := r.appendRow(&rows, row); err != nil {
				return err
			}
			if p.shortest == "shortestpath" {
				return errCypherStop
			}
			return nil
		})
		if err != nil && !errors.Is(err, errCypherStop) {
			return err
		}
		if found {
			for _, row := range rows {
				if err := emit(row); err != nil {
					return err
				}
			}
			return nil
		}
	}
	if optional {
		row := r.cloneRow(input)
		for _, slot := range cl.newSlots {
			row[slot] = nil
		}
		return emit(row)
	}
	return nil
}
