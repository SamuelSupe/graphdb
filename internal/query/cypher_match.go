package query

import (
	"context"
	"fmt"
	"slices"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (r *cyRuntime) internNode(e graph.Entity) (*cyNodeValue, error) {
	if n := r.nodes[e.ID]; n != nil {
		return n, nil
	}
	if err := r.reserve(128 + len(e.ID) + cySize(map[string]any(e.Fields))); err != nil {
		return nil, err
	}
	n := &cyNodeValue{e}
	r.nodes[e.ID] = n
	return n, nil
}
func (r *cyRuntime) internRel(e graph.Edge) (*cyRelValue, error) {
	if rel := r.rels[e.ID]; rel != nil {
		return rel, nil
	}
	if err := r.reserve(128 + len(e.ID) + cySize(map[string]any(e.Fields))); err != nil {
		return nil, err
	}
	rel := &cyRelValue{e}
	r.rels[e.ID] = rel
	return rel, nil
}
func (r *cyRuntime) findNode(id string) (*cyNodeValue, error) {
	if n := r.nodes[id]; n != nil {
		return n, nil
	}
	if err := r.budget.add(1); err != nil {
		return nil, err
	}
	if lookup := r.options.EntityLookup; lookup != nil {
		fields := r.plan.fields
		if r.plan.allFields {
			fields = nil
		}
		get := lookup.GetEntity
		if checked, ok := lookup.(interface {
			GetEntityChecked(context.Context, string, []string) (graph.Entity, bool, error)
		}); ok {
			get = checked.GetEntityChecked
		}
		entity, ok, err := get(r.budget.ctx, id, fields)
		if err != nil || !ok {
			return nil, err
		}
		return r.internNode(entity)
	}
	e, ok := r.g.GetEntity(id)
	if !ok {
		return nil, nil
	}
	return r.internNode(e)
}

func (r *cyRuntime) node(id string) (*cyNodeValue, error) {
	n, err := r.findNode(id)
	if n != nil || err != nil {
		return n, err
	}
	if r.options.EntityLookup != nil {
		return nil, ErrIndexUnavailable
	}
	return nil, fmt.Errorf("%w: missing endpoint %s", ErrInvalid, id)
}
func cyBind(row cyRow, slot int, value any) bool {
	if _, unbound := row[slot].(cyUnbound); unbound {
		row[slot] = value
		return true
	}
	return cyEqual(row[slot], value) == true
}
func (r *cyRuntime) propertiesMatch(props *cyExpr, value any, row cyRow) (bool, error) {
	if props == nil {
		return true, nil
	}
	v, err := r.eval(props, row, nil)
	if err != nil {
		return false, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return false, props.pos.fail("cypher_type_error", "pattern properties require a map")
	}
	for k, want := range m {
		actual, err := cyProperty(value, k)
		if err != nil {
			return false, err
		}
		if cyEqual(actual, want) != true {
			return false, nil
		}
	}
	return true, nil
}
func (r *cyRuntime) nodeMatches(p cyNodePattern, node *cyNodeValue, row cyRow) (bool, error) {
	if node == nil {
		return false, nil
	}
	for _, want := range p.labels {
		found := false
		for _, label := range cyLabels(node) {
			if label == want {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return r.propertiesMatch(p.props, node, row)
}

func cyIDHint(e *cyExpr, slot int) *cyExpr {
	if e == nil {
		return nil
	}
	if e.op == "binary" && e.name == "AND" {
		if hint := cyIDHint(e.args[0], slot); hint != nil {
			return hint
		}
		return cyIDHint(e.args[1], slot)
	}
	if e.op != "binary" || e.name != "=" {
		return nil
	}
	for i := 0; i < 2; i++ {
		a, b := e.args[i], e.args[1-i]
		if a.op == "call" && a.name == "graphdb.id" && len(a.args) == 1 && a.args[0].op == "var" && a.args[0].slot == slot && (b.op == "literal" || b.op == "param") {
			return b
		}
	}
	return nil
}
func (r *cyRuntime) visitNodes(p cyNodePattern, row cyRow, where *cyExpr, visit func(*cyNodeValue) error) error {
	if _, unbound := row[p.slot].(cyUnbound); !unbound {
		n, ok := row[p.slot].(*cyNodeValue)
		if !ok {
			return nil
		}
		return visit(n)
	}
	if hint := cyIDHint(where, p.slot); hint != nil {
		v, err := r.eval(hint, row, nil)
		if err != nil {
			return err
		}
		id, ok := v.(string)
		if !ok {
			return nil
		}
		n, err := r.findNode(id)
		if err != nil || n == nil || n.entity.ID != id {
			return err
		}
		return visit(n)
	}
	covered, err := r.indexedNodes(p, row, where, visit)
	if err != nil {
		return err
	}
	callback := func(entity graph.Entity) (bool, error) {
		if err := r.budget.add(1); err != nil {
			return false, err
		}
		r.budget.scanned++
		if covered[entity.Kind] {
			return true, nil
		}
		n, err := r.internNode(entity)
		if err != nil {
			return false, err
		}
		return true, visit(n)
	}
	if r.options.EntityLookup != nil {
		pages, ok := r.options.EntityLookup.(EntityPageLookup)
		if !ok {
			return ErrIndexUnavailable
		}
		fields := r.plan.fields
		if r.plan.allFields {
			fields = nil
		}
		used, err := pages.VisitEntities(r.budget.ctx, "", fields, "", callback)
		if err != nil {
			return err
		}
		if !used {
			return ErrIndexUnavailable
		}
		return nil
	}
	return r.g.VisitEntitiesByID("", "", callback)
}

func (r *cyRuntime) visitEdges(id string, p cyRelPattern, visit func(*cyRelValue, *cyNodeValue) error) error {
	types := map[string]struct{}{}
	for _, t := range p.types {
		types[t] = struct{}{}
	}
	selfLoops := map[string]bool{}
	callback := func(edge graph.Edge) (bool, error) {
		if err := r.budget.add(1); err != nil {
			return false, err
		}
		r.budget.visited++
		if len(types) > 0 {
			if _, ok := types[edge.Type]; !ok {
				return true, nil
			}
		}
		endpoint := edge.To
		if p.direction == "in" || (p.direction == "both" && edge.To == id) {
			endpoint = edge.From
		}
		if p.direction == "both" && edge.From == edge.To {
			if selfLoops[edge.ID] {
				return true, nil
			}
			selfLoops[edge.ID] = true
		}
		rel, err := r.internRel(edge)
		if err != nil {
			return false, err
		}
		node, err := r.node(endpoint)
		if err != nil {
			return false, err
		}
		return true, visit(rel, node)
	}
	if r.options.IndexLookup != nil {
		switch p.direction {
		case "out":
			if lookup, ok := r.options.IndexLookup.(OutEdgeVisitLookup); ok {
				used, err := lookup.VisitOutEdges(r.budget.ctx, id, types, "", callback)
				if err != nil {
					return err
				}
				if used {
					return nil
				}
			}
		case "in":
			if lookup, ok := r.options.IndexLookup.(InEdgeVisitLookup); ok {
				used, err := lookup.VisitInEdges(r.budget.ctx, id, types, "", callback)
				if err != nil {
					return err
				}
				if used {
					return nil
				}
			}
		case "both":
			if lookup, ok := r.options.IndexLookup.(BothEdgeVisitLookup); ok {
				used, err := lookup.VisitBothEdges(r.budget.ctx, id, types, "", func(edge graph.Edge, _ string) (bool, error) { return callback(edge) })
				if err != nil {
					return err
				}
				if used {
					return nil
				}
			}
		}
		return ErrIndexUnavailable
	}
	return r.g.VisitNeighborsByEdgeID(id, p.direction, types, nil, false, "", func() error { return r.budget.add(1) }, func(n graph.Neighbor) (bool, error) { return callback(n.Edge) })
}

func (r *cyRuntime) match(rows []cyRow, cl cyClause) ([]cyRow, error) {
	out := []cyRow{}
	for _, input := range rows {
		if err := r.visitMatch(input, cl, func(row cyRow) error { return r.appendRow(&out, row) }); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *cyRuntime) visitMatch(input cyRow, cl cyClause, emit func(cyRow) error) error {
	if len(cl.patterns) == 1 && cl.patterns[0].shortest != "" {
		return r.visitShortest(input, cl, emit)
	}
	for _, p := range cl.patterns {
		if p.shortest != "" {
			return fmt.Errorf("%w: shortest paths require a separate MATCH clause", ErrInvalid)
		}
	}
	matched := false
	used := map[string]bool{}
	var patterns func(int, cyRow) error
	patterns = func(index int, row cyRow) error {
		if index == len(cl.patterns) {
			ok, err := r.predicate(cl.where, row)
			if err != nil {
				return err
			}
			if ok {
				matched = true
				return emit(row)
			}
			return nil
		}
		p := r.choosePattern(cl.patterns[index], row, cl.where)
		return r.visitNodes(p.nodes[0], row, cl.where, func(start *cyNodeValue) error {
			bound := r.cloneRow(row)
			if !cyBind(bound, p.nodes[0].slot, start) {
				return nil
			}
			ok, err := r.nodeMatches(p.nodes[0], start, bound)
			if err != nil || !ok {
				return err
			}
			path := cyPathValue{nodes: []*cyNodeValue{start}}
			var step func(int, *cyNodeValue, cyRow) error
			step = func(level int, current *cyNodeValue, state cyRow) error {
				if err := r.budget.check(); err != nil {
					return err
				}
				if level == len(p.rels) {
					next := r.cloneRow(state)
					if p.slot >= 0 {
						if err := r.reserve(48 + 16*(len(path.nodes)+len(path.rels))); err != nil {
							return err
						}
						value := &cyPathValue{append([]*cyNodeValue(nil), path.nodes...), append([]*cyRelValue(nil), path.rels...)}
						if p.reversed {
							slices.Reverse(value.nodes)
							slices.Reverse(value.rels)
						}
						if !cyBind(next, p.slot, value) {
							return nil
						}
					}
					return patterns(index+1, next)
				}
				relPattern := p.rels[level]
				target := p.nodes[level+1]
				segment := []any{}
				var expand func(*cyNodeValue, int) error
				expand = func(node *cyNodeValue, depth int) error {
					if depth >= relPattern.min {
						next := r.cloneRow(state)
						var value any
						if relPattern.variable {
							list := append([]any{}, segment...)
							if p.reversed {
								slices.Reverse(list)
							}
							value = list
						} else if len(segment) > 0 {
							value = segment[0]
						}
						if cyBind(next, relPattern.slot, value) && cyBind(next, target.slot, node) {
							ok, err := r.nodeMatches(target, node, next)
							if err != nil {
								return err
							}
							if ok {
								if err := step(level+1, node, next); err != nil {
									return err
								}
							}
						}
					}
					if depth == relPattern.max {
						return nil
					}
					return r.visitEdges(node.entity.ID, relPattern, func(rel *cyRelValue, nextNode *cyNodeValue) error {
						if used[rel.edge.ID] {
							return nil
						}
						ok, err := r.propertiesMatch(relPattern.props, rel, state)
						if err != nil || !ok {
							return err
						}
						used[rel.edge.ID] = true
						segment = append(segment, rel)
						path.nodes = append(path.nodes, nextNode)
						path.rels = append(path.rels, rel)
						err = expand(nextNode, depth+1)
						segment = segment[:len(segment)-1]
						path.nodes = path.nodes[:len(path.nodes)-1]
						path.rels = path.rels[:len(path.rels)-1]
						delete(used, rel.edge.ID)
						return err
					})
				}
				return expand(current, 0)
			}
			return step(0, start, bound)
		})
	}
	if err := patterns(0, input); err != nil {
		return err
	}
	if !matched && cl.optional {
		next := r.cloneRow(input)
		for _, slot := range cl.newSlots {
			next[slot] = nil
		}
		return emit(next)
	}
	return nil
}
