package partition

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type edgeIterator struct {
	lookup              *Lookup
	id, direction, next string
	allowed             map[string]struct{}
	page                []graph.Edge
	done                bool
}

func (it *edgeIterator) advance(ctx context.Context) (bool, error) {
	if len(it.page) > 0 {
		it.page = it.page[1:]
	}
	if len(it.page) > 0 {
		return true, nil
	}
	if it.done {
		return false, nil
	}
	batchSize := 16
	// Sparse type filters may scan many references before producing a page.
	// Retain their original scan batch to avoid multiplying cross-group RPCs.
	if len(it.allowed) > 0 {
		batchSize = 64
	}
	_, err := it.lookup.edges(ctx, it.id, it.direction, it.allowed, it.next, batchSize, func(edge graph.Edge) (bool, error) { it.page = append(it.page, edge); return len(it.page) < 16, nil })
	if err != nil {
		return false, err
	}
	it.done = len(it.page) < 16
	if len(it.page) == 0 {
		return false, nil
	}
	// The underlying seek is inclusive; NUL is the immediate string successor.
	it.next = it.page[len(it.page)-1].ID + "\x00"
	return true, nil
}

func (l *Lookup) VisitBothEdges(ctx context.Context, id string, allowed map[string]struct{}, start string, visit func(graph.Edge, string) (bool, error)) (bool, error) {
	out := edgeIterator{lookup: l, id: id, direction: "out", allowed: allowed, next: start}
	in := edgeIterator{lookup: l, id: id, direction: "in", allowed: allowed, next: start}
	hasOut, err := out.advance(ctx)
	if err != nil {
		return false, err
	}
	hasIn, err := in.advance(ctx)
	if err != nil {
		return false, err
	}
	for hasOut || hasIn {
		selected := &out
		if !hasOut || hasIn && in.page[0].ID <= out.page[0].ID {
			selected = &in
		}
		more, err := visit(selected.page[0], selected.direction)
		if err != nil || !more {
			return true, err
		}
		available, err := selected.advance(ctx)
		if err != nil {
			return false, err
		}
		if selected == &out {
			hasOut = available
		} else {
			hasIn = available
		}
	}
	return true, nil
}
