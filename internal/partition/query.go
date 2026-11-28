package partition

import (
	"container/heap"
	"context"
	"errors"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

type Lookup struct {
	View       View
	entities   map[string]graph.Entity
	scanWindow int
}

func (l *Lookup) GetEntity(ctx context.Context, id string, _ []string) (graph.Entity, bool, error) {
	if cached, ok := l.entities[id]; ok {
		return cached, true, nil
	}
	var entity graph.Entity
	found, err := l.View.Get(ctx, Bucket(id), EntityKey(id), &entity)
	if err != nil {
		return entity, false, err
	}
	if !found {
		key := EntityAliasKey(id)
		var owners []string
		_, err = l.View.Get(ctx, Bucket(key), key, &owners)
		if err != nil {
			return entity, false, err
		}
		if len(owners) != 1 || owners[0] == id {
			return entity, false, nil
		}
		found, err = l.View.Get(ctx, Bucket(owners[0]), EntityKey(owners[0]), &entity)
	}
	if found && err == nil {
		if l.entities == nil {
			l.entities = map[string]graph.Entity{}
		}
		if len(l.entities) < 1024 {
			l.entities[id] = entity
			l.entities[entity.ID] = entity
		}
	}
	return entity, found, err
}

func (l *Lookup) GetEntities(ctx context.Context, ids []string, _ []string) (map[string]graph.Entity, bool, error) {
	out := map[string]graph.Entity{}
	byPartition := map[uint8][]string{}
	for _, id := range ids {
		if entity, ok := l.entities[id]; ok {
			out[id] = entity
		} else {
			byPartition[Bucket(id)] = append(byPartition[Bucket(id)], EntityKey(id))
		}
	}
	err := l.View.visitKeys(ctx, byPartition, func(bucket uint8, row Row) error {
		var entity graph.Entity
		if err := decodeRecord(row, &entity); err != nil {
			return fmt.Errorf("%w: malformed entity record: %v", ErrPartitionCorrupt, err)
		}
		if row.Key != EntityKey(entity.ID) || Bucket(entity.ID) != bucket {
			return fmt.Errorf("%w: entity fragment identity mismatch", ErrPartitionCorrupt)
		}
		out[entity.ID] = entity
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (l *Lookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

type entityIterator struct {
	lookup  *Lookup
	request ReadRequest
	rows    []Row
	done    bool
	id      string
}

func (it *entityIterator) accept(result ReadResult) error {
	if len(result.Rows) == 0 && result.Next == "" && it.request.Start == "" && it.request.Kind == "" {
		return fmt.Errorf("%w: entity scan directory is missing", ErrPartitionCorrupt)
	}
	if result.Next != "" && result.Next <= it.request.Start {
		return fmt.Errorf("%w: fragment scan made no progress", ErrPartitionCorrupt)
	}
	it.rows = result.Rows
	it.request.Start = result.Next
	it.done = result.Next == ""
	return nil
}

func (it *entityIterator) advance(ctx context.Context) (bool, error) {
	for len(it.rows) == 0 {
		if it.done {
			return false, nil
		}
		result, err := it.lookup.View.Read(ctx, it.request.Partition, it.request)
		if err != nil {
			return false, err
		}
		if err := it.accept(result); err != nil {
			return false, err
		}
	}
	var entry entityScanEntry
	if err := decodeRecord(it.rows[0], &entry); err != nil {
		return false, fmt.Errorf("%w: malformed entity directory record: %v", ErrPartitionCorrupt, err)
	}
	it.rows = it.rows[1:]
	it.id = entry.ID
	return true, nil
}

type entityHeap []*entityIterator

func (h entityHeap) Len() int           { return len(h) }
func (h entityHeap) Less(i, j int) bool { return h[i].id < h[j].id }
func (h entityHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *entityHeap) Push(x any)        { *h = append(*h, x.(*entityIterator)) }
func (h *entityHeap) Pop() any          { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

func (l *Lookup) VisitEntities(ctx context.Context, kind string, _ []string, start string, visit func(graph.Entity) (bool, error)) (bool, error) {
	var pending entityHeap
	window := max(2, l.scanWindow)
	var requests []ReadRequest
	for i, count := range l.View.Graph.EntityCounts {
		if count == 0 {
			continue
		}
		request := ReadRequest{Partition: uint8(i), Prefix: Key("entity_scan"), Limit: max(16, window), Kind: kind}
		if start != "" {
			request.Start = Key("entity_scan", start)
		}
		requests = append(requests, request)
	}
	initial, err := l.View.readBatch(ctx, requests)
	if err != nil {
		return false, err
	}
	iterators := make([]*entityIterator, 0, len(initial))
	for i, result := range initial {
		it := &entityIterator{lookup: l, request: requests[i]}
		if err := it.accept(result); err != nil {
			return false, err
		}
		iterators = append(iterators, it)
	}
	// Advance empty iterators in parallel batched waves rather than falling back
	// to sequential single-partition RPCs on sparse kind-filtered scans.
	for len(iterators) > 0 {
		var needBatch []*entityIterator
		var batchReqs []ReadRequest
		for _, it := range iterators {
			if len(it.rows) > 0 {
				found, err := it.advance(ctx)
				if err != nil {
					return false, err
				}
				if found {
					heap.Push(&pending, it)
				}
			} else if !it.done {
				needBatch = append(needBatch, it)
				batchReqs = append(batchReqs, it.request)
			}
		}
		if len(needBatch) == 0 {
			break
		}
		results, err := l.View.readBatch(ctx, batchReqs)
		if err != nil {
			return false, err
		}
		for i, result := range results {
			if err := needBatch[i].accept(result); err != nil {
				return false, err
			}
		}
		iterators = needBatch
	}
	ids := make([]string, 0, 128)
	for len(pending) > 0 {
		ids = ids[:0]
		var last *entityIterator
		for len(ids) < window && len(pending) > 0 {
			last = heap.Pop(&pending).(*entityIterator)
			ids = append(ids, last.id)
			if len(ids) == window {
				break
			}
			found, err := last.advance(ctx)
			if err != nil {
				return false, err
			}
			if found {
				heap.Push(&pending, last)
			}
			last = nil
		}
		entities, _, err := l.GetEntities(ctx, ids, nil)
		if err != nil {
			return false, err
		}
		for _, id := range ids {
			entity, found := entities[id]
			if !found {
				return false, fmt.Errorf("%w: entity scan directory has no matching primary record", ErrPartitionCorrupt)
			}
			more, err := visit(entity)
			if err != nil || !more {
				return true, err
			}
		}
		if last != nil {
			found, err := last.advance(ctx)
			if err != nil {
				return false, err
			}
			if found {
				heap.Push(&pending, last)
			}
		}
		window = min(128, window*2)
	}
	return true, nil
}

func (l *Lookup) edges(ctx context.Context, id, direction string, allowed map[string]struct{}, start string, batchSize int, visit func(graph.Edge) (bool, error)) (bool, error) {
	prefix := Key(direction, id)
	next := ""
	if start != "" {
		next = AdjacencyKey(id, direction, start)
	}
	for {
		batch, err := l.View.Read(ctx, Bucket(id), ReadRequest{Prefix: prefix, Start: next, Limit: batchSize})
		if err != nil {
			return false, err
		}
		locations := make([]edgeLocation, 0, len(batch.Rows))
		byPartition := map[uint8][]string{}
		for _, row := range batch.Rows {
			var loc edgeLocation
			if err := decodeRecord(row, &loc); err != nil {
				return false, err
			}
			locations = append(locations, loc)
			byPartition[loc.Partition] = append(byPartition[loc.Partition], EdgeKey(loc.ID))
		}
		edges, err := l.View.fetchEdges(ctx, byPartition)
		if err != nil {
			return false, err
		}
		for _, loc := range locations {
			edge, ok := edges[loc.ID]
			if !ok {
				return false, fmt.Errorf("%w: adjacency references a missing edge", ErrPartitionCorrupt)
			}
			if len(allowed) > 0 {
				if _, ok := allowed[edge.Type]; !ok {
					continue
				}
			}
			more, err := visit(edge)
			if err != nil || !more {
				return true, err
			}
		}
		if batch.Next == "" {
			return true, nil
		}
		if batch.Next <= next {
			return false, fmt.Errorf("%w: adjacency scan made no progress", ErrPartitionCorrupt)
		}
		next = batch.Next
	}
}

func (v View) fetchEdges(ctx context.Context, byPartition map[uint8][]string) (map[string]graph.Edge, error) {
	edges := make(map[string]graph.Edge)
	err := v.visitKeys(ctx, byPartition, func(bucket uint8, row Row) error {
		var edge graph.Edge
		if err := decodeRecord(row, &edge); err != nil {
			return err
		}
		if row.Key != EdgeKey(edge.ID) || Bucket(edge.From) != bucket {
			return fmt.Errorf("%w: edge fragment identity mismatch", ErrPartitionCorrupt)
		}
		edges[edge.ID] = edge
		return nil
	})
	return edges, err
}

func (l *Lookup) VisitOutEdges(ctx context.Context, id string, allowed map[string]struct{}, start string, visit func(graph.Edge) (bool, error)) (bool, error) {
	return l.edges(ctx, id, "out", allowed, start, 64, visit)
}
func (l *Lookup) VisitInEdges(ctx context.Context, id string, allowed map[string]struct{}, start string, visit func(graph.Edge) (bool, error)) (bool, error) {
	return l.edges(ctx, id, "in", allowed, start, 64, visit)
}
func (l *Lookup) OutEdges(ctx context.Context, id string, allowed map[string]struct{}) ([]graph.Edge, bool, error) {
	var out []graph.Edge
	ok, err := l.edges(ctx, id, "out", allowed, "", 64, func(edge graph.Edge) (bool, error) { out = append(out, edge); return true, nil })
	return out, ok, err
}
func (l *Lookup) InEdges(ctx context.Context, id string, allowed map[string]struct{}) ([]graph.Edge, bool, error) {
	var out []graph.Edge
	ok, err := l.edges(ctx, id, "in", allowed, "", 64, func(edge graph.Edge) (bool, error) { out = append(out, edge); return true, nil })
	return out, ok, err
}

func ExecuteQuery(ctx context.Context, view View, request query.Request) (query.Response, error) {
	return ExecuteQueryWithPredicate(ctx, view, request, nil)
}

func ExecuteQueryWithPredicate(ctx context.Context, view View, request query.Request, predicate func(graph.Entity) bool) (query.Response, error) {
	if err := query.ValidateRequest(request); err != nil {
		return query.Response{}, err
	}
	if view.Graph.State != "active" {
		return query.Response{}, conflict("graph is not active")
	}
	if view.Graph.Version < request.MinVersion {
		return query.Response{}, ErrVersionNotReady
	}
	if view.Budget == nil {
		view.Budget = &ReadBudget{Requests: 2048, Bytes: 64 << 20}
	}
	window := min(128, request.Limit+1)
	if len(request.Sort) != 0 || len(request.Aggregate) != 0 || len(request.GroupBy) != 0 {
		window = 128
	}
	lookup := &Lookup{View: view, scanWindow: window}
	stats := query.PlannerStats{Version: view.Graph.Version, EntityScanAvailable: true, ForwardEdgeIndexAvailable: true, ReverseEdgeIndexAvailable: true}
	for i, count := range view.Graph.EntityCounts {
		if count > 0 {
			stats.EntityPages = append(stats.EntityPages, query.PlannerEntityPageStat{Shard: fmt.Sprintf("%02x", i), EntityCount: int(count)})
		}
	}
	if len(stats.EntityPages) == 0 {
		stats.EntityPages = []query.PlannerEntityPageStat{{Shard: "empty"}}
	}
	g, err := graph.FromSnapshot(graph.Snapshot{Version: view.Graph.Version, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes})
	if err != nil {
		return query.Response{}, err
	}
	for _, rt := range g.RelationTypes {
		edgeStat := query.PlannerEdgeStat{RelationType: rt.Name, ImpactDirection: rt.ImpactDirection, EdgeCount: int(view.Graph.EdgeTypeCounts[rt.Name])}
		stats.EdgeShards = append(stats.EdgeShards, edgeStat)
		stats.ReverseEdgeShards = append(stats.ReverseEdgeShards, edgeStat)
	}
	if !query.SupportsLazyRead(request, stats) {
		return query.Response{}, fmt.Errorf("%w: distributed execution does not support this query", query.ErrInvalid)
	}
	result, err := query.ExecuteContextWithOptions(ctx, g, request, query.ExecuteOptions{
		PlannerStats:              stats,
		IndexLookup:               lookup,
		EntityLookup:              lookup,
		AuthoritativeEntityLookup: true,
		EntityPredicate:           predicate,
	})
	if errors.Is(err, ErrReadBudget) {
		err = fmt.Errorf("%w: %v", query.ErrLimitExceeded, err)
	}
	return result, err
}

var _ query.IndexLookup = (*Lookup)(nil)
var _ query.EntityBatchLookup = (*Lookup)(nil)
var _ query.EntityPageLookup = (*Lookup)(nil)
var _ query.BothEdgeVisitLookup = (*Lookup)(nil)
var _ query.InEdgeVisitLookup = (*Lookup)(nil)
