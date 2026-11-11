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
	var requests []ReadRequest
	for bucket, keys := range byPartition {
		for len(keys) > 0 {
			end := min(128, len(keys))
			requests = append(requests, ReadRequest{Partition: bucket, Keys: keys[:end]})
			keys = keys[end:]
		}
	}
	for len(requests) > 0 {
		batches, err := l.View.readBatch(ctx, requests)
		if err != nil {
			return nil, false, err
		}
		next := requests[:0]
		for i, batch := range batches {
			for _, row := range batch.Rows {
				var entity graph.Entity
				if err := decodeRecord(row, &entity); err != nil {
					return nil, false, fmt.Errorf("%w: malformed entity record: %v", ErrPartitionCorrupt, err)
				}
				out[entity.ID] = entity
			}
			if len(batch.Remaining) > 0 {
				if len(batch.Remaining) >= len(requests[i].Keys) {
					return nil, false, fmt.Errorf("%w: entity batch made no progress", ErrPartitionCorrupt)
				}
				pending := requests[i]
				pending.Keys = batch.Remaining
				next = append(next, pending)
			}
		}
		requests = next
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
	for i, result := range initial {
		it := &entityIterator{lookup: l, request: requests[i]}
		if err := it.accept(result); err != nil {
			return false, err
		}
		found, err := it.advance(ctx)
		if err != nil {
			return false, err
		}
		if found {
			heap.Push(&pending, it)
		}
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
		edges := map[string]graph.Edge{}
		for bucket, keys := range byPartition {
			for len(keys) > 0 {
				result, err := l.View.Read(ctx, bucket, ReadRequest{Keys: keys})
				if err != nil {
					return false, err
				}
				for _, row := range result.Rows {
					var edge graph.Edge
					if err := decodeRecord(row, &edge); err != nil {
						return false, err
					}
					edges[edge.ID] = edge
				}
				keys = result.Remaining
			}
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
		next = batch.Next
	}
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
	g, options, err := queryReadOptions(view, window)
	if err != nil {
		return query.Response{}, err
	}
	if !query.SupportsLazyRead(request, options.PlannerStats) {
		return query.Response{}, fmt.Errorf("%w: distributed execution does not support this query", query.ErrInvalid)
	}
	result, err := query.ExecuteContextWithOptions(ctx, g, request, options)
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

func queryReadOptions(view View, window int) (*graph.Graph, query.ExecuteOptions, error) {
	if view.Budget == nil {
		view.Budget = &ReadBudget{Requests: 2048, Bytes: 64 << 20}
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
	for _, rt := range view.Graph.RelationTypes {
		stats.EdgeShards = append(stats.EdgeShards, query.PlannerEdgeStat{RelationType: rt.Name, ImpactDirection: rt.ImpactDirection, EdgeCount: int(view.Graph.EdgeTypeCounts[rt.Name])})
	}
	g, err := graph.FromSnapshot(graph.Snapshot{Version: view.Graph.Version, CITypes: view.Graph.CITypes, RelationTypes: view.Graph.RelationTypes})
	if err != nil {
		return nil, query.ExecuteOptions{}, err
	}

	return g, query.ExecuteOptions{PlannerStats: stats, IndexLookup: lookup, EntityLookup: lookup}, nil
}
