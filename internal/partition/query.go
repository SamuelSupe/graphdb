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
	View     View
	entities map[string]graph.Entity
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
	for bucket, keys := range byPartition {
		for len(keys) > 0 {
			end := min(128, len(keys))
			pending := keys[:end]
			for len(pending) > 0 {
				batch, err := l.View.Read(ctx, bucket, ReadRequest{Keys: pending})
				if err != nil {
					return nil, false, err
				}
				for _, row := range batch.Rows {
					var entity graph.Entity
					if err := decodeRecord(row, &entity); err != nil {
						return nil, false, err
					}
					out[entity.ID] = entity
				}
				pending = batch.Remaining
			}
			keys = keys[end:]
		}
	}
	return out, true, nil
}

func (l *Lookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

type entityIterator struct {
	lookup *Lookup
	bucket uint8
	prefix string
	next   string
	rows   []Row
	done   bool
	seen   bool
	entity graph.Entity
}

func (it *entityIterator) advance(ctx context.Context, kind string) (bool, error) {
	for {
		if len(it.rows) == 0 {
			if it.done {
				return false, nil
			}
			result, err := it.lookup.View.Read(ctx, it.bucket, ReadRequest{Prefix: it.prefix, Start: it.next, Limit: 16})
			if err != nil {
				return false, err
			}
			it.rows = result.Rows
			it.next = result.Next
			it.done = result.Next == ""
			if len(it.rows) == 0 {
				if !it.seen && it.done {
					return false, fmt.Errorf("entity scan directory is missing")
				}
				if !it.done {
					return false, fmt.Errorf("fragment scan made no progress")
				}
				return false, nil
			}
		}
		it.seen = true
		row := it.rows[0]
		it.rows = it.rows[1:]
		if err := decodeRecord(row, &it.entity); err != nil {
			return false, err
		}
		if kind == "" || it.entity.Kind == kind {
			return true, nil
		}
	}
}

type entityHeap []*entityIterator

func (h entityHeap) Len() int           { return len(h) }
func (h entityHeap) Less(i, j int) bool { return h[i].entity.ID < h[j].entity.ID }
func (h entityHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *entityHeap) Push(x any)        { *h = append(*h, x.(*entityIterator)) }
func (h *entityHeap) Pop() any          { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

func (l *Lookup) VisitEntities(ctx context.Context, kind string, _ []string, start string, visit func(graph.Entity) (bool, error)) (bool, error) {
	var pending entityHeap
	for i, count := range l.View.Graph.EntityCounts {
		if count == 0 {
			continue
		}
		it := &entityIterator{lookup: l, bucket: uint8(i), prefix: Key("entity_scan")}
		if start != "" {
			it.next = Key("entity_scan", start)
		}
		found, err := it.advance(ctx, kind)
		if err != nil {
			return false, err
		}
		if found {
			heap.Push(&pending, it)
		}
	}
	for len(pending) > 0 {
		it := heap.Pop(&pending).(*entityIterator)
		entity, found, err := l.GetEntity(ctx, it.entity.ID, nil)
		if err != nil {
			return false, err
		}
		if !found {
			return false, fmt.Errorf("entity scan directory has no matching primary record")
		}
		more, err := visit(entity)
		if err != nil || !more {
			return true, err
		}
		found, err = it.advance(ctx, kind)
		if err != nil {
			return false, err
		}
		if found {
			heap.Push(&pending, it)
		}
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
				return false, fmt.Errorf("adjacency references a missing edge")
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
	lookup := &Lookup{View: view}
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
		return query.Response{}, err
	}
	if !query.SupportsLazyRead(request, stats) {
		return query.Response{}, fmt.Errorf("%w: distributed execution does not support this query", query.ErrInvalid)
	}
	result, err := query.ExecuteContextWithOptions(ctx, g, request, query.ExecuteOptions{PlannerStats: stats, IndexLookup: lookup, EntityLookup: lookup})
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
