package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// TestDifferentialLazyMaterializedStreaming exercises randomized graphs
// (including self-loops, multi-edges, prefix-overlapping edge IDs, and
// forward/reverse/both impact relations) across match, neighbors, traverse,
// shortest_path, impact, and pattern with pagination (Limit=1..5), Sort,
// Aggregate, GroupBy, EntityPredicate, and both EntityPageOrder modes.
func TestDifferentialLazyMaterializedStreaming(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 99, 2026} {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			runDifferentialSeed(t, seed)
		})
	}
}

func FuzzLazyMaterializedDifferential(f *testing.F) {
	f.Add(int64(1))
	f.Add(int64(42))
	f.Add(int64(2026))
	f.Fuzz(func(t *testing.T, seed int64) {
		runDifferentialSeed(t, seed)
	})
}

func runDifferentialSeed(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	full := buildDifferentialGraph(t, rng)
	lazy := graph.New()
	lazy.Version = full.Version

	for _, pageOrder := range []string{EntityPageOrderIdentity, EntityPageOrderShard} {
		lookup := newDifferentialLookup(full, pageOrder)
		baseOpts := ExecuteOptions{
			PlannerStats:              buildDifferentialStats(full),
			IndexLookup:               lookup,
			EntityLookup:              lookup,
			AuthoritativeEntityLookup: true,
		}
		requests := generateDifferentialRequests(rng, full)
		for idx, req := range requests {
			// Test both without and with an EntityPredicate.
			predicates := []func(graph.Entity) bool{nil}
			if idx%3 == 0 {
				predicates = append(predicates, func(e graph.Entity) bool {
					// Keep explicit endpoints visible so the predicate tests graph expansion.
					if e.ID == req.ID || e.ID == req.TargetID {
						return true
					}
					v, _ := e.Fields["active"].(bool)
					return v
				})
			}
			for predIdx, pred := range predicates {
				matOpts := ExecuteOptions{EntityPredicate: pred}
				lazyOpts := baseOpts
				lazyOpts.EntityPredicate = pred
				assertDifferentialParity(t, fmt.Sprintf("order=%s req=%d op=%s pred=%d", pageOrder, idx, req.Op, predIdx), full, lazy, req, matOpts, lazyOpts)
			}
		}
	}
}

func buildDifferentialGraph(t *testing.T, rng *rand.Rand) *graph.Graph {
	t.Helper()
	const nodeCount = 10
	kinds := []string{"service", "host"}
	envs := []string{"prod", "stage", "dev"}
	entities := make([]graph.Entity, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		kind := kinds[i%len(kinds)]
		entities = append(entities, graph.Entity{
			ID:   fmt.Sprintf("%s:%d", kind, i),
			Kind: kind,
			Fields: graph.Fields{
				"group":  i % 3,
				"score":  (i*7 + rng.Intn(5)) % 11,
				"env":    envs[i%len(envs)],
				"active": i%4 != 0,
			},
		})
	}
	relTypes := []graph.RelationType{
		{Name: "calls", Directed: true, FromKinds: []string{"service", "host"}, ToKinds: []string{"service", "host"}, AllowCrossKind: true, ImpactDirection: "forward"},
		{Name: "depends_on", Directed: true, FromKinds: []string{"service", "host"}, ToKinds: []string{"service", "host"}, AllowCrossKind: true, ImpactDirection: "reverse"},
		{Name: "peers_with", Directed: true, FromKinds: []string{"service", "host"}, ToKinds: []string{"service", "host"}, AllowCrossKind: true, ImpactDirection: "both"},
	}
	// Build edges including self-loops and prefix-overlapping IDs ("e1", "e10", "e100", "e2", "e20").
	edgeIDs := []string{"e1", "e10", "e100", "e2", "e20", "e3", "e30", "e4", "e5", "e6", "e7", "e8", "e9", "e11", "e12", "e13", "e14", "e15", "e16", "e17"}
	edges := make([]graph.Edge, 0, len(edgeIDs))
	for i, eid := range edgeIDs {
		fromIdx := rng.Intn(nodeCount)
		toIdx := rng.Intn(nodeCount)
		if i == 0 {
			// Guaranteed self-loop on node 0.
			fromIdx, toIdx = 0, 0
		} else if i == 1 {
			// Guaranteed second self-loop with prefix-overlapping ID "e10".
			fromIdx, toIdx = 0, 0
		}
		rt := relTypes[i%len(relTypes)]
		edges = append(edges, graph.Edge{
			ID:     eid,
			Type:   rt.Name,
			From:   entities[fromIdx].ID,
			To:     entities[toIdx].ID,
			Fields: graph.Fields{"weight": (i % 4) + 1},
		})
	}
	g, err := graph.FromSnapshot(graph.Snapshot{
		Version:       3,
		CITypes:       []graph.CIType{{Name: "service"}, {Name: "host"}},
		RelationTypes: relTypes,
		Entities:      entities,
		Edges:         edges,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	return g
}

func buildDifferentialStats(g *graph.Graph) PlannerStats {
	stats := PlannerStats{
		Version:                   g.Version,
		EntityScanAvailable:       true,
		ForwardEdgeIndexAvailable: true,
		ReverseEdgeIndexAvailable: true,
		EntityPages:               []PlannerEntityPageStat{{Shard: "00", EntityCount: g.Entities.Len()}},
		Indexes: []PlannerIndexStat{
			{Kind: "service", Field: "env", Status: "ready", EntryCount: 5, DistinctValues: 3},
			{Kind: "service", Field: "group", Status: "ready", EntryCount: 5, DistinctValues: 3},
			{Kind: "host", Field: "env", Status: "ready", EntryCount: 5, DistinctValues: 3},
		},
	}
	counts := map[string]int{}
	for _, edge := range g.Edges.All() {
		counts[edge.Type]++
	}
	for name, rt := range g.RelationTypes {
		c := counts[name]
		stats.EdgeShards = append(stats.EdgeShards, PlannerEdgeStat{
			RelationType: name, ImpactDirection: rt.ImpactDirection, Shard: "00", EdgeCount: c,
		})
		stats.ReverseEdgeShards = append(stats.ReverseEdgeShards, PlannerEdgeStat{
			RelationType: name, ImpactDirection: rt.ImpactDirection, Shard: "00", EdgeCount: c,
		})
	}
	return stats
}

func generateDifferentialRequests(rng *rand.Rand, g *graph.Graph) []Request {
	ids := make([]string, 0, g.Entities.Len())
	for id := range g.Entities.All() {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	pickID := func() string { return ids[rng.Intn(len(ids))] }
	pickLimit := func() int { return 1 + rng.Intn(5) }
	dirs := []string{"out", "in", "both"}
	rels := []string{"calls", "depends_on", "peers_with"}

	reqs := []Request{
		// match: simple, field-index, range filter, sort+aggregate+groupby+having
		{Op: "match", Kind: "service", Limit: pickLimit()},
		{Op: "match", Kind: "service", Filters: graph.Fields{"env": "prod"}, Limit: pickLimit()},
		{
			Op: "match", Kind: "host", Limit: pickLimit(),
			Where:   []Filter{{Field: "score", Op: "gte", Value: 3}},
			Project: []string{"id", "score", "env"},
		},
		{
			Op: "match", Kind: "service", Limit: pickLimit(),
			Sort:      []SortSpec{{Field: "score", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}, {Name: "sum_score", Op: "sum", Field: "score"}},
			GroupBy:   []string{"group"},
			Having:    []Filter{{Field: "count", Op: "gte", Value: 1}},
		},
		// neighbors: out, in, both, self-loop root, aggregate+sort
		{Op: "neighbors", ID: ids[0], Direction: "both", Limit: pickLimit()},
		{Op: "neighbors", ID: pickID(), Direction: dirs[rng.Intn(len(dirs))], RelationTypes: []string{rels[rng.Intn(len(rels))]}, Limit: pickLimit()},
		{
			Op: "neighbors", ID: ids[0], Direction: "both", Limit: pickLimit(),
			EdgeWhere: []Filter{{Field: "weight", Op: "gte", Value: 2}},
			Sort:      []SortSpec{{Field: "score", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}, {Op: "sum", Field: "weight"}},
			GroupBy:   []string{"group"},
		},
		// traverse: out, both, stepped
		{Op: "traverse", ID: ids[0], Direction: "out", Depth: 3, Limit: pickLimit()},
		{Op: "traverse", ID: ids[0], Direction: "both", Depth: 2, Limit: pickLimit()},
		{
			Op: "traverse", ID: ids[0], Direction: "both", Depth: 3, Limit: pickLimit(),
			Path:      PathFilter{MaxPaths: 1 + rng.Intn(7)},
			Sort:      []SortSpec{{Field: "end_id", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}, {Op: "sum", Field: "length"}},
		},
		{
			Op: "traverse", ID: ids[0], Depth: 2, Limit: pickLimit(),
			Path: PathFilter{Steps: []PathStep{
				{Direction: "out", RelationTypes: []string{"calls", "depends_on"}},
				{Direction: "in", RelationTypes: []string{"calls", "peers_with"}},
			}},
		},
		// shortest_path
		{Op: "shortest_path", ID: ids[0], TargetID: pickID(), Direction: "both", Depth: 4},
		{Op: "shortest_path", ID: pickID(), TargetID: pickID(), Direction: "out", Depth: 4},
		// impact (exercises forward, reverse, both relation impact directions)
		{Op: "impact", ID: ids[0], Depth: 3, Limit: pickLimit()},
		{Op: "impact", ID: pickID(), Depth: 2, Limit: pickLimit(), RelationTypes: []string{"calls", "depends_on"}},
		{
			Op: "impact", ID: ids[0], Depth: 3, Limit: pickLimit(),
			Path:      PathFilter{MaxPaths: 1 + rng.Intn(7)},
			Sort:      []SortSpec{{Field: "length", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}},
			GroupBy:   []string{"env"},
		},
		// pattern (with EntityPredicate tested by caller loop!)
		{
			Op: "pattern", Kind: "service", Limit: pickLimit(),
			Path: PathFilter{Steps: []PathStep{
				{Direction: "out", RelationTypes: []string{"calls", "depends_on", "peers_with"}},
				{Direction: "both", RelationTypes: []string{"calls", "peers_with"}},
			}},
		},
		{
			Op: "pattern", Kind: "service", Limit: pickLimit(),
			Path: PathFilter{MaxPaths: 1 + rng.Intn(7), Steps: []PathStep{
				{Direction: "both", RelationTypes: []string{"calls", "peers_with"}},
			}},
			Sort:      []SortSpec{{Field: "end_id", Desc: true}},
			Aggregate: []Aggregation{{Op: "count"}},
			GroupBy:   []string{"env"},
		},
	}
	return reqs
}

func assertDifferentialParity(
	t *testing.T,
	label string,
	full *graph.Graph,
	lazy *graph.Graph,
	request Request,
	matOpts ExecuteOptions,
	lazyOpts ExecuteOptions,
) {
	t.Helper()
	// Collect full sequences across pagination for materialized, lazy, and streaming.
	matAll, matFinal := collectAllPages(t, label+" materialized", full, request, matOpts)
	lazyAll, lazyFinal := collectAllPages(t, label+" lazy", lazy, request, lazyOpts)

	// When EntityPageOrderShard is used for an unsorted match scan, individual page
	// ordering follows shard order instead of identity order, so compare normalized
	// multisets for shard order and exact page-by-page JSON for identity order.
	shardOrder := false
	if ol, ok := lazyOpts.EntityLookup.(EntityPageOrderLookup); ok && ol.EntityPageOrder() == EntityPageOrderShard && request.Op == "match" && len(request.Sort) == 0 {
		shardOrder = true
	}

	if !shardOrder {
		if !jsonEqual(matAll, lazyAll) {
			t.Fatalf("%s: lazy results diverged from materialized\nmat=%s\nlazy=%s", label, mustJSON(matAll), mustJSON(lazyAll))
		}
	} else {
		matSorted := canonicalResultSlice(matAll)
		lazySorted := canonicalResultSlice(lazyAll)
		if !jsonEqual(matSorted, lazySorted) {
			t.Fatalf("%s (shard order): lazy result set diverged from materialized\nmat=%s\nlazy=%s", label, mustJSON(matSorted), mustJSON(lazySorted))
		}
	}
	if !jsonEqual(matFinal.Aggregates, lazyFinal.Aggregates) || !jsonEqual(matFinal.Groups, lazyFinal.Groups) {
		t.Fatalf("%s: aggregates/groups diverged\nmat=%s/%s\nlazy=%s/%s",
			label, mustJSON(matFinal.Aggregates), mustJSON(matFinal.Groups), mustJSON(lazyFinal.Aggregates), mustJSON(lazyFinal.Groups))
	}

	// Also compare Streaming execution when StreamContextWithOptions accepts the request.
	streamAll, streamed := collectAllStreamPages(t, label+" stream", lazy, request, lazyOpts)
	if streamed {
		if !jsonEqual(lazyAll, streamAll) {
			t.Fatalf("%s: streaming results diverged from lazy\nlazy=%s\nstream=%s", label, mustJSON(lazyAll), mustJSON(streamAll))
		}
	}
}

func collectAllPages(t *testing.T, label string, g *graph.Graph, req Request, opts ExecuteOptions) ([]Result, Response) {
	t.Helper()
	var all []Result
	var lastResp Response
	for page := 0; page < 200; page++ {
		resp, err := ExecuteContextWithOptions(context.Background(), g, req, opts)
		if err != nil {
			t.Fatalf("%s page %d error: %v", label, page, err)
		}
		all = append(all, resp.Results...)
		lastResp = resp
		if resp.NextCursor == "" {
			return all, lastResp
		}
		req.Cursor = resp.NextCursor
	}
	t.Fatalf("%s pagination did not terminate after 200 pages", label)
	return nil, Response{}
}

func collectAllStreamPages(t *testing.T, label string, g *graph.Graph, req Request, opts ExecuteOptions) ([]Result, bool) {
	t.Helper()
	var all []Result
	for page := 0; page < 200; page++ {
		var pageResults []Result
		var nextCursor string
		handled, err := StreamContextWithOptions(context.Background(), g, req, opts, func(item any) error {
			switch v := item.(type) {
			case Result:
				pageResults = append(pageResults, v)
			case StreamMeta:
				if v.Done {
					nextCursor = v.NextCursor
				}
			}
			return nil
		})
		if !handled {
			return nil, false
		}
		if err != nil {
			t.Fatalf("%s page %d error: %v", label, page, err)
		}
		all = append(all, pageResults...)
		if nextCursor == "" {
			return all, true
		}
		req.Cursor = nextCursor
	}
	t.Fatalf("%s streaming pagination did not terminate after 200 pages", label)
	return nil, true
}

func canonicalResultSlice(results []Result) []Result {
	cp := append([]Result(nil), results...)
	sort.Slice(cp, func(i, j int) bool {
		return compareResults(cp[i], cp[j], nil) < 0
	})
	return cp
}

func jsonEqual(a, b any) bool {
	return bytes.Equal(mustJSON(a), mustJSON(b))
}

func mustJSON(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

type differentialLookup struct {
	graphParityLookup
	pageOrder string
}

func newDifferentialLookup(g *graph.Graph, pageOrder string) *differentialLookup {
	return &differentialLookup{
		graphParityLookup: graphParityLookup{graph: g},
		pageOrder:         pageOrder,
	}
}

func (l *differentialLookup) EntityPageOrder() string {
	return l.pageOrder
}

func (l *differentialLookup) MatchFieldIndex(_ context.Context, kind, field string, values []any) ([]string, bool, error) {
	wanted := map[string]struct{}{}
	for _, v := range values {
		wanted[fmt.Sprint(v)] = struct{}{}
	}
	var ids []string
	for id, entity := range l.graph.Entities.All() {
		if entity.Kind != kind {
			continue
		}
		if fv, ok := entity.Fields[field]; ok {
			if _, match := wanted[fmt.Sprint(fv)]; match {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	return ids, true, nil
}

func (l *differentialLookup) VisitEntities(ctx context.Context, kind string, _ []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, error) {
	ids := make([]string, 0, l.graph.Entities.Len())
	for id, entity := range l.graph.Entities.All() {
		if kind == "" || entity.Kind == kind {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	ids = orderEntityIDs(ids, l.pageOrder)
	for _, id := range ids {
		if afterID != "" && compareEntityPageOrder(id, afterID, l.pageOrder) < 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		keepGoing, err := visit(l.graph.Entities.At(id))
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}
