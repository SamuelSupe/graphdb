package query

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type fakeLookup struct {
	fieldCalls int
	edgeCalls  int
}

func (f *fakeLookup) MatchFieldIndex(_ context.Context, _ string, _ string, _ []any) ([]string, bool, error) {
	f.fieldCalls++
	return []string{"host:app-02"}, true, nil
}

func (f *fakeLookup) OutEdges(_ context.Context, _ string, _ map[string]struct{}) ([]graph.Edge, bool, error) {
	f.edgeCalls++
	return []graph.Edge{{ID: "edge:fake", Type: "runs_on", From: "service:api", To: "host:app-02"}}, true, nil
}

type rangeScanLookup struct {
	scanCalls int
	entities  map[string]graph.Entity
}

func (l *rangeScanLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (l *rangeScanLookup) ScanFieldIndex(context.Context, string, string) (map[string][]string, bool, error) {
	l.scanCalls++
	return map[string][]string{
		"s:app-01": {"host:1"},
		"s:app-02": {"host:2"},
		"s:app-03": {"host:3"},
		"s:app-04": {"host:4"},
	}, true, nil
}

func (l *rangeScanLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (l *rangeScanLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	entity, ok := l.entities[id]
	return entity, ok, nil
}

type unavailableLookup struct{}

func (u unavailableLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (u unavailableLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (u unavailableLookup) GetEntity(context.Context, string, []string) (graph.Entity, bool, error) {
	return graph.Entity{}, false, nil
}

type missingEntityLookup struct{}

func (m missingEntityLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return []string{"host:app-01"}, true, nil
}

func (m missingEntityLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (m missingEntityLookup) GetEntity(context.Context, string, []string) (graph.Entity, bool, error) {
	return graph.Entity{}, false, nil
}

type missingEdgeShardLookup struct{}

func (m missingEdgeShardLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (m missingEdgeShardLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (m missingEdgeShardLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	if id == "service:api" {
		return graph.Entity{ID: id, Kind: "service"}, true, nil
	}
	return graph.Entity{}, false, nil
}

type cancelingEntityLookup struct {
	cancel context.CancelFunc
	calls  int
}

func (l *cancelingEntityLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return []string{"host:app-01", "host:app-02"}, true, nil
}

func (l *cancelingEntityLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (l *cancelingEntityLookup) GetEntity(context.Context, string, []string) (graph.Entity, bool, error) {
	l.calls++
	if l.calls == 1 {
		l.cancel()
	}
	return graph.Entity{ID: "host:app", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}, true, nil
}

type countingLazyMatchLookup struct {
	ids   []string
	calls []string
}

func (l *countingLazyMatchLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return append([]string(nil), l.ids...), true, nil
}

func (l *countingLazyMatchLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (l *countingLazyMatchLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	l.calls = append(l.calls, id)
	return graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"hostname": "app"}}, true, nil
}

type pageScanLookup struct {
	entities []graph.Entity
	fields   []string
	afterID  string
	calls    int
	visited  int
}

func (l *pageScanLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (l *pageScanLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (l *pageScanLookup) GetEntity(context.Context, string, []string) (graph.Entity, bool, error) {
	return graph.Entity{}, false, nil
}

func (l *pageScanLookup) VisitEntities(_ context.Context, kind string, fields []string, afterID string, visit func(graph.Entity) (bool, error)) (bool, error) {
	l.calls++
	l.fields = append([]string(nil), fields...)
	l.afterID = afterID
	for _, entity := range l.entities {
		if kind != "" && entity.Kind != kind {
			continue
		}
		if afterID != "" && entity.ID < afterID {
			continue
		}
		l.visited++
		keepGoing, err := visit(entity)
		if err != nil {
			return false, err
		}
		if !keepGoing {
			return true, nil
		}
	}
	return true, nil
}

type driftingEdgeLookup struct {
	entityCalls int
}

func (l *driftingEdgeLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (l *driftingEdgeLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	edges := make([]graph.Edge, 0, 8)
	for i := 0; i < 8; i++ {
		edges = append(edges, graph.Edge{
			ID:   graph.CanonicalEdgeID(graph.Edge{Type: "runs_on", From: "service:api", To: fmt.Sprintf("host:%02d", i)}),
			Type: "runs_on",
			From: "service:api",
			To:   fmt.Sprintf("host:%02d", i),
		})
	}
	return edges, true, nil
}

func (l *driftingEdgeLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	l.entityCalls++
	if id == "service:api" {
		return graph.Entity{ID: id, Kind: "service"}, true, nil
	}
	return graph.Entity{ID: id, Kind: "host"}, true, nil
}

type countingOutEdgeLookup struct {
	edges       []graph.Edge
	calls       []string
	outCalls    int
	visitStarts []string
	visited     int
}

type countingInEdgeLookup struct {
	edges       []graph.Edge
	calls       []string
	inCalls     int
	visitStarts []string
	visited     int
}

type bidirectionalNeighborLookup struct {
	entities map[string]graph.Entity
	out      []graph.Edge
	in       []graph.Edge
}

type countingBothEdgeLookup struct {
	entities    map[string]graph.Entity
	neighbors   []graph.Neighbor
	outCalls    int
	inCalls     int
	visitStarts []string
	visited     int
}

type adjacencyLookup struct {
	entities map[string]graph.Entity
	edges    map[string][]graph.Edge
	calls    map[string]int
}

func (l *adjacencyLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (l *adjacencyLookup) OutEdges(_ context.Context, from string, _ map[string]struct{}) ([]graph.Edge, bool, error) {
	if l.calls == nil {
		l.calls = map[string]int{}
	}
	l.calls[from]++
	return append([]graph.Edge(nil), l.edges[from]...), true, nil
}

func (l *adjacencyLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	entity, ok := l.entities[id]
	return entity, ok, nil
}

func (l *countingOutEdgeLookup) MatchFieldIndex(context.Context, string, string, []any) ([]string, bool, error) {
	return nil, false, nil
}

func (l *countingOutEdgeLookup) OutEdges(context.Context, string, map[string]struct{}) ([]graph.Edge, bool, error) {
	l.outCalls++
	return append([]graph.Edge(nil), l.edges...), true, nil
}

func (l *countingOutEdgeLookup) VisitOutEdges(
	_ context.Context,
	_ string,
	_ map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	l.visitStarts = append(l.visitStarts, startEdgeID)
	for _, edge := range l.edges {
		if startEdgeID != "" && edge.ID < startEdgeID {
			continue
		}
		l.visited++
		keepGoing, err := visit(edge)
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func (l *countingOutEdgeLookup) GetEntity(_ context.Context, id string, _ []string) (graph.Entity, bool, error) {
	l.calls = append(l.calls, id)
	if id == "service:start" {
		return graph.Entity{ID: id, Kind: "service"}, true, nil
	}
	return graph.Entity{ID: id, Kind: "host"}, true, nil
}

func (l *countingInEdgeLookup) MatchFieldIndex(
	context.Context, string, string, []any,
) ([]string, bool, error) {
	return nil, false, nil
}

func (l *countingInEdgeLookup) OutEdges(
	context.Context, string, map[string]struct{},
) ([]graph.Edge, bool, error) {
	return nil, false, nil
}

func (l *countingInEdgeLookup) InEdges(
	context.Context, string, map[string]struct{},
) ([]graph.Edge, bool, error) {
	l.inCalls++
	return append([]graph.Edge(nil), l.edges...), true, nil
}

func (l *countingInEdgeLookup) VisitInEdges(
	_ context.Context,
	_ string,
	_ map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge) (bool, error),
) (bool, error) {
	l.visitStarts = append(l.visitStarts, startEdgeID)
	for _, edge := range l.edges {
		if startEdgeID != "" && edge.ID < startEdgeID {
			continue
		}
		l.visited++
		keepGoing, err := visit(edge)
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func (l *countingInEdgeLookup) GetEntity(
	_ context.Context,
	id string,
	_ []string,
) (graph.Entity, bool, error) {
	l.calls = append(l.calls, id)
	if id == "host:start" {
		return graph.Entity{ID: id, Kind: "host"}, true, nil
	}
	return graph.Entity{ID: id, Kind: "service"}, true, nil
}

func (l *bidirectionalNeighborLookup) MatchFieldIndex(
	context.Context, string, string, []any,
) ([]string, bool, error) {
	return nil, false, nil
}

func (l *bidirectionalNeighborLookup) OutEdges(
	context.Context, string, map[string]struct{},
) ([]graph.Edge, bool, error) {
	return append([]graph.Edge(nil), l.out...), true, nil
}

func (l *bidirectionalNeighborLookup) InEdges(
	context.Context, string, map[string]struct{},
) ([]graph.Edge, bool, error) {
	return append([]graph.Edge(nil), l.in...), true, nil
}

func (l *bidirectionalNeighborLookup) GetEntity(
	_ context.Context,
	id string,
	_ []string,
) (graph.Entity, bool, error) {
	entity, ok := l.entities[id]
	return entity, ok, nil
}

func (l *countingBothEdgeLookup) MatchFieldIndex(
	context.Context, string, string, []any,
) ([]string, bool, error) {
	return nil, false, nil
}

func (l *countingBothEdgeLookup) OutEdges(
	context.Context, string, map[string]struct{},
) ([]graph.Edge, bool, error) {
	l.outCalls++
	return nil, true, nil
}

func (l *countingBothEdgeLookup) InEdges(
	context.Context, string, map[string]struct{},
) ([]graph.Edge, bool, error) {
	l.inCalls++
	return nil, true, nil
}

func (l *countingBothEdgeLookup) VisitBothEdges(
	_ context.Context,
	_ string,
	_ map[string]struct{},
	startEdgeID string,
	visit func(graph.Edge, string) (bool, error),
) (bool, error) {
	l.visitStarts = append(l.visitStarts, startEdgeID)
	for _, neighbor := range l.neighbors {
		if startEdgeID != "" && neighbor.Edge.ID < startEdgeID {
			continue
		}
		l.visited++
		keepGoing, err := visit(
			neighbor.Edge, neighbor.Direction,
		)
		if err != nil || !keepGoing {
			return true, err
		}
	}
	return true, nil
}

func (l *countingBothEdgeLookup) GetEntity(
	_ context.Context,
	id string,
	_ []string,
) (graph.Entity, bool, error) {
	entity, ok := l.entities[id]
	return entity, ok, nil
}

func TestExecutorUsesExternalFieldIndexLookup(t *testing.T) {
	g := seedCMDBGraph(t)
	lookup := &fakeLookup{}
	response, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:      "match",
		Kind:    "host",
		Where:   []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Profile: true,
	}, ExecuteOptions{IndexLookup: lookup})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if lookup.fieldCalls != 1 {
		t.Fatalf("field calls = %d", lookup.fieldCalls)
	}
	if len(response.Results) != 0 {
		t.Fatalf("fake lookup should be exact-filtered out: %#v", response.Results)
	}
}

func TestExecutorUsesFieldIndexScanForRangeAggregateSort(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &rangeScanLookup{entities: map[string]graph.Entity{
		"host:1": {ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "r1"}},
		"host:2": {ID: "host:2", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "region": "r2"}},
		"host:3": {ID: "host:3", Kind: "host", Fields: graph.Fields{"hostname": "app-03", "region": "r2"}},
		"host:4": {ID: "host:4", Kind: "host", Fields: graph.Fields{"hostname": "app-04", "region": "r3"}},
	}}
	response, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:        "match",
		Kind:      "host",
		Where:     []Filter{{Field: "hostname", Op: "gte", Value: "app-02"}, {Field: "hostname", Op: "lt", Value: "app-04"}},
		Sort:      []SortSpec{{Field: "hostname", Desc: true}},
		Aggregate: []Aggregation{{Name: "matched", Op: "count"}, {Name: "by_region", Op: "count_by", Field: "region"}},
		Limit:     1,
		Profile:   true,
	}, ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			Indexes:     []PlannerIndexStat{{Kind: "host", Field: "hostname", Status: "ready", EntryCount: 4, DistinctValues: 4}},
			EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: 4}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if lookup.scanCalls != 1 || response.Plan == nil || response.Plan.Strategy != "field-index-scan" {
		t.Fatalf("scanCalls=%d plan=%#v", lookup.scanCalls, response.Plan)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:3" || response.Aggregates["matched"] != 2 {
		t.Fatalf("response=%#v", response)
	}
	counts, ok := response.Aggregates["by_region"].(map[string]int)
	if !ok || counts["r2"] != 2 {
		t.Fatalf("aggregates=%#v", response.Aggregates)
	}
	if !hasOperator(response.Profile, "index-scan") {
		t.Fatalf("profile=%#v", response.Profile)
	}
}

func TestNeighborsUseExternalEdgeShardLookupForOutDirection(t *testing.T) {
	g := seedCMDBGraph(t)
	lookup := &fakeLookup{}
	response, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
	}, ExecuteOptions{IndexLookup: lookup})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if lookup.edgeCalls != 1 || len(response.Results) != 1 || response.Results[0].Entity.ID != "host:app-02" {
		t.Fatalf("response=%#v edgeCalls=%d", response, lookup.edgeCalls)
	}
}

func TestLazyMaterializationStopsAfterContextCancellation(t *testing.T) {
	g := graph.New()
	g.Version = 1
	ctx, cancel := context.WithCancel(context.Background())
	lookup := &cancelingEntityLookup{cancel: cancel}
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			Indexes:     []PlannerIndexStat{{Kind: "host", Field: "hostname", Status: "ready", EntryCount: 2, DistinctValues: 2}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 2}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
	_, err := ExecuteContextWithOptions(ctx, g, Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Sort:  []SortSpec{{Field: "id"}},
	}, options)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if lookup.calls != 1 {
		t.Fatalf("entity materializations = %d, want stop after cancellation", lookup.calls)
	}
}

func TestLazyMatchCursorSkipsPrefixIDsBeforeMaterialization(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &countingLazyMatchLookup{ids: []string{"host:00", "host:01", "host:02", "host:03", "host:04", "host:05"}}
	request := Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "eq", Value: "app"}},
		Limit: 1,
	}
	request.Cursor = encodeCursor(cursorState{Version: 1, After: "entity:host:03", Query: cursorQueryHash(request)})
	response, err := ExecuteContextWithOptions(context.Background(), g, request, lazyMatchExecuteOptions(lookup))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:04" {
		t.Fatalf("results = %#v, want host:04", response.Results)
	}
	if got, want := fmt.Sprint(lookup.calls), "[host:03 host:04 host:05]"; got != want {
		t.Fatalf("materialized ids = %s, want %s", got, want)
	}
}

func TestLazyStreamCursorSkipsPrefixIDsBeforeMaterialization(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &countingLazyMatchLookup{ids: []string{"host:00", "host:01", "host:02", "host:03", "host:04", "host:05"}}
	request := Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "eq", Value: "app"}},
		Limit: 1,
	}
	request.Cursor = encodeCursor(cursorState{Version: 1, After: "entity:host:03", Query: cursorQueryHash(request)})
	emitted := 0
	ok, err := StreamContextWithOptions(context.Background(), g, request, lazyMatchExecuteOptions(lookup), func(any) error {
		emitted++
		return nil
	})
	if !ok || err != nil {
		t.Fatalf("stream ok=%v err=%v", ok, err)
	}
	if emitted != 3 {
		t.Fatalf("emitted = %d, want meta/result/done", emitted)
	}
	if got, want := fmt.Sprint(lookup.calls), "[host:03 host:04 host:05]"; got != want {
		t.Fatalf("materialized ids = %s, want %s", got, want)
	}
}

func TestLazyKindScanUsesEntityPagesForFuzzyMatch(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &pageScanLookup{entities: []graph.Entity{
		{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "api service", "owner": "platform"}},
		{ID: "service:billing", Kind: "service", Fields: graph.Fields{"name": "billing", "owner": "finance"}},
		{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"name": "api host"}},
	}}
	request := Request{
		Op:      "match",
		Kind:    "service",
		Where:   []Filter{{Field: "name", Op: "fuzzy", Value: "apisvc"}},
		Project: []string{"name", "owner"},
		Limit:   10,
	}
	response, err := ExecuteContextWithOptions(context.Background(), g, request, ExecuteOptions{
		PlannerStats: PlannerStats{Version: 1, EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 3}}},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("lazy fuzzy match: %v", err)
	}
	if lookup.calls != 1 {
		t.Fatalf("page scan calls = %d, want 1", lookup.calls)
	}
	if got, want := fmt.Sprint(lookup.fields), "[name owner]"; got != want {
		t.Fatalf("projection fields = %s, want %s", got, want)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "service:api" {
		t.Fatalf("results = %#v, want service:api", response.Results)
	}
	if response.Results[0].Fields["owner"] != "platform" {
		t.Fatalf("projection = %#v", response.Results[0].Fields)
	}
}

func TestLazyKindScanStopsAfterLimitLookahead(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &pageScanLookup{}
	for i := 0; i < 100; i++ {
		lookup.entities = append(lookup.entities, graph.Entity{ID: fmt.Sprintf("service:%03d", i), Kind: "service"})
	}
	response, err := ExecuteContextWithOptions(context.Background(), g, Request{Op: "match", Kind: "service", Limit: 2}, ExecuteOptions{
		PlannerStats: PlannerStats{Version: 1, EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: 100}}},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("lazy kind scan: %v", err)
	}
	if len(response.Results) != 2 || response.NextCursor == "" {
		t.Fatalf("response = %#v, want two results and next cursor", response)
	}
	if lookup.visited != 3 {
		t.Fatalf("visited = %d, want limit+1", lookup.visited)
	}
}

func TestLazyKindScanSecondPageStartsAtCursorEntity(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &pageScanLookup{}
	for i := 0; i < 8; i++ {
		lookup.entities = append(lookup.entities, graph.Entity{ID: fmt.Sprintf("service:%02d", i), Kind: "service"})
	}
	request := Request{Op: "match", Kind: "service", Limit: 2}
	options := ExecuteOptions{
		PlannerStats: PlannerStats{Version: 1, EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: len(lookup.entities)}}},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
	first, err := ExecuteContextWithOptions(context.Background(), g, request, options)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("first page missing cursor")
	}
	lookup.visited = 0
	request.Cursor = first.NextCursor
	second, err := ExecuteContextWithOptions(context.Background(), g, request, options)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if lookup.afterID != "service:01" {
		t.Fatalf("visitor after id = %q, want service:01", lookup.afterID)
	}
	if lookup.visited != 4 {
		t.Fatalf("second page visited = %d, want cursor + page + lookahead", lookup.visited)
	}
	if len(second.Results) != 2 || second.Results[0].Entity.ID != "service:02" || second.Results[1].Entity.ID != "service:03" {
		t.Fatalf("second page results = %#v", second.Results)
	}
}

func TestShortestPathExpandsDiamondNodeOnce(t *testing.T) {
	ids := []string{"start", "left", "right", "merge", "leaf", "target"}
	lookup := &adjacencyLookup{entities: map[string]graph.Entity{}, edges: map[string][]graph.Edge{}}
	for _, id := range ids {
		lookup.entities[id] = graph.Entity{ID: id, Kind: "node"}
	}
	add := func(from, to string) {
		lookup.edges[from] = append(lookup.edges[from], graph.Edge{ID: from + "->" + to, Type: "links", From: from, To: to})
	}
	add("start", "left")
	add("start", "right")
	add("left", "merge")
	add("right", "merge")
	add("merge", "leaf")
	g := graph.New()
	g.Version = 1
	response, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op: "shortest_path", ID: "start", TargetID: "target", Direction: "out", Depth: 4,
	}, ExecuteOptions{
		PlannerStats: PlannerStats{Version: 1, EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: len(ids)}}, EdgeShards: []PlannerEdgeStat{{RelationType: "links", Shard: "00", EdgeCount: 5}}},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("shortest path: %v", err)
	}
	if len(response.Results) != 0 {
		t.Fatalf("results = %#v, want none", response.Results)
	}
	if lookup.calls["merge"] != 1 {
		t.Fatalf("merge expansions = %d, want 1", lookup.calls["merge"])
	}
}

func TestSteppedShortestPathKeepsDistinctCycleHistories(t *testing.T) {
	ids := []string{"start", "a", "b", "x", "target"}
	lookup := &adjacencyLookup{entities: map[string]graph.Entity{}, edges: map[string][]graph.Edge{}}
	for _, id := range ids {
		lookup.entities[id] = graph.Entity{ID: id, Kind: "node"}
	}
	add := func(from, to string) {
		lookup.edges[from] = append(lookup.edges[from], graph.Edge{ID: from + "->" + to, Type: "links", From: from, To: to})
	}
	add("start", "a")
	add("start", "b")
	add("a", "x")
	add("a", "target")
	add("b", "x")
	add("x", "a")
	g := graph.New()
	g.Version = 1
	response, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op: "shortest_path", ID: "start", TargetID: "target", Direction: "out", Depth: 4,
		Path: PathFilter{Steps: make([]PathStep, 4)},
	}, ExecuteOptions{
		PlannerStats: PlannerStats{Version: 1, EntityPages: []PlannerEntityPageStat{{Shard: "00", EntityCount: len(ids)}}, EdgeShards: []PlannerEdgeStat{{RelationType: "links", Shard: "00", EdgeCount: 6}}},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("shortest path: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Path == nil {
		t.Fatalf("results = %#v, want one path", response.Results)
	}
	path := response.Results[0].Path
	want := []string{"start", "b", "x", "a", "target"}
	if len(path.Entities) != len(want) {
		t.Fatalf("path entities = %#v, want %v", path.Entities, want)
	}
	for i, entity := range path.Entities {
		if entity.ID != want[i] {
			t.Fatalf("path[%d] = %q, want %q", i, entity.ID, want[i])
		}
	}
}

func TestLazyStreamKindScanUsesEntityPagesForFuzzyMatch(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &pageScanLookup{entities: []graph.Entity{
		{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "api service"}},
		{ID: "service:billing", Kind: "service", Fields: graph.Fields{"name": "billing"}},
	}}
	request := Request{
		Op:    "match",
		Kind:  "service",
		Where: []Filter{{Field: "name", Op: "fuzzy", Value: "apisvc"}},
		Limit: 1,
	}
	emitted := 0
	ok, err := StreamContextWithOptions(context.Background(), g, request, ExecuteOptions{
		PlannerStats: PlannerStats{Version: 1, EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 2}}},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}, func(any) error {
		emitted++
		return nil
	})
	if !ok || err != nil {
		t.Fatalf("stream ok=%v err=%v", ok, err)
	}
	if emitted != 3 {
		t.Fatalf("emitted = %d, want meta/result/done", emitted)
	}
}

func TestLazyOutNeighborsCursorSkipsPrefixEdgesBeforeMaterialization(t *testing.T) {
	g := graph.New()
	g.Version = 1
	edges := make([]graph.Edge, 0, 6)
	for i := 0; i < 6; i++ {
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%02d", i),
			Type: "runs_on",
			From: "service:start",
			To:   fmt.Sprintf("host:%02d", i),
		})
	}
	lookup := &countingOutEdgeLookup{edges: edges}
	request := Request{
		Op:           "neighbors",
		ID:           "service:start",
		Direction:    "out",
		RelationType: "runs_on",
		Limit:        1,
	}
	request.Cursor = encodeCursor(cursorState{Version: 1, After: "edge:out:edge:03", Query: cursorQueryHash(request)})
	response, err := ExecuteContextWithOptions(context.Background(), g, request, ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			EdgeShards:  []PlannerEdgeStat{{RelationType: "runs_on", Shard: plannerEdgeShardID("service:start"), EdgeCount: len(edges)}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: len(edges) + 1}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Entity.ID != "host:04" {
		t.Fatalf("results = %#v, want host:04", response.Results)
	}
	if got, want := fmt.Sprint(lookup.calls), "[service:start host:03 host:04 host:05]"; got != want {
		t.Fatalf("materialized ids = %s, want %s", got, want)
	}
	if lookup.outCalls != 0 ||
		fmt.Sprint(lookup.visitStarts) != "[edge:03]" ||
		lookup.visited != 3 {
		t.Fatalf(
			"out calls=%d visit starts=%v visited=%d, want visitor range only",
			lookup.outCalls, lookup.visitStarts, lookup.visited,
		)
	}
}

func TestLazyInNeighborsCursorSkipsPrefixEdgesBeforeMaterialization(t *testing.T) {
	g := graph.New()
	g.Version = 1
	edges := make([]graph.Edge, 0, 6)
	for i := 0; i < 6; i++ {
		edges = append(edges, graph.Edge{
			ID:   fmt.Sprintf("edge:%02d", i),
			Type: "runs_on",
			From: fmt.Sprintf("service:%02d", i),
			To:   "host:start",
		})
	}
	lookup := &countingInEdgeLookup{edges: edges}
	request := Request{
		Op:           "neighbors",
		ID:           "host:start",
		Direction:    "in",
		RelationType: "runs_on",
		Limit:        1,
	}
	request.Cursor = encodeCursor(cursorState{
		Version: 1,
		After:   "edge:in:edge:03",
		Query:   cursorQueryHash(request),
	})
	response, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		request,
		ExecuteOptions{
			PlannerStats: PlannerStats{
				Version: 1,
				ReverseEdgeShards: []PlannerEdgeStat{{
					RelationType: "runs_on",
					Shard:        plannerEdgeShardID("host:start"),
					EdgeCount:    len(edges),
				}},
				EntityPages: []PlannerEntityPageStat{{
					Shard: "68", EntityCount: len(edges) + 1,
				}},
			},
			IndexLookup:  lookup,
			EntityLookup: lookup,
		},
	)
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 1 ||
		response.Results[0].Entity.ID != "service:04" {
		t.Fatalf("results = %#v, want service:04", response.Results)
	}
	if got, want := fmt.Sprint(lookup.calls),
		"[host:start service:03 service:04 service:05]"; got != want {
		t.Fatalf("materialized ids = %s, want %s", got, want)
	}
	if lookup.inCalls != 0 ||
		fmt.Sprint(lookup.visitStarts) != "[edge:03]" ||
		lookup.visited != 3 {
		t.Fatalf(
			"in calls=%d visit starts=%v visited=%d, want visitor range only",
			lookup.inCalls, lookup.visitStarts, lookup.visited,
		)
	}
}

func TestIndexedBothNeighborsPreserveOrderAndSelfLoopDirections(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &bidirectionalNeighborLookup{
		entities: map[string]graph.Entity{
			"node:start": {ID: "node:start", Kind: "node"},
			"node:in":    {ID: "node:in", Kind: "node"},
			"node:out":   {ID: "node:out", Kind: "node"},
		},
		out: []graph.Edge{
			{ID: "edge:02", Type: "links", From: "node:start", To: "node:out"},
			{ID: "edge:03", Type: "links", From: "node:start", To: "node:start"},
		},
		in: []graph.Edge{
			{ID: "edge:01", Type: "links", From: "node:in", To: "node:start"},
			{ID: "edge:03", Type: "links", From: "node:start", To: "node:start"},
		},
	}
	response, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		Request{
			Op: "neighbors", ID: "node:start",
			Direction: "both", RelationType: "links", Limit: 10,
		},
		ExecuteOptions{
			PlannerStats: PlannerStats{
				Version: 1,
				EdgeShards: []PlannerEdgeStat{{
					RelationType: "links", Shard: "00", EdgeCount: 2,
				}},
				ReverseEdgeShards: []PlannerEdgeStat{{
					RelationType: "links", Shard: "00", EdgeCount: 2,
				}},
				EntityPages: []PlannerEntityPageStat{{
					Shard: "00", EntityCount: 3,
				}},
			},
			IndexLookup:  lookup,
			EntityLookup: lookup,
		},
	)
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	identities := make([]string, 0, len(response.Results))
	for _, result := range response.Results {
		identities = append(identities, resultIdentity(result))
	}
	want := []string{
		"edge:in:edge:01",
		"edge:out:edge:02",
		"edge:in:edge:03",
		"edge:out:edge:03",
	}
	if fmt.Sprint(identities) != fmt.Sprint(want) {
		t.Fatalf("identities=%v, want %v", identities, want)
	}
}

func TestLazyBothNeighborsCursorUsesMergedVisitorRange(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &countingBothEdgeLookup{
		entities: map[string]graph.Entity{
			"node:start": {ID: "node:start", Kind: "node"},
			"node:in":    {ID: "node:in", Kind: "node"},
			"node:out":   {ID: "node:out", Kind: "node"},
			"node:later": {ID: "node:later", Kind: "node"},
		},
		neighbors: []graph.Neighbor{
			{
				Edge: graph.Edge{
					ID: "edge:01", Type: "links",
					From: "node:in", To: "node:start",
				},
				Direction: "in",
			},
			{
				Edge: graph.Edge{
					ID: "edge:02", Type: "links",
					From: "node:start", To: "node:out",
				},
				Direction: "out",
			},
			{
				Edge: graph.Edge{
					ID: "edge:03", Type: "links",
					From: "node:start", To: "node:start",
				},
				Direction: "in",
			},
			{
				Edge: graph.Edge{
					ID: "edge:03", Type: "links",
					From: "node:start", To: "node:start",
				},
				Direction: "out",
			},
			{
				Edge: graph.Edge{
					ID: "edge:04", Type: "links",
					From: "node:later", To: "node:start",
				},
				Direction: "in",
			},
		},
	}
	request := Request{
		Op: "neighbors", ID: "node:start",
		Direction: "both", RelationType: "links", Limit: 1,
	}
	request.Cursor = encodeCursor(cursorState{
		Version: 1,
		After:   "edge:in:edge:03",
		Query:   cursorQueryHash(request),
	})
	response, err := ExecuteContextWithOptions(
		context.Background(),
		g,
		request,
		ExecuteOptions{
			PlannerStats: PlannerStats{
				Version: 1,
				EdgeShards: []PlannerEdgeStat{{
					RelationType: "links",
					Shard:        plannerEdgeShardID("node:start"),
					EdgeCount:    3,
				}},
				ReverseEdgeShards: []PlannerEdgeStat{{
					RelationType: "links",
					Shard:        plannerEdgeShardID("node:start"),
					EdgeCount:    3,
				}},
				EntityPages: []PlannerEntityPageStat{{
					Shard: "00", EntityCount: 4,
				}},
			},
			IndexLookup:  lookup,
			EntityLookup: lookup,
		},
	)
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(response.Results) != 1 ||
		resultIdentity(response.Results[0]) != "edge:out:edge:03" {
		t.Fatalf(
			"results=%#v, want outgoing self-loop after cursor",
			response.Results,
		)
	}
	if lookup.outCalls != 0 || lookup.inCalls != 0 ||
		fmt.Sprint(lookup.visitStarts) != "[edge:03]" ||
		lookup.visited != 3 {
		t.Fatalf(
			"out=%d in=%d starts=%v visited=%d, want merged range only",
			lookup.outCalls,
			lookup.inCalls,
			lookup.visitStarts,
			lookup.visited,
		)
	}
}

func lazyMatchExecuteOptions(lookup *countingLazyMatchLookup) ExecuteOptions {
	return ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			Indexes:     []PlannerIndexStat{{Kind: "host", Field: "hostname", Status: "ready", EntryCount: len(lookup.ids), DistinctValues: 1}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: len(lookup.ids)}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
}

func TestEdgeShardExecutionCostUsesActualReturnedEdges(t *testing.T) {
	g := graph.New()
	g.Version = 1
	lookup := &driftingEdgeLookup{}
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			EdgeShards:  []PlannerEdgeStat{{RelationType: "runs_on", Shard: plannerEdgeShardID("service:api"), EdgeCount: 1}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 8}},
		},
		IndexLookup:  lookup,
		EntityLookup: lookup,
	}
	_, err := ExecuteContextWithOptions(context.Background(), g, Request{
		Op:           "neighbors",
		ID:           "service:api",
		Direction:    "out",
		RelationType: "runs_on",
		CostLimit:    3,
	}, options)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("err = %v, want ErrLimitExceeded", err)
	}
	if lookup.entityCalls > 4 {
		t.Fatalf("entity materializations = %d, want stop before fourth edge entity", lookup.entityCalls)
	}
}

func TestLazyMatchReportsUnavailablePersistedIndexBeforeReturningEmptyResults(t *testing.T) {
	g := graph.New()
	g.Version = 1
	request := Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
	}
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			Indexes:     []PlannerIndexStat{{Kind: "host", Field: "hostname", Status: "ready", EntryCount: 1, DistinctValues: 1}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 1}},
		},
		IndexLookup:  unavailableLookup{},
		EntityLookup: unavailableLookup{},
	}
	if _, err := ExecuteContextWithOptions(context.Background(), g, request, options); !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("err = %v, want ErrIndexUnavailable", err)
	}
	emitted := 0
	ok, err := StreamContextWithOptions(context.Background(), g, request, options, func(any) error {
		emitted++
		return nil
	})
	if !ok || !errors.Is(err, ErrIndexUnavailable) || emitted != 0 {
		t.Fatalf("stream ok=%v err=%v emitted=%d, want unavailable before emit", ok, err, emitted)
	}
}

func TestLazyStreamReportsUnavailableEntityBeforeEmitting(t *testing.T) {
	g := graph.New()
	g.Version = 1
	request := Request{
		Op:    "match",
		Kind:  "host",
		Where: []Filter{{Field: "hostname", Op: "eq", Value: "app-01"}},
		Limit: 1,
	}
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			Indexes:     []PlannerIndexStat{{Kind: "host", Field: "hostname", Status: "ready", EntryCount: 1, DistinctValues: 1}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 1}},
		},
		IndexLookup:  missingEntityLookup{},
		EntityLookup: missingEntityLookup{},
	}
	emitted := 0
	ok, err := StreamContextWithOptions(context.Background(), g, request, options, func(any) error {
		emitted++
		return nil
	})
	if !ok || !errors.Is(err, ErrIndexUnavailable) || emitted != 0 {
		t.Fatalf("stream ok=%v err=%v emitted=%d, want unavailable before emit", ok, err, emitted)
	}
}

func TestLazyEdgeTraversalReportsUnavailablePersistedIndexBeforeReturningEmptyResults(t *testing.T) {
	g := graph.New()
	g.Version = 1
	options := ExecuteOptions{
		PlannerStats: PlannerStats{
			Version:     1,
			EdgeShards:  []PlannerEdgeStat{{RelationType: "runs_on", Shard: plannerEdgeShardID("service:api"), EdgeCount: 1}},
			EntityPages: []PlannerEntityPageStat{{Shard: "68", EntityCount: 1}},
		},
		IndexLookup:  missingEdgeShardLookup{},
		EntityLookup: missingEdgeShardLookup{},
	}
	for _, request := range []Request{
		{Op: "neighbors", ID: "service:api", Direction: "out", RelationType: "runs_on"},
		{Op: "traverse", ID: "service:api", Direction: "out", RelationType: "runs_on", Depth: 1},
	} {
		if _, err := ExecuteContextWithOptions(context.Background(), g, request, options); !errors.Is(err, ErrIndexUnavailable) {
			t.Fatalf("%s err = %v, want ErrIndexUnavailable", request.Op, err)
		}
	}
}
